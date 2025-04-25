import express from "express";
import body_parser from "body-parser";
import pg from "pg";
import assert from "node:assert";

import { EventMonitor } from "./event-monitor.ts";
import { sleep } from "./sleep.ts";

const port = 3000;
const db_user = "admin";
const db_pass = "letmein";
const db_name = "eventdb";

const pool = new pg.Pool({
  user: db_user,
  password: db_pass,
  database: db_name,
});

const app = express();
app.use(body_parser.json());

app.get("/", async (_req, res) => {
  try {
    const pgres = await pool.query("SELECT NOW() as t");
    res.send(`Hello World! Time is ${pgres.rows[0].t}`);
  } catch (err) {
    res.json(err);
  }
});

app.post("/event-apis/submit-command", async (req, res) => {
  const { command_uuid, command_type, command_data } = req.body;
  console.log(
    "submitting " + JSON.stringify({ command_uuid, command_type, command_data })
  );
  try {
    await pool.query("select enqueue_command($1,$2,$3)", [
      command_uuid,
      command_type,
      command_data,
    ]);
    res.status(200).send("ok");
  } catch (error: any) {
    if (error.constraint === "command_pkey") {
      res.status(202).send("already inserted");
    } else {
      console.error(error);
      res.status(500).send("could not insert command");
    }
  }
});

app.post("/event-apis/fail-command", async (req, res) => {
  const { command_uuid, reason } = req.body;
  console.log(
    "failing " + JSON.stringify({ command_uuid, reason }, undefined, 2)
  );
  try {
    await pool.query("select fail_command($1,$2)", [command_uuid, reason]);
    res.status(200).send("ok");
  } catch (error: any) {
    if (error.constraint === "command_outcome_pkey") {
      res.status(202).send("already processed");
    } else {
      console.error(error);
      res.status(500).send("could not insert command");
    }
  }
});

app.post("/event-apis/succeed-command", async (req, res) => {
  const { command_uuid, event_t, events } = req.body;
  console.log(
    "succeeding " +
      JSON.stringify({ command_uuid, event_t, events }, undefined, 2)
  );
  assert(
    Array.isArray(events) &&
      events.every(
        (x) =>
          typeof x === "object" &&
          x !== null &&
          typeof x.type === "string" &&
          typeof x.data === "object" &&
          x.data !== null
      )
  );
  try {
    await pool.query("select succeed_command($1,$2,$3)", [
      command_uuid,
      event_t,
      JSON.stringify(events),
    ]);
    res.status(200).send("ok");
  } catch (error: any) {
    if (String(error) === "error: invalid_event_t") {
      res.status(409).send("reprocess");
    } else if (error.constraint === "command_outcome_pkey") {
      res.status(202).send("already processed");
    } else {
      console.error(String(error));
      res.status(500).send("could not insert command");
    }
  }
});

app.get("/event-apis/pending-commands", async (_req, res) => {
  try {
    const outcome = await pool.query(
      "select * from queue join command on queue.command_uuid = command.command_uuid"
    );
    res.status(200).json(outcome.rows);
  } catch (error: any) {
    console.error(error);
    res.status(500).send("failed");
  }
});

type command_message = {
  type: "queued" | "succeeded" | "failed" | "aborted";
  status_t: number;
  command_uuid: string;
};

function parse_command_payload(payload: string): command_message {
  const m = payload.split(":");
  if (m.length !== 3) throw new Error(`invalid payload ${payload}`);
  const [status_t, type, command_uuid] = m;
  if (
    type === "queued" ||
    type === "succeeded" ||
    type === "failed" ||
    type === "aborted"
  ) {
    return { status_t: parseInt(status_t), type, command_uuid };
  } else {
    throw new Error(`invalid payload ${payload}`);
  }
}

const event_monitor = new EventMonitor(pool);

app.get("/event-apis/event-t", async (_req, res) => {
  res.status(200).json(await event_monitor.get_t());
});

app.get("/event-apis/get-events", async (req, res) => {
  const event_t_str = ((s) => (typeof s === "string" ? s : ""))(
    req.query["event_t"]
  );
  if (!event_t_str.match(/^\d+$/)) {
    res.status(401).send("invalid event_t");
    return;
  }
  const event_t = parseInt(event_t_str);
  if (Number.isNaN(event_t) || event_t > Number.MAX_SAFE_INTEGER) {
    res.status(401).send("invalid event_t");
    return;
  }
  res.status(200).json(await event_monitor.wait_events(event_t));
});

class CommandQueue {
  private status_t: number | undefined = undefined;
  private status_t_waiters: Set<(status_t: number) => void> = new Set();
  private status_t_pollers: Map<
    number,
    Set<(message: command_message) => void>
  > = new Map();

  public add_message(message: command_message) {
    this.advance_to(message);
  }

  public advance_to(message: command_message) {
    const { status_t } = message;
    if (this.status_t === undefined) {
      this.status_t = status_t;
      this.status_t_waiters.forEach((f) => f(status_t));
      this.status_t_waiters.clear();
    } else if (status_t > this.status_t) {
      this.status_t = status_t;
    }
    for (const t of this.status_t_pollers.keys()) {
      if (t <= status_t) {
        const s = this.status_t_pollers.get(t);
        s?.forEach((f) => f(message));
        this.status_t_pollers.delete(t);
      }
    }
  }

  public init_empty() {
    assert(this.status_t === undefined);
    this.status_t = 0;
    this.status_t_waiters.forEach((f) => f(0));
    this.status_t_waiters.clear();
  }

  public get_status_t(): Promise<number> {
    if (this.status_t === undefined) {
      return new Promise((resolve) => this.status_t_waiters.add(resolve));
    } else {
      return Promise.resolve(this.status_t);
    }
  }

  public poll_queue(t: number): Promise<command_message> {
    if (this.status_t && t <= this.status_t) {
      return fetch_command_message(t);
    } else {
      return new Promise((resolve) => {
        const s =
          this.status_t_pollers.get(t) ??
          ((s: Set<() => void>) => {
            this.status_t_pollers.set(t, s);
            return s;
          })(new Set());
        s.add(resolve);
      });
    }
  }
}

async function fetch_command_message(
  status_t: number
): Promise<command_message> {
  const data = await pool.query("select * from status where status_t = $1", [
    status_t,
  ]);
  console.log(data);
  const row = data.rows[0];
  console.log(row);
  return {
    status_t: parseInt(row.status_t, 10),
    command_uuid: row.command_uuid,
    type: row.status_type,
  };
}

const command_queue = new CommandQueue();

async function init_queue() {
  while (true) {
    try {
      const conn = await pool.connect();
      try {
        const p = new Promise(async (_resolve, reject) => {
          conn.on("error", (error) => reject(error));
          conn.on("end", () => reject(new Error("connection ended")));
          conn.on("notification", ({ payload }) => {
            if (!payload) return reject(new Error("no payload"));
            const message = parse_command_payload(payload);
            console.log(message);
            command_queue.add_message(message);
          });
          try {
            await conn.query("listen status");
            const res = await conn.query(
              "select coalesce(max(status_t), 0) as t from status"
            );
            const t = parseInt(res.rows[0].t);
            if (t > 0) {
              const message = await fetch_command_message(t);
              command_queue.advance_to(message);
            } else {
              command_queue.init_empty();
            }
          } catch (error: any) {
            reject(error);
          }
        });
        await p;
      } catch (error: any) {
        console.error(error);
        conn.removeAllListeners();
        conn.release(error);
      }
    } catch (error) {
      console.error(error);
      await sleep(1000);
    }
  }
}

app.get("/event-apis/status-t", async (_req, res) => {
  res.status(200).json(await command_queue.get_status_t());
});

function int_param(x: any) {
  const str = typeof x === "string" ? x : "";
  if (str.match(/^\d+$/)) {
    const n = parseInt(str);
    if (Number.isNaN(n) || n > Number.MAX_SAFE_INTEGER) {
      return undefined;
    }
    return n;
  } else {
    return undefined;
  }
}

app.get("/event-apis/poll-status", async (req, res) => {
  const status_t = int_param(req.query["status_t"]);
  if (status_t === undefined) {
    res.status(404).send("invalid status_t");
    return;
  }
  res.status(200).json(await command_queue.poll_queue(status_t));
});

app.get("/event-apis/recent-events", async (req, res) => {
  const limit = int_param(req.query["limit"]);
  if (limit === undefined) {
    res.status(404).send("invalid limit");
    return;
  }
  try {
    const outcome = await pool.query(
      `select * from event order by event_t desc limit $1`,
      [limit]
    );
    res.status(200).json(outcome.rows);
  } catch (error) {
    console.error(error);
    res.status(500).send("failed");
  }
});

init_queue();

app.listen(port, () => {
  console.log(`events server listening on port ${port}`);
});
