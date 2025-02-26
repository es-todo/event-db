import assert from "node:assert";
import pg from "pg";

type events = { type: string; data: any }[];

type pool = pg.Pool;

async function get_events(pool: pool, event_t: number): Promise<events> {
  while (true) {
    try {
      const res = await pool.query(
        "select event_data as data from event where event_t = $1",
        [event_t]
      );
      assert(res.rows.length === 1);
      const events = res.rows[0].data;
      assert(Array.isArray(events));
      console.log({ event_t, events });
      return events;
    } catch (error) {
      console.error(error);
      sleep(100);
    }
  }
}

export class EventMonitor {
  private t: number | undefined = undefined;
  private waiters: Set<(t: number) => void> = new Set();
  private pollers: Map<number, Set<(events: events) => void>> = new Map();
  private pool: pool;

  constructor(pool: pool) {
    this.pool = pool;
    this.init();
  }

  public get_t(): Promise<number> {
    if (this.t === undefined) {
      return new Promise((resolve) => this.waiters.add(resolve));
    } else {
      return Promise.resolve(this.t);
    }
  }

  public wait_events(t: number): Promise<events> {
    console.log({ t, mine: this.t });
    if (this.t === undefined || t > this.t) {
      return new Promise((resolve) => {
        const s =
          this.pollers.get(t) ??
          ((s: Set<(events: events) => void>) => {
            this.pollers.set(t, s);
            return s;
          })(new Set());
        s.add(resolve);
      });
    } else {
      return get_events(this.pool, t);
    }
  }

  private async dequeue_all(event_t: number, s: Set<(events: events) => void>) {
    const events = await get_events(this.pool, event_t);
    s.forEach((f) => f(events));
  }

  private note_t(t: number) {
    console.log({ noting: t });
    if (this.t === undefined) {
      this.t = t;
      this.waiters.forEach((f) => f(t));
      this.waiters.clear();
      for (const k of this.pollers.keys()) {
        if (k <= t) {
          const s = this.pollers.get(k);
          assert(s);
          this.pollers.delete(k);
          this.dequeue_all(k, s);
        }
      }
    }
    while (this.t < t) {
      const s = this.pollers.get(this.t);
      if (s) {
        this.pollers.delete(this.t);
        this.dequeue_all(this.t, s);
      }
      this.t += 1;
    }
  }

  private async init() {
    while (true) {
      try {
        const conn = await this.pool.connect();
        const p = new Promise(async (_resolve, reject) => {
          conn.on("error", (error) => reject(error));
          conn.on("end", () => reject(new Error("connection ended")));
          conn.on("notification", (message) => {
            assert(message.payload !== undefined);
            this.note_t(parseInt(message.payload));
          });
          try {
            await conn.query("listen event_stream");
            const data = await conn.query(
              "select coalesce(max(event_t), 0) as t from event"
            );
            this.note_t(parseInt(data.rows[0].t));
          } catch (error: any) {
            conn.removeAllListeners();
            conn.release(error);
            reject(error);
          }
        });
        await p;
      } catch (error) {
        console.error(error);
      }
    }
  }
}
