import express from "express";
import body_parser from "body-parser";

import pg from "pg";

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
  console.log({ command_uuid, command_type, command_data });
  try {
    const outcome = await pool.query("select enqueue_command($1,$2,$3)", [
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

app.listen(port, () => {
  console.log(`events server listening on port ${port}`);
});
