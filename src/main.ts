import express from "express";
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

app.get("/", async (_req, res) => {
  try {
    const pgres = await pool.query("SELECT NOW() as t");
    res.send(`Hello World! Time is ${pgres.rows[0].t}`);
  } catch (err) {
    res.json(err);
  }
});

app.listen(port, () => {
  console.log(`events server listening on port ${port}`);
});
