import libsql from "libsql-js";
import test from "ava";

test("basic usage", (t) => {
  const options = {};
  const db = new libsql.Database(":memory:", options);

  db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");

  db.exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.org')");

  const userId = 1;

  const row = db.prepare("SELECT * FROM users WHERE id = ?").get(userId, "foo");

  t.is(row.name, "Alice");
});
