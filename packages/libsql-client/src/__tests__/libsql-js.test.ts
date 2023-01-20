import { connect } from "../";

test("execute", async () => {
  const url = process.env.DB_URL ?? "file::memory:";
  const config = { url };
  const db = connect(config);
  await db.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)");
  await db.execute("DELETE FROM users");
  await db.execute("INSERT INTO users (email) VALUES ('alice@example.com')");
  await db.execute("INSERT INTO users (email) VALUES ('bob@example.com')");
  const rs = await db.execute("SELECT * FROM users");
  expect(rs.columns).toEqual(['email']);
  expect(rs.rows).toEqual([['alice@example.com'], ['bob@example.com']]);
});
