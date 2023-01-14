import { connect } from "@libsql/client"

async function example() {
  const url = process.env.DB_URL ?? "file::memory:";
  const config = {
    url
  };
  const db = connect(config);
  await db.transaction([
    "CREATE TABLE IF NOT EXISTS users (email TEXT)",
    "INSERT INTO users (email) VALUES ('alice@example.com')",
    "INSERT INTO users (email) VALUES ('bob@example.com')"
  ]);
  const rs = await db.execute("SELECT * FROM users");
  console.log(rs);
}

example()
