import { connect } from "@libsql/client"

async function example() {
  const config = {
    url: "http://localhost:8080"
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
