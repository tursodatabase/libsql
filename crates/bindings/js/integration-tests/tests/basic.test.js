import test from "ava";

test("basic usage", async (t) => {
  for (const provider of ["libsql", "sqlite"]) {
    await testBasicUsage(provider, t);
  }
});

const testBasicUsage = async (provider, t) => {
  const db = await connect(provider);

  db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");

  db.exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.org')");

  const userId = 1;

  const row = db.prepare("SELECT * FROM users WHERE id = ?").get(userId);

  t.is(row.name, "Alice");
};

const connect = async (provider) => {
  if (provider === "libsql") {
    const x = await import("libsql-js");
    const options = {};
    const db = new x.Database(":memory:", options);
    return db;
  }
  if (provider == "sqlite") {
    const x = await import("better-sqlite3");
    const options = {};
    const db = x.default(":memory:", options);
    return db;
  }
  throw new Error("Unknown provider: " + provider);
};
