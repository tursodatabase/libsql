import { Database } from "../";

test("execute", async () => {
  const db = new Database();
  await db.execute("CREATE TABLE users (email TEXT)");
  const stmt = db.execute("SELECT * FROM users");
  // TODO: check results
});