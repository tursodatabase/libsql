import { connect } from "../";

test("execute", async () => {
  const config = {};
  const db = connect(config);
  await db.execute("CREATE TABLE users (email TEXT)");
  const stmt = db.execute("SELECT * FROM users");
  // TODO: check results
});
