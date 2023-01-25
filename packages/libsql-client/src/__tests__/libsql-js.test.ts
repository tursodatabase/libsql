import { connect, ResultSet } from "../";

const SQLITE_URL = "file::memory:";

test("execute", async () => {
    const table = "_test_table_";
    const url = process.env.DB_URL ?? SQLITE_URL;
    const db = connect({ url });
    let rs: ResultSet;

    rs = await db.execute(`CREATE TABLE IF NOT EXISTS ${table} (email TEXT)`);
    assertEmptySuccessResult(rs);

    rs = await db.execute(`DELETE FROM ${table}`);
    assertEmptySuccessResult(rs);

    rs = await db.execute(`INSERT INTO ${table} (email) VALUES ('alice@example.com')`);
    assertEmptySuccessResult(rs);

    rs = await db.execute(`INSERT INTO ${table} (email) VALUES ('bob@example.com')`);
    assertEmptySuccessResult(rs);

    rs = await db.execute(`SELECT * FROM ${table}`);
    expect(rs.columns).toEqual(["email"]);
    expect(rs.rows).toEqual([["alice@example.com"], ["bob@example.com"]]);

    rs = await db.execute(`DROP TABLE ${table}`);
    assertEmptySuccessResult(rs);
});

test("execute-error", async () => {
    const url = process.env.DB_URL ?? SQLITE_URL;
    const db = connect({ url });
    let rs: ResultSet;

    rs = await db.execute("SELECT * FROM table_does_not_exist");
    expect(rs.success).toEqual(false);
    expect(rs.columns).toBeUndefined();
    expect(rs.rows).toBeUndefined();
    expect(typeof rs.error).toBe("object");
    expect(rs.error!.message).toBe("no such table: table_does_not_exist");
    expect(typeof rs.meta.duration).toBe("number");
});

test("execute-params", async () => {
    const table = "_test_table_";
    const url = process.env.DB_URL ?? SQLITE_URL;
    const db = connect({ url });
    let rs: ResultSet;

    rs = await db.execute(`CREATE TABLE IF NOT EXISTS ${table} (email TEXT)`);
    assertEmptySuccessResult(rs);

    rs = await db.execute(`DELETE FROM ${table}`);
    assertEmptySuccessResult(rs);

    const value = "alice@example.com";
    rs = await db.execute(`INSERT INTO ${table} (email) VALUES (?)`, [value]);
    assertEmptySuccessResult(rs);

    rs = await db.execute(`SELECT * FROM ${table} WHERE email = :email`, { email: value });
    expect(rs.columns).toEqual(["email"]);
    expect(rs.rows).toEqual([[value]]);

    rs = await db.execute(`DROP TABLE ${table}`);
    assertEmptySuccessResult(rs);
});

function assertEmptySuccessResult(rs: ResultSet) {
    expect(rs.success).toEqual(true);
    expect(rs.columns).toEqual([]);
    expect(rs.rows).toEqual([]);
    expect(typeof rs.meta.duration).toBe("number");
}
