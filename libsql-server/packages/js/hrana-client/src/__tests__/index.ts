import * as hrana from "..";

function withClient(f: (c: hrana.Client) => Promise<void>): () => Promise<void> {
    return async () => {
        const c = hrana.open("ws://localhost:2023");
        try {
            await f(c);
        } finally {
            c.close();
        }
    };
}

test("Stream.queryValue()", withClient(async (c) => {
    const s = c.openStream();
    expect(await s.queryValue("SELECT 1")).toStrictEqual(1);
    expect(await s.queryValue("SELECT 'elephant'")).toStrictEqual("elephant");
    expect(await s.queryValue("SELECT 42.5")).toStrictEqual(42.5);
    expect(await s.queryValue("SELECT NULL")).toStrictEqual(null);
}));

test("Stream.queryRow()", withClient(async (c) => {
    const s = c.openStream();
    
    const row = await s.queryRow(
        "SELECT 1 AS one, 'elephant' AS two, 42.5 AS three, NULL as four");
    expect(row[0]).toStrictEqual(1);
    expect(row[1]).toStrictEqual("elephant");
    expect(row[2]).toStrictEqual(42.5);
    expect(row[3]).toStrictEqual(null);

    expect(row[0]).toStrictEqual(row.one);
    expect(row[1]).toStrictEqual(row.two);
    expect(row[2]).toStrictEqual(row.three);
    expect(row[3]).toStrictEqual(row.four);
}));

test("Stream.query()", withClient(async (c) => {
    const s = c.openStream();

    await s.execute("BEGIN");
    await s.execute("DROP TABLE IF EXISTS t");
    await s.execute("CREATE TABLE t (one, two, three, four)");
    await s.execute(
        `INSERT INTO t VALUES
            (1, 'elephant', 42.5, NULL),
            (2, 'hippopotamus', '123', 0.0)`
    );

    const rows = await s.query("SELECT * FROM t ORDER BY one");
    expect(rows.length).toStrictEqual(2);
    expect(rows.rowsAffected).toStrictEqual(0);

    const row0 = rows[0];
    expect(row0[0]).toStrictEqual(1);
    expect(row0[1]).toStrictEqual("elephant");
    expect(row0["three"]).toStrictEqual(42.5);
    expect(row0["four"]).toStrictEqual(null);

    const row1 = rows[1];
    expect(row1["one"]).toStrictEqual(2);
    expect(row1["two"]).toStrictEqual("hippopotamus");
    expect(row1[2]).toStrictEqual("123");
    expect(row1[3]).toStrictEqual(0.0);
}));

test("Stream.execute()", withClient(async (c) => {
    const s = c.openStream();

    let res = await s.execute("BEGIN");
    expect(res.rowsAffected).toStrictEqual(0);

    res = await s.execute("DROP TABLE IF EXISTS t");
    expect(res.rowsAffected).toStrictEqual(0);

    res = await s.execute("CREATE TABLE t (num, word)");
    expect(res.rowsAffected).toStrictEqual(0);

    res = await s.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')");
    expect(res.rowsAffected).toStrictEqual(3);

    const rows = await s.query("SELECT * FROM t ORDER BY num");
    expect(rows.length).toStrictEqual(3);
    expect(rows.rowsAffected).toStrictEqual(0);

    res = await s.execute("DELETE FROM t WHERE num >= 2");
    expect(res.rowsAffected).toStrictEqual(2);

    res = await s.execute("UPDATE t SET num = 4, word = 'four'");
    expect(res.rowsAffected).toStrictEqual(1);

    res = await s.execute("DROP TABLE t");
    expect(res.rowsAffected).toStrictEqual(0);

    await s.execute("COMMIT");
}));

test("Stream.executeRaw()", withClient(async (c) => {
    const s = c.openStream();

    let res = await s.executeRaw({
        "sql": "SELECT 1 as one, ? as two, NULL as three",
        "args": [{"type": "text", "value": "1+1"}],
        "want_rows": true,
    });

    expect(res.cols).toStrictEqual([
        {"name": "one"},
        {"name": "two"},
        {"name": "three"},
    ]);
    expect(res.rows).toStrictEqual([
        [
            {"type": "integer", "value": "1"},
            {"type": "text", "value": "1+1"},
            {"type": "null"},
        ],
    ]);
}));

test("concurrent streams are separate", withClient(async (c) => {
    const s1 = c.openStream();
    await s1.execute("DROP TABLE IF EXISTS t");
    await s1.execute("CREATE TABLE t (number)");
    await s1.execute("INSERT INTO t VALUES (1)");

    const s2 = c.openStream();

    await s1.execute("BEGIN");

    await s2.execute("BEGIN");
    await s2.execute("INSERT INTO t VALUES (10)");

    expect(await s1.queryValue("SELECT SUM(number) FROM t")).toStrictEqual(1);
    expect(await s2.queryValue("SELECT SUM(number) FROM t")).toStrictEqual(11);
}));

test("concurrent operations are correctly ordered", withClient(async (c) => {
    const s = c.openStream();
    await s.execute("DROP TABLE IF EXISTS t");
    await s.execute("CREATE TABLE t (stream, value)");

    async function stream(streamId: number): Promise<void> {
        const s = c.openStream();

        let value = "s" + streamId;
        await s.execute(["INSERT INTO t VALUES (?, ?)", [streamId, value]]);

        const promises: Array<Promise<any>> = [];
        const expectedValues = [];
        for (let i = 0; i < 10; ++i) {
            const promise = s.queryValue([
                "UPDATE t SET value = value || ? WHERE stream = ? RETURNING value",
                ["_" + i, streamId],
            ]);
            value = value + "_" + i;
            promises.push(promise);
            expectedValues.push(value);
        }

        for (let i = 0; i < promises.length; ++i) {
            expect(await promises[i]).toStrictEqual(expectedValues[i]);
        }

        s.close();
    }

    const promises = [];
    for (let i = 0; i < 10; ++i) {
        promises.push(stream(i));
    }
    await Promise.all(promises);
}));
