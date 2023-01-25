import * as hrana from "..";

function openClient(): hrana.Client {
    return hrana.open("ws://localhost:2023");
}

test("simple query", async () => {
    const c = openClient();
    const s = c.openStream();
    expect(await s.queryValue("SELECT 1")).toStrictEqual(1);
    c.close();
})
