import * as hrana from "@libsql/hrana-client";

const client = hrana.open("ws://localhost:2023", process.env.JWT);
const stream = client.openStream();
console.log(await stream.queryValue("SELECT 1"));
client.close();
