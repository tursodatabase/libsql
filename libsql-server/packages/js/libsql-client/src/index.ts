import { Config, Client } from "./shared-types.js";
import { HttpDriver } from "./http/http-driver.js";
import { SqliteDriver } from "./sqlite-driver.js";

export function createClient(config: Config): Client {
    const rawUrl = config.url;
    const url = new URL(rawUrl);
    if (url.protocol == "http:" || url.protocol == "https:") {
        return new Client(new HttpDriver(url));
    } else {
        return new Client(new SqliteDriver(rawUrl));
    }
}

export * from "./shared-types.js";
