import { Config, Connection } from "./shared-types.js";
import { HttpDriver } from "./http/http-driver.js";
import { SqliteDriver } from "./sqlite-driver.js";

export function connect(config: Config): Connection {
    const rawUrl = config.url;
    const url = new URL(rawUrl);
    if (url.protocol == "http:" || url.protocol == "https:") {
        return new Connection(new HttpDriver(url));
    } else {
        return new Connection(new SqliteDriver(rawUrl));
    }
}

export * from "./shared-types.js";
