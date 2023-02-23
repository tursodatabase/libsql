import { HttpDriver } from "./http-driver.js";
import { Config, Client } from "./../shared-types.js";

export function createClient(config: Config): Client {
    const rawUrl = config.url;
    const url = new URL(rawUrl);
    if (url.protocol == "http:" || url.protocol == "https:") {
        return new Client(new HttpDriver(url));
    } else {
        throw new Error(
            "libsql-http-client package supports only http connections. For memory of file storage, please use libsql-client package."
        );
    }
}

export * from "./../shared-types.js";
