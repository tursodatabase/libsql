import fetch from 'cross-fetch';
import { Row, ResultSet } from "../libsql-js";
import { Driver } from "./Driver";

export class HttpDriver implements Driver {
    url: URL;

    constructor(url: URL) {
        this.url = url;
    }

    async transaction(sql: string[]): Promise<ResultSet[]> {
        const query = {
            statements: sql
        };
        const response = await fetch(this.url, {
            method: 'POST',
            body: JSON.stringify(query),
        });
        const results = await response.json() as any[];
        return results.map(rs => {
            return rs.results as ResultSet;
        });
    }
}
