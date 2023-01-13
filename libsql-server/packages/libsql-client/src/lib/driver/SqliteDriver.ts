import * as sqlite3 from "sqlite3";
import { ResultSet } from "../libsql-js";
import { Driver } from "./Driver";

export class SqliteDriver implements Driver {
    db: sqlite3.Database;
    constructor(url: string) {
        this.db = new sqlite3.Database(url, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE | sqlite3.OPEN_FULLMUTEX | sqlite3.OPEN_URI);
    }
    async transaction(sqls: string[]): Promise<ResultSet[]> {
        const result = [];
        for (let sql of sqls) {
            const rs = await this.execute(sql);
            result.push(rs);
        }
        return result;
    }
    async execute(sql: string): Promise<ResultSet> {
        return await new Promise(resolve => {
            this.db.all(sql, (err, rows) => {
                // FIXME: error handling
                const rs = {
                    results: rows,
                    success: true,
                    meta: {
                        duration: 0,
                    },
                };
                resolve(rs);
            })
        });
    }
}
