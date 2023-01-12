import "sqlite3";
import {
    Database,
} from "sqlite3";
import { ResultSet } from "../libsql-js";
import { Driver } from "./Driver";

export class SqliteDriver implements Driver {
    db: Database;
    constructor() {
        this.db = new Database(":memory:");
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
