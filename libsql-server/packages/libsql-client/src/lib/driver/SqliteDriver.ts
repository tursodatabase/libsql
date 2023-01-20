import DatabaseConstructor, {Database} from "better-sqlite3";
import { ResultSet } from "../libsql-js";
import { Driver } from "./Driver";

export class SqliteDriver implements Driver {
    db: Database;
    constructor(url: string) {
        this.db = new DatabaseConstructor(url.substring(5))
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
            const stmt = this.db.prepare(sql);
            var columns: string[];
            var rows: any[];
            if (stmt.reader) {
                columns = stmt.columns().map(c => c.name);
                rows = stmt.all().map(row => {
                    return columns.map(column => row[column]);
                });
            } else {
                columns = [];
                rows = [];
                stmt.run();
            }
            // FIXME: error handling
            const rs = {
                columns,
                rows,
                success: true,
                meta: {
                    duration: 0,
                },
            };
            resolve(rs);
        });
    }
}
