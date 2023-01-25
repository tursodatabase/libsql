import DatabaseConstructor, { Database, SqliteError, Statement } from "better-sqlite3";
import { BoundStatement, Params, ResultSet, SqlValue } from "../libsql-js";
import { Driver } from "./Driver";

export class SqliteDriver implements Driver {
    private db: Database;

    constructor(url: string) {
        this.db = new DatabaseConstructor(url.substring(5));
    }

    async execute(sql: string, params?: Params): Promise<ResultSet> {
        return await new Promise((resolve) => {
            let columns: string[];
            let rows: any[];

            try {
                const stmt = this.db.prepare(sql);
                if (stmt.reader) {
                    columns = stmt.columns().map((c) => c.name);
                    if (params === undefined) {
                        rows = stmt.all();
                    } else {
                        rows = stmt.all(params);
                    }
                    rows = rows.map((row) => {
                        return columns.map((column) => row[column]);
                    });
                } else {
                    columns = [];
                    rows = [];
                    if (params === undefined) {
                        stmt.run();
                    } else {
                        stmt.run(params);
                    }
                }
            } catch (e: any) {
                resolve({
                    success: false,
                    error: { message: e.message },
                    meta: { duration: 0 }
                });
                return;
            }

            resolve({
                success: true,
                columns,
                rows,
                meta: { duration: 0 }
            });
        });
    }

    async transaction(stmts: (string | BoundStatement)[]): Promise<ResultSet[]> {
        // TODO this is not really a "transaction", however, the better-sqlite3
        // transaction API blocks the event loop and does not work with async
        // functions. We need to investigate working the transaction manually
        // with begin/commit, however, that likely does not support concurrent
        // overlapping invocations from multiple procedures in the same process.
        //
        // https://github.com/WiseLibs/better-sqlite3/blob/HEAD/docs/api.md#transactionfunction---function

        const result = [];
        for (const stmt of stmts) {
            let rs;
            if (typeof stmt === "string") {
                rs = await this.execute(stmt);
            } else {
                rs = await this.execute(stmt.sql, stmt.params);
            }
            result.push(rs);
        }
        return result;
    }
}
