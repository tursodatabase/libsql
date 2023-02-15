import initSqlJs from "sql.js";
import { BoundStatement, Params, ResultSet, SqlValue } from "../libsql-js";
import { Driver } from "./Driver";

export class BrowserSqliteDriver implements Driver {
    private sql?: initSqlJs.SqlJsStatic;
    private db?: initSqlJs.Database;

    constructor(url: string) {
        if (url !== "file::memory:" && url !== ":memory:") {
            console.warn(
                `BrowserSqliteDriver will ignore given db url '${url}' as in browser mode only the memory storage is available`
            );
        }
    }

    private async loadWasm() {
        this.sql = await initSqlJs({
            // Required to load the wasm binary asynchronously. Of course, you can host it wherever you want
            // You can omit locateFile completely when running in node
            locateFile: (file) => "https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/sql-wasm.js"
        });
        this.db = new this.sql.Database();
    }

    async execute(sql: string, params?: Params): Promise<ResultSet> {
        if (this.sql === undefined) {
            await this.loadWasm();
        }

        return await new Promise((resolve) => {
            let columns: string[];
            const rows: Record<string, any>[] = [];

            try {
                const stmt = this.db!.prepare(sql);
                columns = stmt.getColumnNames();
                stmt.bind(params as initSqlJs.BindParams);

                while (stmt.step()) {
                    rows.push(stmt.get());
                }
                stmt.free();
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
        try {
            const result = [];
            await this.execute("BEGIN TRANSACTION");
            for (const stmt of stmts) {
                let rs;
                if (typeof stmt === "string") {
                    rs = await this.execute(stmt);
                } else {
                    rs = await this.execute(stmt.q, stmt.params);
                }
                result.push(rs);
            }
            await this.execute("COMMIT TRANSACTION");
            return result;
        } catch (e) {
            await this.execute("ROLLBACK TRANSACTION");
            throw e;
        }
    }
}
