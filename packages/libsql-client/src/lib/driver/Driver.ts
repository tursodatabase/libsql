import { BoundStatement, Params, ResultSet } from "../libsql-js";

export interface Driver {
    execute(stmt: string, params?: Params): Promise<ResultSet>;
    transaction(stmts: (string | BoundStatement)[]): Promise<ResultSet[]>;
}
