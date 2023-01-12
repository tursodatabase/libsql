import { ResultSet } from "../libsql-js"

export interface Driver {
    transaction(sql: string[]): Promise<ResultSet[]>;
}
