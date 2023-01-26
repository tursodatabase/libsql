import type * as proto from "./proto.js";

/** A statement that you can send to the database. Either a plain SQL string, or an SQL string together with
 * values for the `?` parameters.
 */
export type Stmt =
    | string
    | [string, Array<Value>];

export function stmtToProto(stmtLike: Stmt, wantRows: boolean): proto.Stmt {
    let sql;
    let args: Array<proto.Value> = [];
    if (typeof stmtLike === "string") {
        sql = stmtLike;
    } else {
        sql = stmtLike[0];
        args = stmtLike[1].map(valueToProto);
    }

    return {"sql": sql, "args": args, "want_rows": wantRows};
}

/** JavaScript values that you can get from the database. */
export type Value =
    | null
    | string
    | number
    | ArrayBuffer;

export function valueToProto(value: Value): proto.Value {
    if (value === null) {
        return {"type": "null"};
    } else if (typeof value === "number") {
        return {"type": "float", "value": +value};
    } else if (value instanceof ArrayBuffer) {
        throw new Error("ArrayBuffer is not yet supported");
    } else {
        return {"type": "text", "value": ""+value};
    }
}

export function valueFromProto(value: proto.Value): Value {
    if (value["type"] === "null") {
        return null;
    } else if (value["type"] === "integer") {
        return parseInt(value["value"], 10);
    } else if (value["type"] === "float") {
        return value["value"];
    } else if (value["type"] === "text") {
        return value["value"];
    } else if (value["type"] === "blob") {
        throw new Error("blob is not yet supported");
    } else {
        throw new Error("Unexpected value type");
    }
}

export function stmtResultFromProto(result: proto.StmtResult): StmtResult {
    return {rowsAffected: result["affected_row_count"]};
}

export function rowArrayFromProto(result: proto.StmtResult): RowArray {
    const array = new RowArray(result["affected_row_count"]);
    for (const row of result["rows"]) {
        array.push(rowFromProto(result, row));
    }
    return array;
}

export function rowFromProto(result: proto.StmtResult, row: Array<proto.Value>): Row {
    const array = row.map((value) => valueFromProto(value));

    for (let i = 0; i < result["cols"].length; ++i) {
        const colName = result["cols"][i]["name"];
        if (colName && !Object.hasOwn(array, colName)) {
            Object.defineProperty(array, colName, {
                value: array[i],
                enumerable: true,
            });
        }
    }

    return array;
}

export interface StmtResult {
    rowsAffected: number;
}

export class RowArray extends Array<Row> implements StmtResult {
    constructor(public rowsAffected: number) {
        super();
        Object.setPrototypeOf(this, RowArray.prototype);
    }
}

export type Row = any;

export function errorFromProto(error: proto.Error): Error {
    return new Error(`Server returned error ${JSON.stringify(error["message"])}`);
}

