import fetch from 'cross-fetch';
import { ResultSet, BoundStatement, Params } from "../libsql-js";
import { Driver } from "./Driver";

export class HttpDriver implements Driver {
    private url: URL;

    constructor(url: URL) {
        this.url = url;
    }

    async execute(stmt: string, params?: Params): Promise<ResultSet> {
        let rs;
        if (params === undefined) {
            rs = (await this.transaction([stmt]))[0];
        } else {
            rs = (await this.transaction([{sql: stmt, params: params}]))[0];
        }
        return rs
    }

    async transaction(stmts: (string | BoundStatement)[]): Promise<ResultSet[]> {
        if (stmts.length === 0) {
            return [];
        }

        const statements = buildStatements(["BEGIN", ...stmts, "COMMIT"]);

        const response = await fetch(this.url, {
            method: 'POST',
            body: JSON.stringify(statements),
        });
        if (response.status === 200) {
            const results = await response.json();
            validateTopLevelResults(results, statements.statements.length);
            const resultSets: ResultSet[] = [];
            for (var rsIdx = 1; rsIdx < results.length-1; rsIdx++) {
                const result = results[rsIdx];
                const rs = parseResultSet(result, rsIdx);
                // TODO duration needs to be provided by sqld
                rs.meta = { duration: 0 };
                resultSets.push(rs as ResultSet);
            }
            return resultSets;
        } else {
            const errorObj = await response.json();
            if (typeof errorObj === "object" && "error" in errorObj) {
                throw new Error(errorObj.error);
            } else {
                throw new Error(`${response.status} ${response.statusText}`);
            }
        }
    }
}


function buildStatements(stmts: (string | BoundStatement)[]) {
    let statements;
    if (typeof stmts[0] === "string") {
        statements = { statements: stmts }
    } else {
        const s = stmts as BoundStatement[];
        statements = {
            statements: s.map(st => {
                return { q: st.sql, params: st.params };
            })
        }
    }
    return statements
}

function validateTopLevelResults(results: any, numResults: number) {
    if (! Array.isArray(results)) {
        throw new Error("Response JSON was not an array");
    }
    if (results.length !== numResults) {
        throw new Error(`Response array did not contain expected ${numResults} results`);
    }
}

function parseResultSet(result: any, rsIdx: number): ResultSet {
    if (typeof result !== "object") {
        throw new Error(`Result ${rsIdx} was not an object`);
    }

    let rs: ResultSet;
    if ("results" in result) {
        validateSuccessResult(result, rsIdx);
        rs = result.results as ResultSet;
        validateRowsAndCols(rs, rsIdx);
        checkSuccess(rs);
        rs.success = true;
    } else if ("error" in result) {
        validateErrorResult(result, rsIdx);
        rs = result as ResultSet;
        rs.success = false;
    }
    else {
        throw new Error(`Result ${rsIdx} did not contain results or error`);
    }
    return rs;
}

function validateSuccessResult(result: any, rsIdx: number) {
    if (typeof result.results !== "object") {
        throw new Error(`Result ${rsIdx} results was not an object`);
    }
}

// "success" currently just means rows and columns are present in the result.
function checkSuccess(rs: ResultSet): boolean {
    return Array.isArray(rs.rows) && Array.isArray(rs.columns);
}

// Check that the number of values in each row equals the number of columns.
//
// TODO this could go further by checking the typeof each value and looking
// for inconsistencies among the rows.
function validateRowsAndCols(r: ResultSet, rsIdx: number) {
    const numCols = r.columns!.length;
    const rows = r.rows!;
    for (var i = 0; i < rows.length; i++) {
        if (rows[i].length !== numCols) {
            throw new Error(`Result ${rsIdx} row ${i} number of values != ${numCols}`);
        }
    }
}

function validateErrorResult(result: any, rsIdx: number) {
    if (typeof result.error !== "object") {
        throw new Error(`Result ${rsIdx} results was not an object`);
    }
    if (typeof result.error.message !== "string") {
        throw new Error(`Result ${rsIdx} error message was not a string`);
    }
}
