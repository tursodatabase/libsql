"use strict";

const { databaseNew, databaseExec, databasePrepare, statementGet } = require("./index.node");

class Database {
    constructor(url) {
        this.db = databaseNew(url);
    }

    exec(sql) {
        databaseExec.call(this.db, sql);
    }

    prepare(sql) {
        const stmt = databasePrepare.call(this.db, sql);
        return new Statement(stmt);
    }
}

class Statement {
    constructor(stmt) {
        this.stmt = stmt;
    }

    get(...bindParameters) {
        return statementGet.call(this.stmt, ...bindParameters);
    }
}

module.exports = {
    Database: Database,
};
