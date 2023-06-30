"use strict";

const { databaseNew } = require("./index.node");

class Database {
    constructor(url) {
        this.db = databaseNew(url);
    }

    all(sql, f) {
    }
}

module.exports = {
    Database: Database,
};
