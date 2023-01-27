import * as hrana from "@libsql/hrana-client";
import * as kysely from "kysely";

export interface SqldDialectConfig {
    client: hrana.Client,
}

export class SqldDialect implements kysely.Dialect {
    #config: SqldDialectConfig

    constructor(config: SqldDialectConfig) {
        this.#config = config;
    }

    createAdapter(): kysely.DialectAdapter {
        return new kysely.SqliteAdapter();
    }

    createDriver(): kysely.Driver {
        return new HranaDriver(this.#config.client);
    }

    createIntrospector(db: kysely.Kysely<any>): kysely.DatabaseIntrospector {
        return new kysely.SqliteIntrospector(db);
    }

    createQueryCompiler(): kysely.QueryCompiler {
        return new kysely.SqliteQueryCompiler();
    }
}

export class HranaDriver implements kysely.Driver {
    client: hrana.Client

    constructor(client: hrana.Client) {
        this.client = client;
    }

    async init(): Promise<void> {
    }

    async acquireConnection(): Promise<HranaConnection> {
        return new HranaConnection(this.client.openStream());
    }

    async beginTransaction(
        connection: HranaConnection,
        _settings: kysely.TransactionSettings,
    ): Promise<void> {
        await connection.stream.execute("BEGIN");
    }

    async commitTransaction(connection: HranaConnection): Promise<void> {
        await connection.stream.execute("COMMIT");
    }

    async rollbackTransaction(connection: HranaConnection): Promise<void> {
        await connection.stream.execute("ROLLBACK");
    }

    async releaseConnection(connection: HranaConnection): Promise<void> {
        return connection.stream.close();
    }

    async destroy(): Promise<void> {
        this.client.close();
    }
}

export class HranaConnection implements kysely.DatabaseConnection {
    stream: hrana.Stream

    constructor(stream: hrana.Stream) {
        this.stream = stream;
    }

    async executeQuery<R>(compiledQuery: kysely.CompiledQuery): Promise<kysely.QueryResult<R>> {
        const stmt: hrana.Stmt = [compiledQuery.sql, compiledQuery.parameters as Array<hrana.Value>];
        const rowArray = await this.stream.query(stmt);
        return {
            numAffectedRows: BigInt(rowArray.rowsAffected),
            rows: rowArray,
        };
    }

    async *streamQuery<R>(
        _compiledQuery: kysely.CompiledQuery,
        _chunkSize: number,
    ): AsyncIterableIterator<kysely.QueryResult<R>> {
        throw new Error("Hrana protocol for sqld does not support streaming yet");
    }
}
