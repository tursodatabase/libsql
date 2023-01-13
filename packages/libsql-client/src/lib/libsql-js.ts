import { Driver } from "./driver/Driver";
import { HttpDriver } from "./driver/HttpDriver";
import { SqliteDriver } from "./driver/SqliteDriver";

export type Config = {
  url: string,
};

/**
 * A SQL query result set row.
 */
export type Row = Record<string, string | number | boolean | null>;

/**
 * A SQL query result set.
 */
export type ResultSet = {
  /**
   * Query results.
   */
  results: Row[] | null;
  /**
   * Was the query successful?
   */
  success: boolean;
  meta: {
    /**
     * Time duration to execute the query.
     */
    duration: number;
  };
};

/**
 * A libSQL database connection.
 */
export class Connection {
  driver: Driver;

  constructor(driver: Driver) {
    this.driver = driver;
  }

  /**
   * Execute a SQL statement in a transaction.
   */
  async execute(sql: string): Promise<ResultSet> {
    const results = await this.transaction([sql]) 
    return results[0];
  }

  /**
   * Execute a batch of SQL statements in a transaction.
   */
  async transaction(stmts: string[]): Promise<ResultSet[]> {
    return await this.driver.transaction(stmts);
  }
}

export function connect(config: Config): Connection {
  const rawUrl = config.url;
  const url = new URL(rawUrl);
  if (url.protocol == "http:" || url.protocol == "https:") {
    return new Connection(new HttpDriver(url))
  } else {
    return new Connection(new SqliteDriver(rawUrl));
  }
}
