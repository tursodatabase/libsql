mod hrana;
mod rows;
mod statement;

use std::sync::Arc;

use crate::{Params, Result};
pub use hrana::{Client, HranaError};

pub use rows::{Row, Rows};
use statement::LibsqlStmt;
pub use statement::Statement;

// TODO(lucio): Improve construction via
//      1) Move open errors into open fn rather than connect
//      2) Support replication setup
enum DbType {
    Memory,
    File { path: String },
    Remote { url: String },
}

pub struct Database {
    db_type: DbType,
}

impl Database {
    pub fn open_in_memory() -> Result<Self> {
        Ok(Database {
            db_type: DbType::Memory,
        })
    }

    pub fn open(db_path: impl Into<String>) -> Result<Database> {
        Ok(Database {
            db_type: DbType::File {
                path: db_path.into(),
            },
        })
    }

    pub fn open_remote(url: impl Into<String>) -> Result<Self> {
        Ok(Database {
            db_type: DbType::Remote { url: url.into() },
        })
    }

    pub fn connect(&self) -> Result<Connection> {
        match &self.db_type {
            DbType::Memory => {
                let db = crate::Database::open(":memory:")?;
                let conn = db.connect()?;

                let conn = Arc::new(LibsqlConnection { conn });

                Ok(Connection { conn })
            }

            DbType::File { path } => {
                let db = crate::Database::open(path)?;
                let conn = db.connect()?;

                let conn = Arc::new(LibsqlConnection { conn });

                Ok(Connection { conn })
            }

            DbType::Remote { url } => {
                let conn = Arc::new(hrana::Client::new(url, ""));

                Ok(Connection { conn })
            }
        }
    }
}

#[async_trait::async_trait]
trait Conn {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64>;

    async fn execute_batch(&self, sql: &str) -> Result<()>;

    async fn prepare(&self, sql: &str) -> Result<Statement>;
}

#[derive(Clone)]
pub struct Connection {
    conn: Arc<dyn Conn + Send + Sync>,
}

impl Connection {
    pub async fn execute(&self, sql: &str, params: impl Into<Params>) -> Result<u64> {
        self.conn.execute(sql, params.into()).await
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql).await
    }

    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        self.conn.prepare(sql).await
    }
}

struct LibsqlConnection {
    conn: crate::Connection,
}

#[async_trait::async_trait]
impl Conn for LibsqlConnection {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64> {
        self.conn.execute(sql, params)
    }

    async fn execute_batch(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql)
    }

    async fn prepare(&self, sql: &str) -> Result<Statement> {
        let sql = sql.to_string();

        let stmt = self.conn.prepare(sql)?;

        Ok(Statement {
            inner: Arc::new(LibsqlStmt(stmt)),
        })
    }
}
