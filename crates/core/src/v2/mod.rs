pub mod hrana;
pub mod rows;
pub mod statement;
pub mod transaction;

use std::sync::Arc;

use crate::box_clone_service::BoxCloneService;
use crate::params::{IntoParams, Params};
use crate::v1::TransactionBehavior;
use crate::Result;
pub use hrana::{Client, HranaError};

use hyper::client::HttpConnector;
use hyper::service::Service;
use hyper::Uri;
pub use rows::{Row, Rows};
use statement::LibsqlStmt;
pub use statement::Statement;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::ServiceExt;
use transaction::LibsqlTx;
pub use transaction::Transaction;

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    #[repr(C)]
    pub struct OpenFlags: ::std::os::raw::c_int {
        const SQLITE_OPEN_READ_ONLY = libsql_sys::ffi::SQLITE_OPEN_READONLY as i32;
        const SQLITE_OPEN_READ_WRITE = libsql_sys::ffi::SQLITE_OPEN_READWRITE as i32;
        const SQLITE_OPEN_CREATE = libsql_sys::ffi::SQLITE_OPEN_CREATE as i32;
    }
}

impl Default for OpenFlags {
    #[inline]
    fn default() -> OpenFlags {
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
    }
}

// TODO(lucio): Improve construction via
//      1) Move open errors into open fn rather than connect
//      2) Support replication setup
enum DbType {
    Memory,
    File {
        path: String,
        flags: OpenFlags,
    },
    Sync {
        db: crate::v1::Database,
    },
    Remote {
        url: String,
        auth_token: String,
        connector: ConnectorService,
    },
}

pub(crate) trait Socket:
    hyper::client::connect::Connection + AsyncRead + AsyncWrite + Send + Unpin + 'static + Sync
{
}

impl<T> Socket for T where
    T: hyper::client::connect::Connection + AsyncRead + AsyncWrite + Send + Unpin + 'static + Sync
{
}

impl hyper::client::connect::Connection for Box<dyn Socket> {
    fn connected(&self) -> hyper::client::connect::Connected {
        self.as_ref().connected()
    }
}

pub(crate) type ConnectorService =
    BoxCloneService<Uri, Box<dyn Socket>, Box<dyn std::error::Error + Sync + Send + 'static>>;

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
        Database::open_with_flags(db_path, OpenFlags::default())
    }

    pub fn open_with_flags(db_path: impl Into<String>, flags: OpenFlags) -> Result<Database> {
        Ok(Database {
            db_type: DbType::File {
                path: db_path.into(),
                flags,
            },
        })
    }

    /// Open a local database file with the ability to sync from snapshots from local filesystem.
    #[cfg(feature = "replication")]
    pub async fn open_with_local_sync(db_path: impl Into<String>) -> Result<Database> {
        let opts = crate::Opts::with_sync();
        let db = crate::v1::Database::open_with_opts(db_path, opts).await?;
        Ok(Database {
            db_type: DbType::Sync { db },
        })
    }

    /// Open a local database file with the ability to sync from a remote database.
    #[cfg(feature = "replication")]
    pub async fn open_with_remote_sync(
        db_path: impl Into<String>,
        url: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Database> {
        let opts = crate::Opts::with_http_sync(url, token);
        let db = crate::v1::Database::open_with_opts(db_path, opts).await?;
        Ok(Database {
            db_type: DbType::Sync { db },
        })
    }

    pub fn open_remote(url: impl Into<String>, auth_token: impl Into<String>) -> Result<Self> {
        Self::open_remote_with_connector(url, auth_token, HttpConnector::new())
    }

    // For now, only expose this for sqld testing purposes
    #[doc(hidden)]
    pub fn open_remote_with_connector<C>(
        url: impl Into<String>,
        auth_token: impl Into<String>,
        connector: C,
    ) -> Result<Self>
    where
        C: Service<Uri> + Send + Clone + Sync + 'static,
        C::Response: hyper::client::connect::Connection
            + AsyncRead
            + AsyncWrite
            + Send
            + Unpin
            + 'static
            + Sync,
        C::Future: Send + 'static,
        C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let svc = connector
            .map_err(|e| e.into())
            .map_response(|s| Box::new(s) as Box<dyn Socket>);
        Ok(Database {
            db_type: DbType::Remote {
                url: url.into(),
                auth_token: auth_token.into(),
                connector: ConnectorService::new(svc),
            },
        })
    }

    pub fn connect(&self) -> Result<Connection> {
        match &self.db_type {
            DbType::Memory => {
                let db = crate::v1::Database::open(":memory:", OpenFlags::default())?;
                let conn = db.connect()?;

                let conn = Arc::new(LibsqlConnection { conn });

                Ok(Connection { conn })
            }

            DbType::File { path, flags } => {
                let db = crate::v1::Database::open(path, *flags)?;
                let conn = db.connect()?;

                let conn = Arc::new(LibsqlConnection { conn });

                Ok(Connection { conn })
            }

            DbType::Sync { db } => {
                let conn = db.connect()?;

                let local = LibsqlConnection { conn };
                let writer = local.conn.writer().unwrap().clone();

                let remote = crate::replication::RemoteConnection::new(local, writer);

                let conn = Arc::new(remote);

                Ok(Connection { conn })
            }

            DbType::Remote {
                url,
                auth_token,
                connector,
            } => {
                let conn = Arc::new(hrana::Client::new_with_connector(
                    url,
                    auth_token,
                    connector.clone(),
                ));

                Ok(Connection { conn })
            }
        }
    }

    #[cfg(feature = "replication")]
    pub async fn sync(&self) -> Result<usize> {
        match &self.db_type {
            DbType::Sync { db } => db.sync().await,
            DbType::Memory => Err(crate::Error::SyncNotSupported("in-memory".into())),
            DbType::File { .. } => Err(crate::Error::SyncNotSupported("file".into())),
            DbType::Remote { .. } => Err(crate::Error::SyncNotSupported("remote".into())),
        }
    }

    #[cfg(feature = "replication")]
    pub fn sync_frames(&self, frames: crate::replication::Frames) -> Result<usize> {
        match &self.db_type {
            DbType::Sync { db } => db.sync_frames(frames),
            DbType::Memory => Err(crate::Error::SyncNotSupported("in-memory".into())),
            DbType::File { .. } => Err(crate::Error::SyncNotSupported("file".into())),
            DbType::Remote { .. } => Err(crate::Error::SyncNotSupported("remote".into())),
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait Conn {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64>;

    async fn execute_batch(&self, sql: &str) -> Result<()>;

    async fn prepare(&self, sql: &str) -> Result<Statement>;

    async fn transaction(&self, tx_behavior: TransactionBehavior) -> Result<Transaction>;

    fn is_autocommit(&self) -> bool;

    fn changes(&self) -> u64;

    fn last_insert_rowid(&self) -> i64;

    fn close(&self);
}

#[derive(Clone)]
pub struct Connection {
    pub(crate) conn: Arc<dyn Conn + Send + Sync>,
}

// TODO(lucio): Convert to using tryinto params
impl Connection {
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<u64> {
        self.conn.execute(sql, params.into_params()?).await
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql).await
    }

    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        self.conn.prepare(sql).await
    }

    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        let mut stmt = self.prepare(sql).await?;

        stmt.query(params).await
    }

    /// Begin a new transaction in DEFERRED mode, which is the default.
    pub async fn transaction(&self) -> Result<Transaction> {
        self.transaction_with_behavior(TransactionBehavior::Deferred)
            .await
    }

    /// Begin a new transaction in the given mode.
    pub async fn transaction_with_behavior(
        &self,
        tx_behavior: TransactionBehavior,
    ) -> Result<Transaction> {
        self.conn.transaction(tx_behavior).await
    }

    pub fn is_autocommit(&self) -> bool {
        self.conn.is_autocommit()
    }

    pub fn changes(&self) -> u64 {
        self.conn.changes()
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }

    pub fn close(&self) {
        self.conn.close()
    }
}

#[derive(Clone)]
pub(crate) struct LibsqlConnection {
    conn: crate::v1::Connection,
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
            inner: Box::new(LibsqlStmt(stmt)),
        })
    }

    async fn transaction(&self, tx_behavior: TransactionBehavior) -> Result<Transaction> {
        let tx = crate::v1::Transaction::begin(self.conn.clone(), tx_behavior)?;
        // TODO(lucio): Can we just use the conn passed to the transaction?
        Ok(Transaction {
            inner: Box::new(LibsqlTx(Some(tx))),
            conn: Connection {
                conn: Arc::new(self.clone()),
            },
        })
    }

    fn is_autocommit(&self) -> bool {
        self.conn.is_autocommit()
    }

    fn changes(&self) -> u64 {
        self.conn.changes()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }

    fn close(&self) {
        self.conn.disconnect()
    }
}
