//! This module contains a special [`Connection`] struct that can be used in
//! constrained wasm environments. This struct is separate from the main connection
//! struct in the root of the crate due to the nature of some wasm clients requiring
//! `!Send`/`!Sync` support.
//!
//! To use these connections in wasm, you must disable default features then enable
//! the backend below that you would like.
//!
//! Currently implemented wasm http backends are:
//! - [cloudflare workers] which can be accessed via the `cloudflare` feature flag.
//!
//! [cloudflare workers]: https://developers.cloudflare.com/workers
//!
//! # Example `Cargo.toml`
//! ```toml,ignore,no_run
//! [packages]
//! libsql = { version = "*", default-features = false, features = ["cloudflare"] }
//! ```
//!
//! # Example Rust usage
//!
//! ```rust,no_run
//! use libsql::wasm::Connection;
//!
//! let conn = Connection::open_cloudflare_worker("libsql://my-turso-db.turso.io", "my-auth-token");
//!
//! conn.execute("select 1", ()).await?;
//! conn.query("select 1", ()).await?;
//! ```

use crate::{
    hrana::{connection::HttpConnection, HttpSend},
    params::IntoParams,
    Rows,
};

cfg_cloudflare! {
    mod cloudflare;
    pub use cloudflare::CloudflareSender;
}

#[derive(Debug, Clone)]
pub struct Connection<T> {
    conn: HttpConnection<T>,
}

cfg_cloudflare! {
    impl Connection<CloudflareSender> {
        pub fn open_cloudflare_worker(url: impl Into<String>, auth_token: impl Into<String>) -> Self    {
            Connection {
                conn: HttpConnection::new(url.into(), auth_token.into(), CloudflareSender::new()),
            }
        }
    }
}

impl<T> Connection<T>
where
    T: for<'a> HttpSend<'a>,
{
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> crate::Result<u64> {
        tracing::trace!("executing `{}`", sql);
        let mut stmt = crate::hrana::Statement::new(self.conn.clone(), sql.to_string(), true);
        let rows = stmt.execute(&params.into_params()?).await?;
        Ok(rows as u64)
    }

    pub async fn execute_batch(&self, sql: &str) -> crate::Result<()> {
        let mut statements = Vec::new();
        let stmts = crate::parser::Statement::parse(sql);
        for s in stmts {
            let s = s?;
            statements.push(crate::hrana::proto::Stmt::new(s.stmt, false));
        }
        self.conn
            .raw_batch(statements)
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        Ok(())
    }

    pub async fn query(&self, sql: &str, params: impl IntoParams) -> crate::Result<Rows> {
        tracing::trace!("querying `{}`", sql);
        let mut stmt = crate::hrana::Statement::new(self.conn.clone(), sql.to_string(), true);
        stmt.query(&params.into_params()?).await
    }
}
