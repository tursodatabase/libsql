use crate::auth::{Authenticated, Authorized, Permission};
use crate::connection::Connection;
use crate::database::Database;
use crate::namespace::{MakeNamespace, NamespaceName, NamespaceStore};
use crate::query::Params;
use crate::query_analysis::Statement;
use crate::query_result_builder::IgnoreResult;
use crate::Result;
use rusqlite::Transaction;

#[derive(Debug)]
pub enum UpdateStatus {
    Pending,
    Finished,
    Failed(String),
}

pub enum LastUpdateStatus {
    None,
    Completed(String),
    NeedsRetry(i64),
}

#[derive(Debug)]
pub struct MultiDbUpdate {
    id: i64,
    sql: String,
    namespaces: Vec<(NamespaceName, UpdateStatus)>,
}

impl MultiDbUpdate {
    pub(super) fn new(
        id: i64,
        sql: String,
        namespaces: Vec<(NamespaceName, UpdateStatus)>,
    ) -> Self {
        MultiDbUpdate {
            id,
            sql,
            namespaces,
        }
    }

    pub async fn dry_run<N: MakeNamespace>(&mut self, store: &NamespaceStore<N>) -> Result<()> {
        for (namespace, _) in self.namespaces.iter_mut() {
            let conn_maker = store
                .with(namespace.clone(), |ns| ns.db.connection_maker())
                .await?;
            let conn = conn_maker.create().await?;
            Self::exec_with(conn, self.id, &self.sql, "ROLLBACK").await?;
        }
        Ok(())
    }

    pub async fn execute<N: MakeNamespace>(&mut self, store: &NamespaceStore<N>) -> Result<()> {
        for (namespace, _) in self.namespaces.iter_mut() {
            let conn_maker = store
                .with(namespace.clone(), |ns| ns.db.connection_maker())
                .await?;
            let conn = conn_maker.create().await?;
            Self::exec_with(conn, self.id, &self.sql, "COMMIT").await?;
        }
        Ok(())
    }

    async fn exec_with<C: Connection>(
        conn: C,
        id: i64,
        sql: &str,
        finalize_tx: &str,
    ) -> Result<()> {
        let auth = Authenticated::Authorized(Authorized {
            namespace: None,
            permission: Permission::FullAccess,
        });
        let mut batch = Vec::new();
        batch.push(query(r#"BEGIN"#)?);
        batch.push(query(
            r#"CREATE TABLE IF NOT EXISTS __libsql_schema_updates(
            id INTEGER PRIMARY KEY NOT NULL);"#,
        )?);
        batch.push(query(&format!(
            "INSERT INTO __libsql_schema_updates(id) VALUES ({id})"
        ))?);
        let statements = Statement::parse(sql);
        for res in statements {
            let stmt = res?;
            batch.push(crate::query::Query {
                stmt,
                params: Params::empty(),
                want_rows: false,
            });
        }
        batch.push(query(finalize_tx)?);
        conn.execute_batch(batch, auth, IgnoreResult, None).await?;
        Ok(())
    }

    pub(super) fn last_update_status(
        tx: &Transaction,
        shared_schema_name: &str,
    ) -> crate::Result<LastUpdateStatus> {
        let res: rusqlite::Result<(i64, String, u32)> = tx.query_row(
            r#"
            SELECT u.id, script, count(1)
            FROM multi_db_update_progress up
            JOIN multi_db_update u ON up.id = u.id
            WHERE shared_schema = ? AND up.status != 0
            ORDER BY up.id DESC
            TAKE 1"#,
            [shared_schema_name],
            |row| {
                let id = row.get(0)?;
                let script = row.get(1)?;
                let count = row.get(2)?;
                Ok((id, script, count))
            },
        );
        match res {
            Ok((_, script, 0)) => Ok(LastUpdateStatus::Completed(script)),
            Ok((id, _, _)) => Ok(LastUpdateStatus::NeedsRetry(id)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(LastUpdateStatus::None),
            Err(other_err) => Err(other_err.into()),
        }
    }
}

fn query(sql: &str) -> Result<crate::query::Query> {
    Ok(crate::query::Query {
        stmt: Statement::parse(sql).next().unwrap()?,
        params: Params::empty(),
        want_rows: false,
    })
}
