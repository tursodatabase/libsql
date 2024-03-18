use itertools::Itertools;
use libsql_replication::rpc::metadata;
use prost::Message;
use rusqlite::{params, OptionalExtension};

use crate::connection::config::DatabaseConfig;
use crate::connection::program::Program;
use crate::namespace::NamespaceName;
use crate::schema::status::{MigrationJobProgress, MigrationJobSummary};

use super::{
    status::{MigrationJob, MigrationTask},
    Error, MigrationDetails, MigrationJobStatus, MigrationSummary, MigrationTaskStatus,
};

pub(super) fn setup_schema(conn: &mut rusqlite::Connection) -> Result<(), Error> {
    conn.execute("PRAGMA foreign_key=ON", ())?;
    let txn = conn.transaction()?;

    // this table contains all the migration jobs
    txn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS jobs (
                job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                schema TEXT NOT NULL,
                migration TEXT NOT NULL,
                status INTEGER,
                error TEXT,
                finished BOOLEAN GENERATED ALWAYS AS ({})
            )
            ",
            MigrationJobStatus::finished_states()
                .into_iter()
                .map(|s| format!("status = {}", *s as u64))
                .join(" OR ")
        ),
        (),
    )?;
    // this table contains a list of all the that need to be performed for each migration job
    txn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS pending_tasks (
            task_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            target_namespace TEXT NOT NULL,
            status INTEGER,
            error TEXT,
            finished BOOLEAN GENERATED ALWAYS AS ({}),
            FOREIGN KEY (job_id) REFERENCES jobs (job_id)
        )
        ",
            MigrationTaskStatus::finished_states()
                .into_iter()
                .map(|s| format!("status = {}", *s as u64))
                .join(" OR ")
        ),
        (),
    )?;
    // This temporary table hold the list of tasks that are currently being processed
    txn.execute(
        "
        CREATE TEMPORARY TABLE IF NOT EXISTS enqueued_tasks (task_id)
        ",
        (),
    )?;

    // create a trigger that removes the task from enqueued tasks whenever it's status was updated.
    // The assumption is that the status of the task is only ever updated if work on it is
    // finished.
    txn.execute(
        "
        CREATE TEMPORARY TRIGGER IF NOT EXISTS remove_from_enqueued_tasks 
        AFTER UPDATE OF status ON pending_tasks
        BEGIN
            DELETE FROM enqueued_tasks WHERE task_id = old.task_id;
        END
        ",
        (),
    )?;

    txn.commit()?;
    Ok(())
}

pub(crate) fn has_pending_migration_jobs(
    conn: &rusqlite::Connection,
    schema: &NamespaceName,
) -> Result<bool, Error> {
    let has_pending = conn.query_row(
        "SELECT count(1) FROM jobs WHERE schema = ? AND finished = false",
        [schema.as_str()],
        |row| Ok(row.get::<_, usize>(0)? != 0),
    )?;

    Ok(has_pending)
}

pub(crate) fn schema_has_linked_dbs(
    conn: &rusqlite::Connection,
    schema: &NamespaceName,
) -> Result<bool, Error> {
    let has_linked = conn.query_row(
        "SELECT count(1) FROM (SELECT 0 FROM shared_schema_links WHERE shared_schema_name = ? LIMIT 1)",
        [schema.as_str()],
        |row| Ok(row.get::<_, usize>(0)? != 0),
    )?;

    Ok(has_linked)
}

/// Create a migration job, and returns the job_id
pub(super) fn register_schema_migration_job(
    conn: &mut rusqlite::Connection,
    schema: &NamespaceName,
    migration: &Program,
) -> Result<i64, Error> {
    let txn = conn.transaction()?;

    // get the config for the schema and validate that it's actually a schema
    let mut stmt =
        txn.prepare("SELECT namespace, config FROM namespace_configs where namespace = ?")?;
    let mut rows = stmt.query([schema.as_str()])?;
    let Some(row) = rows.next()? else {
        return Err(Error::SchemaDoesntExist(schema.clone()));
    };
    let config_bytes = row.get_ref(1)?.as_blob().unwrap();
    // TODO: handle corrupted meta
    let config = DatabaseConfig::from(&metadata::DatabaseConfig::decode(config_bytes).unwrap());
    if !config.is_shared_schema {
        return Err(Error::NotASchema(schema.clone()));
    }

    drop(rows);

    stmt.finalize()?;

    let migration_serialized = serde_json::to_string(&migration).unwrap();
    // this query inserts the new job in jobs only if there are no other unfinnished
    // tasks for that schema
    let row_changed = txn.execute(
        "
        INSERT INTO jobs (schema, migration, status)
        SELECT ?1, ?2, ?3 
        WHERE NOT (SELECT COUNT(1) FROM (SELECT 0 from jobs WHERE schema = ?1 AND finished = false))
        ",
        (
            schema.as_str(),
            &migration_serialized,
            MigrationJobStatus::WaitingDryRun as u64,
        ),
    )?;

    if row_changed == 1 {
        let job_id = txn.last_insert_rowid();
        txn.execute(
            "
            INSERT INTO
            pending_tasks (job_id, target_namespace, status)
            SELECT job_id, namespace, status
            FROM shared_schema_links 
            CROSS JOIN (SELECT ? as job_id, ? as status)
            WHERE shared_schema_name = ?",
            (
                job_id,
                MigrationTaskStatus::Enqueued as u64,
                schema.as_ref(),
            ),
        )?;

        txn.commit()?;
        Ok(job_id)
    } else {
        Err(Error::MigrationJobAlreadyInProgress(schema.clone()))
    }
}

/// returns a batch of tasks for job_id that are in the passed status
pub(super) fn get_next_pending_migration_tasks_batch(
    conn: &mut rusqlite::Connection,
    job_id: i64,
    status: MigrationTaskStatus,
    limit: usize,
) -> Result<Vec<MigrationTask>, Error> {
    let txn = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let tasks = txn
        .prepare(
            "SELECT task_id, target_namespace, status, job_id 
            FROM pending_tasks 
            WHERE job_id = ? AND status = ? AND task_id NOT IN (select * from enqueued_tasks)
            LIMIT ?",
        )?
        .query_map((job_id, status as u64, limit), |row| {
            let task_id = row.get::<_, i64>(0)?;
            let namespace = NamespaceName::from_string(row.get::<_, String>(1)?).unwrap();
            let status = MigrationTaskStatus::from_int(row.get::<_, u64>(2)?);
            let job_id = row.get::<_, i64>(3)?;
            Ok(MigrationTask {
                namespace,
                status,
                job_id,
                task_id,
            })
        })?
        .map(|r| r.map_err(Into::into))
        .collect::<Result<Vec<_>, Error>>()?;

    for task in tasks.iter() {
        txn.execute("INSERT INTO enqueued_tasks VALUES (?)", [task.task_id])?;
    }

    txn.commit()?;
    Ok(tasks)
}

/// returns a batch of tasks that are not in a finished state
pub(super) fn get_unfinished_task_batch(
    conn: &mut rusqlite::Connection,
    job_id: i64,
    limit: usize,
) -> Result<Vec<MigrationTask>, Error> {
    let txn = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let tasks = txn
        .prepare(
            "SELECT task_id, target_namespace, status, job_id 
            FROM pending_tasks 
            WHERE job_id = ? AND finished = false AND task_id NOT IN (select * from enqueued_tasks)
            LIMIT ?",
        )?
        .query_map((job_id, limit), |row| {
            let task_id = row.get::<_, i64>(0)?;
            let namespace = NamespaceName::from_string(row.get::<_, String>(1)?).unwrap();
            let status = MigrationTaskStatus::from_int(row.get::<_, u64>(2)?);
            let job_id = row.get::<_, i64>(3)?;
            Ok(MigrationTask {
                namespace,
                status,
                job_id,
                task_id,
            })
        })?
        .map(|r| r.map_err(Into::into))
        .collect::<Result<Vec<_>, Error>>()?;

    for task in tasks.iter() {
        txn.execute("INSERT INTO enqueued_tasks VALUES (?)", [task.task_id])?;
    }

    txn.commit()?;
    Ok(tasks)
}

pub(super) fn update_meta_task_status(
    conn: &mut rusqlite::Connection,
    task: &MigrationTask,
    error: Option<&str>,
) -> Result<(), Error> {
    assert!(error.is_none() || task.status.is_failure());
    let txn = conn.transaction()?;
    txn.execute(
        "UPDATE pending_tasks SET status = ?, error = ? WHERE task_id = ?",
        (task.status as u64, error, task.task_id),
    )?;
    txn.commit()?;
    Ok(())
}

/// Attempt to set the job to DryRunSuccess.
/// Checks that:
/// - current state is WaitinForDryRun
/// - all tasks are DryRunSuccess
/// - this function is idempotent
/// returns the new status if it was updated
pub(super) fn job_step_dry_run_success(
    conn: &mut rusqlite::Connection,
    job_id: i64,
) -> Result<(), Error> {
    let row_changed = conn.execute(
        "
        WITH tasks AS (SELECT * FROM pending_tasks WHERE job_id = ?1)
        UPDATE jobs 
        SET status = ?2
        WHERE job_id = ?1
        AND (status = ?3 OR status = ?2)
        AND (SELECT count(1) from tasks) = (SELECT count(1) FROM tasks WHERE status = ?4)",
        (
            job_id,
            MigrationJobStatus::DryRunSuccess as u64,
            MigrationJobStatus::WaitingDryRun as u64,
            MigrationTaskStatus::DryRunSuccess as u64,
        ),
    )?;

    if row_changed == 0 {
        return Err(Error::CantStepJobDryRunSuccess);
    }

    Ok(())
}

pub(super) fn update_job_status(
    conn: &mut rusqlite::Connection,
    job_id: i64,
    status: MigrationJobStatus,
    error: Option<&str>,
) -> Result<(), Error> {
    conn.execute(
        "UPDATE jobs SET status = ?, error = coalesce(?, error) WHERE job_id = ?",
        (status as u64, error, job_id),
    )?;
    Ok(())
}

pub(super) fn get_next_pending_migration_job(
    conn: &mut rusqlite::Connection,
) -> Result<Option<MigrationJob>, Error> {
    let txn = conn.transaction()?;
    let mut job = txn
        .query_row(
            "SELECT job_id, status, migration, schema
            FROM jobs
            WHERE status != ? AND status != ?
            LIMIT 1",
            (
                MigrationJobStatus::RunSuccess as u64,
                MigrationJobStatus::RunFailure as u64,
            ),
            |row| {
                let job_id = row.get::<_, i64>(0)?;
                let status = MigrationJobStatus::from_int(row.get::<_, u64>(1)?);
                let migration = serde_json::from_str(row.get_ref(2)?.as_str()?).unwrap();
                let schema = NamespaceName::from_string(row.get::<_, String>(3)?).unwrap();
                Ok(MigrationJob {
                    schema,
                    job_id,
                    status,
                    migration,
                    progress: Default::default(),
                    task_error: None,
                })
            },
        )
        .optional()?;

    if let Some(ref mut job) = job {
        txn.prepare(
            "
                SELECT status, count(1)
                FROM pending_tasks 
                WHERE job_id = ?
                GROUP BY status",
        )?
        .query_map([job.job_id], |row| {
            job.progress[row.get::<_, usize>(0)?] = row.get::<_, usize>(1)?;
            Ok(())
        })?
        .collect::<Result<(), rusqlite::Error>>()?;
    }

    Ok(job)
}

pub fn get_migration_details(
    conn: &mut rusqlite::Connection,
    schema: NamespaceName,
    job_id: u64,
) -> crate::Result<Option<MigrationDetails>> {
    let Some((status, error)) = conn
        .query_row(
            "SELECT status, error
            FROM jobs
            WHERE schema = ? AND job_id = ?",
            params![schema.as_str(), job_id],
            |r| {
                let status = MigrationJobStatus::from_int(r.get::<_, u64>(0)?);
                let error: Option<String> = r.get(1)?;
                Ok((status, error))
            },
        )
        .optional()?
    else {
        return Ok(None);
    };

    let mut stmt = conn.prepare(
        "SELECT target_namespace, status, error
            FROM pending_tasks
            WHERE job_id = ?",
    )?;
    let rows = stmt.query([job_id])?.mapped(|r| {
        let target_namespace = r.get(0)?;
        let status: Option<u64> = r.get(1)?;
        let error: Option<String> = r.get(2)?;
        Ok(MigrationJobProgress {
            namespace: target_namespace,
            status: status.map(MigrationJobStatus::from_int),
            error,
        })
    });
    let mut progress = Vec::new();
    for row in rows {
        progress.push(row?);
    }
    Ok(Some(MigrationDetails {
        job_id,
        status,
        error,
        progress,
    }))
}

pub fn get_migrations_summary(
    conn: &mut rusqlite::Connection,
    schema: NamespaceName,
) -> crate::Result<MigrationSummary> {
    let schema_version: i64 = conn.query_row("PRAGMA schema_version;", (), |r| r.get(0))?;
    let mut stmt = conn.prepare(
        "SELECT job_id, status
            FROM jobs
            WHERE schema = ?
            ORDER BY job_id DESC",
    )?;
    let rows = stmt.query([schema.as_str()])?.mapped(|r| {
        let status: Option<u64> = r.get(1)?;
        Ok(MigrationJobSummary {
            job_id: r.get(0)?,
            status: status.map(MigrationJobStatus::from_int),
        })
    });
    let mut migrations = Vec::new();
    for row in rows {
        migrations.push(row?);
    }
    Ok(MigrationSummary {
        schema_version,
        migrations,
    })
}

#[cfg(test)]
mod test {
    use insta::assert_debug_snapshot;
    use tempfile::tempdir;

    use crate::namespace::meta_store::{metastore_connection_maker, MetaStore};

    use super::*;

    async fn register_schema(meta_store: &MetaStore, schema: &'static str) {
        meta_store
            .handle(schema.into())
            .store(DatabaseConfig {
                is_shared_schema: true,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    async fn register_shared(
        meta_store: &MetaStore,
        name: &'static str,
        schema: &'static str,
    ) -> crate::Result<()> {
        meta_store
            .handle(name.into())
            .store(DatabaseConfig {
                shared_schema_name: Some(schema.into()),
                ..Default::default()
            })
            .await
    }

    #[tokio::test]
    async fn enqueue_migration_job() {
        let tmp = tempdir().unwrap();
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let conn = maker().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path(), conn, manager)
            .await
            .unwrap();
        let mut conn = maker().unwrap();
        setup_schema(&mut conn).unwrap();

        // create 2 shared schema tables
        register_schema(&meta_store, "schema1").await;
        register_schema(&meta_store, "schema2").await;

        // create namespaces
        register_shared(&meta_store, "ns1", "schema1")
            .await
            .unwrap();
        register_shared(&meta_store, "ns2", "schema2")
            .await
            .unwrap();
        register_shared(&meta_store, "ns3", "schema1")
            .await
            .unwrap();

        register_schema_migration_job(
            &mut conn,
            &"schema1".into(),
            &Program::seq(&["select * from test"]),
        )
        .unwrap();

        let mut stmt = conn.prepare("select * from jobs").unwrap();
        assert_debug_snapshot!(stmt.query(()).unwrap().next().unwrap().unwrap());
        stmt.finalize().unwrap();

        let mut stmt = conn.prepare("select * from pending_tasks").unwrap();
        let mut rows = stmt.query(()).unwrap();
        assert_debug_snapshot!(rows.next().unwrap().unwrap());
        assert_debug_snapshot!(rows.next().unwrap().unwrap());
        assert!(rows.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn schema_doesnt_exist() {
        let tmp = tempdir().unwrap();
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let conn = maker().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path(), conn, manager)
            .await
            .unwrap();

        // FIXME: the actual error reported here is a shitty constraint error, we should make the
        // necessary checks beforehand, and return a nice error message.
        assert!(meta_store
            .handle("ns1".into())
            .store(DatabaseConfig {
                shared_schema_name: Some("schema1".into()),
                ..Default::default()
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn pending_job() {
        let tmp = tempdir().unwrap();
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let conn = maker().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path(), conn, manager)
            .await
            .unwrap();
        let mut conn = maker().unwrap();
        setup_schema(&mut conn).unwrap();

        register_schema(&meta_store, "schema1").await;
        register_shared(&meta_store, "ns1", "schema1")
            .await
            .unwrap();
        register_shared(&meta_store, "ns2", "schema1")
            .await
            .unwrap();
        register_shared(&meta_store, "ns3", "schema1")
            .await
            .unwrap();

        let job_id = register_schema_migration_job(
            &mut conn,
            &"schema1".into(),
            &Program::seq(&["create table test (x)"]).into(),
        )
        .unwrap();

        assert_debug_snapshot!(get_next_pending_migration_job(&mut conn).unwrap().unwrap());

        let mut tasks = get_next_pending_migration_tasks_batch(
            &mut conn,
            job_id,
            MigrationTaskStatus::Enqueued,
            10,
        )
        .unwrap();
        assert_debug_snapshot!(tasks);

        let mut task = tasks.pop().unwrap();
        *task.status_mut() = MigrationTaskStatus::Success;
        update_meta_task_status(&mut conn, &task, None).unwrap();

        assert_debug_snapshot!(get_next_pending_migration_job(&mut conn).unwrap().unwrap());
    }

    #[tokio::test]
    async fn step_job_dry_run_success() {
        let tmp = tempdir().unwrap();
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let conn = maker().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path(), conn, manager)
            .await
            .unwrap();
        let mut conn = maker().unwrap();
        setup_schema(&mut conn).unwrap();

        register_schema(&meta_store, "schema1").await;
        register_shared(&meta_store, "ns1", "schema1")
            .await
            .unwrap();
        register_shared(&meta_store, "ns2", "schema1")
            .await
            .unwrap();
        register_shared(&meta_store, "ns3", "schema1")
            .await
            .unwrap();

        register_schema_migration_job(
            &mut conn,
            &"schema1".into(),
            &Program::seq(&["create table test (x)"]).into(),
        )
        .unwrap();

        let job = get_next_pending_migration_job(&mut conn).unwrap().unwrap();
        assert!(matches!(
            job_step_dry_run_success(&mut conn, job.job_id()).unwrap_err(),
            Error::CantStepJobDryRunSuccess
        ));

        let tasks = get_next_pending_migration_tasks_batch(
            &mut conn,
            job.job_id(),
            MigrationTaskStatus::Enqueued,
            10,
        )
        .unwrap();
        for mut task in tasks {
            task.status = MigrationTaskStatus::DryRunSuccess;
            update_meta_task_status(&mut conn, &task, None).unwrap();
        }

        job_step_dry_run_success(&mut conn, job.job_id()).unwrap();
    }

    #[tokio::test]
    async fn cannot_enqueue_another_job_for_namespace_while_other_job_still_pending() {
        let tmp = tempdir().unwrap();
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let conn = maker().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path(), conn, manager)
            .await
            .unwrap();
        let mut conn = maker().unwrap();
        setup_schema(&mut conn).unwrap();

        register_schema(&meta_store, "schema1").await;
        register_schema(&meta_store, "schema2").await;

        let job_id = register_schema_migration_job(
            &mut conn,
            &"schema1".into(),
            &Program::seq(&["create table test (x)"]).into(),
        )
        .unwrap();

        // cannot create a job for a task that has a pending job
        assert!(matches!(
            register_schema_migration_job(
                &mut conn,
                &"schema1".into(),
                &Program::seq(&["create table test (x)"]).into(),
            )
            .unwrap_err(),
            Error::MigrationJobAlreadyInProgress(_)
        ));

        // ok for another schema without pending job
        register_schema_migration_job(
            &mut conn,
            &"schema2".into(),
            &Program::seq(&["create table test (x)"]).into(),
        )
        .unwrap();

        update_job_status(&mut conn, job_id, MigrationJobStatus::RunSuccess, None).unwrap();

        // job is finished, we can enqueue now
        register_schema_migration_job(
            &mut conn,
            &"schema1".into(),
            &Program::seq(&["create table test (x)"]).into(),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn has_pending_migration() {
        let tmp = tempdir().unwrap();
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let conn = maker().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path(), conn, manager)
            .await
            .unwrap();
        let mut conn = maker().unwrap();
        setup_schema(&mut conn).unwrap();

        register_schema(&meta_store, "schema").await;

        let job_id = register_schema_migration_job(
            &mut conn,
            &"schema".into(),
            &Program::seq(&["create table test (c)"]),
        )
        .unwrap();

        assert!(super::has_pending_migration_jobs(&conn, &"schema".into()).unwrap());

        update_job_status(&mut conn, job_id, MigrationJobStatus::RunSuccess, None).unwrap();
        assert!(!super::has_pending_migration_jobs(&conn, &"schema".into()).unwrap());
    }

    #[tokio::test]
    async fn attempt_to_create_db_during_migration() {
        let tmp = tempdir().unwrap();
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let conn = maker().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path(), conn, manager)
            .await
            .unwrap();
        let mut conn = maker().unwrap();
        setup_schema(&mut conn).unwrap();

        register_schema(&meta_store, "schema").await;

        let job_id = register_schema_migration_job(
            &mut conn,
            &"schema".into(),
            &Program::seq(&["create table test (c)"]),
        )
        .unwrap();

        assert_debug_snapshot!(register_shared(&meta_store, "ns", "schema")
            .await
            .unwrap_err());

        update_job_status(&mut conn, job_id, MigrationJobStatus::RunSuccess, None).unwrap();

        assert!(register_shared(&meta_store, "ns", "schema").await.is_ok());
    }
}

pub(crate) fn get_job_status(
    conn: &rusqlite::Connection,
    job_id: i64,
) -> Result<(MigrationJobStatus, Option<String>), Error> {
    Ok(conn.query_row(
        "SELECT status, error FROM jobs WHERE job_id = ? LIMIT 1",
        [job_id],
        |row| {
            let status = MigrationJobStatus::from_int(row.get(0)?);
            let error: Option<String> = row.get(1)?;
            Ok((status, error))
        },
    )?)
}
