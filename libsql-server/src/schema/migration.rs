use std::sync::Arc;

use itertools::Itertools;
use once_cell::sync::Lazy;
use rusqlite::Savepoint;

use crate::connection::program::{Program, Vm};
use crate::namespace::NamespaceName;
use crate::query_result_builder::{IgnoreResult, QueryBuilderConfig, QueryResultBuilder};

use super::result_builder::SchemaMigrationResultBuilder;
use super::{Error, MigrationTaskStatus};

pub fn setup_migration_table(conn: &mut rusqlite::Connection) -> Result<(), Error> {
    static TASKS_TABLE_QUERY: Lazy<String> = Lazy::new(|| {
        format!(
            "CREATE TABLE IF NOT EXISTS sqlite3_libsql_tasks (
                job_id INTEGER PRIMARY KEY,
                status INTEGER,
                migration TEXT NOT NULL,
                error TEXT,
                finished BOOLEAN GENERATED ALWAYS AS ({})
            )",
            MigrationTaskStatus::finished_states()
                .iter()
                .map(|s| format!("status = {}", *s as u64))
                .join(" OR ")
        )
    });

    let tx = conn.transaction()?;
    let schema_version =
        tx.pragma_query_value(None, "schema_version", |row| Ok(row.get::<_, u64>(0)?))?;
    tx.execute(&*TASKS_TABLE_QUERY, ())?;
    // We have to make sure schema_version is not changed in case this db is a fork of shared schema
    // or a namespace that links to shared schema.
    // It is ok to do this because we're the only connection to that database at this point.
    tx.pragma_update(None, "schema_version", schema_version)?;
    tx.commit()?;
    Ok(())
}

pub fn has_pending_migration_task(conn: &rusqlite::Connection) -> Result<bool, Error> {
    Ok(conn.query_row(
        "SELECT COUNT(1) FROM sqlite3_libsql_tasks WHERE finished = false",
        (),
        |row| Ok(row.get::<_, usize>(0)? != 0),
    )?)
}

pub fn enqueue_migration_task(
    conn: &rusqlite::Connection,
    job_id: i64,
    status: MigrationTaskStatus,
    migration: &Program,
) -> Result<(), Error> {
    let migration = serde_json::to_string(migration).unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO sqlite3_libsql_tasks (job_id, status, migration) VALUES (?, ?, ?)",
        (job_id, status as u64, &migration),
    )?;

    Ok(())
}

pub fn abort_migration_task(conn: &rusqlite::Connection, job_id: i64) -> Result<(), Error> {
    // there is a `NOT NULL` constraint on migration, but if we are aborting a task that wasn't
    // already enqueued, we need a placeholder. It's ok because we are never gonna try to run a
    // failed task migration.
    conn.execute("INSERT OR REPLACE INTO sqlite3_libsql_tasks (job_id, status, error, migration) VALUES (?, ?, ?, ?)",
    (job_id, MigrationTaskStatus::Failure as u64, "aborted", "aborted"))?;

    Ok(())
}

/// set the task status to `Run` if its current state is `DryRunSuccess`
pub fn step_migration_task_run(conn: &rusqlite::Connection, job_id: i64) -> Result<(), Error> {
    conn.execute(
        "
            UPDATE sqlite3_libsql_tasks
            SET status = ?
            WHERE job_id = ? AND status = ?
            ",
        (
            MigrationTaskStatus::Run as u64,
            job_id,
            MigrationTaskStatus::DryRunSuccess as u64,
        ),
    )?;

    Ok(())
}

fn get_task_infos(
    conn: &rusqlite::Connection,
    job_id: i64,
) -> Result<(MigrationTaskStatus, Option<Program>, Option<String>), Error> {
    Ok(conn.query_row(
        "SELECT status, migration, error FROM sqlite3_libsql_tasks WHERE job_id = ?",
        [job_id],
        |row| {
            let status = MigrationTaskStatus::from_int(row.get::<_, u64>(0)?);
            let (migration, error) = match status {
                MigrationTaskStatus::Enqueued | MigrationTaskStatus::Run => {
                    let migration: Program =
                        serde_json::from_str(row.get_ref(1)?.as_str()?).unwrap();
                    (Some(migration), None)
                }
                MigrationTaskStatus::DryRunSuccess | MigrationTaskStatus::Success => (None, None),
                MigrationTaskStatus::DryRunFailure | MigrationTaskStatus::Failure => {
                    let error = row.get::<_, Option<String>>(2)?;
                    (None, error)
                }
            };

            Ok((status, migration, error))
        },
    )?)
}

pub(super) fn step_task(
    txn: &mut rusqlite::Transaction,
    job_id: i64,
) -> Result<(MigrationTaskStatus, Option<String>), Error> {
    let (current_state, migration, error) = get_task_infos(txn, job_id)?;

    match current_state {
        MigrationTaskStatus::DryRunSuccess | MigrationTaskStatus::DryRunFailure => {
            Ok((current_state, error))
        }
        MigrationTaskStatus::Run | MigrationTaskStatus::Enqueued => {
            let (ret, new_status) = perform_migration(
                txn,
                migration.as_ref().unwrap(),
                current_state.is_enqueued(),
                IgnoreResult,
                &QueryBuilderConfig::default(),
            );
            let error = ret.err().map(|e| e.to_string());
            update_db_task_status(txn, job_id, new_status, error.as_deref())?;

            Ok((new_status, error))
        }
        // final state, nothing to do but report
        MigrationTaskStatus::Success | MigrationTaskStatus::Failure => Ok((current_state, error)),
    }
}

pub fn perform_migration<B: QueryResultBuilder>(
    txn: &mut rusqlite::Transaction,
    migration: &Program,
    dry_run: bool,
    builder: B,
    config: &QueryBuilderConfig,
) -> (Result<B, Error>, MigrationTaskStatus) {
    // todo error handling is sketchy, improve
    let builder = SchemaMigrationResultBuilder::new(builder);
    let mut savepoint = txn.savepoint().unwrap();
    match try_perform_migration(&mut savepoint, migration, builder, config) {
        Ok(builder) if builder.is_success() => {
            let status = if dry_run {
                savepoint.rollback().unwrap();
                drop(savepoint);
                MigrationTaskStatus::DryRunSuccess
            } else {
                savepoint.commit().unwrap();
                MigrationTaskStatus::Success
            };
            (Ok(builder.into_inner()), status)
        }
        Ok(builder) => {
            assert!(!builder.is_success());
            savepoint.rollback().unwrap();
            drop(savepoint);
            let status = if dry_run {
                MigrationTaskStatus::DryRunFailure
            } else {
                MigrationTaskStatus::Failure
            };
            let (step, error) = builder.into_error();
            (Err(Error::MigrationError(step, error)), status)
        }
        Err(e) => {
            savepoint.rollback().unwrap();
            drop(savepoint);
            let status = if dry_run {
                MigrationTaskStatus::DryRunFailure
            } else {
                MigrationTaskStatus::Failure
            };
            (Err(e), status)
        }
    }
}

pub(super) fn update_db_task_status(
    conn: &rusqlite::Connection,
    job_id: i64,
    status: MigrationTaskStatus,
    error: Option<&str>,
) -> Result<(), Error> {
    conn.execute(
        "UPDATE sqlite3_libsql_tasks SET status = ?, error = ? WHERE job_id = ?",
        (status as u64, error, job_id),
    )?;

    Ok(())
}

fn try_perform_migration<B: QueryResultBuilder>(
    savepoint: &mut Savepoint,
    migration: &Program,
    mut builder: B,
    config: &QueryBuilderConfig,
) -> Result<B, Error> {
    builder.init(config).unwrap();
    let mut vm = Vm::new(
        builder, // todo use proper builder
        migration,
        |_| (false, None),
        |_, _, _, _, _| (),
        Arc::new(|_: &NamespaceName| Err(crate::Error::AttachInMigration)),
    );

    while !vm.finished() {
        vm.step(savepoint)
            .map_err(|e| Error::MigrationExecuteError(e.into()))?; // return migration error
    }

    vm.builder()
        .finish(None, true)
        .map_err(|e| Error::MigrationExecuteError(crate::Error::from(e).into()))?;

    Ok(vm.into_builder())
}

#[cfg(test)]
mod test {
    use insta::assert_debug_snapshot;
    use libsql_sys::wal::Sqlite3WalManager;
    use tempfile::tempdir;

    use crate::connection::libsql::open_conn_active_checkpoint;
    use crate::namespace::NamespaceName;
    use crate::schema::status::MigrationTask;

    use super::*;

    #[test]
    fn already_performed_task_is_not_reexecuted() {
        let tmp = tempdir().unwrap();

        let mut conn =
            open_conn_active_checkpoint(tmp.path(), Sqlite3WalManager::default(), None, 1000, None)
                .unwrap();
        setup_migration_table(&mut conn).unwrap();

        let task = MigrationTask {
            namespace: NamespaceName::default(),
            status: MigrationTaskStatus::Success,
            job_id: 1,
            task_id: 1,
        };
        enqueue_migration_task(
            &conn,
            task.job_id(),
            *task.status(),
            &Program::seq(&["create table test (x)"]),
        )
        .unwrap();
        let mut txn = conn.transaction().unwrap();
        let (status, error) = step_task(&mut txn, 1).unwrap();
        txn.commit().unwrap();

        assert!(error.is_none());
        assert_eq!(status, MigrationTaskStatus::Success);

        // The migration should not have been performed, since we declared that it was already
        // successfully executed.
        let schema = conn
            .prepare("select * from sqlite_schema")
            .unwrap()
            .query_map((), |r| Ok(format!("{r:?}")))
            .unwrap()
            .collect::<Vec<_>>();
        assert_debug_snapshot!(schema);
    }

    #[test]
    fn ignore_task_enqueue_if_already_exists() {
        let tmp = tempdir().unwrap();

        let mut conn =
            open_conn_active_checkpoint(tmp.path(), Sqlite3WalManager::default(), None, 1000, None)
                .unwrap();
        setup_migration_table(&mut conn).unwrap();

        let task = MigrationTask {
            namespace: NamespaceName::default(),
            status: MigrationTaskStatus::Success,
            job_id: 1,
            task_id: 1,
        };
        enqueue_migration_task(
            &conn,
            task.job_id(),
            *task.status(),
            &Program::seq(&["create table test (x)"]),
        )
        .unwrap();

        let task = MigrationTask {
            namespace: NamespaceName::default(),
            status: MigrationTaskStatus::Enqueued,
            job_id: 1,
            task_id: 1,
        };
        enqueue_migration_task(
            &conn,
            task.job_id(),
            *task.status(),
            &Program::seq(&["create table test (x)"]),
        )
        .unwrap();
        let (status, _, _) = get_task_infos(&conn, 1).unwrap();
        assert_eq!(status, MigrationTaskStatus::Success);
    }
}
