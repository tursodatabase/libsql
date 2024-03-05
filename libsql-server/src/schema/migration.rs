use rusqlite::Savepoint;

use crate::connection::program::{Program, Vm};
use crate::query_result_builder::{IgnoreResult, QueryResultBuilder};

use super::status::MigrationTask;
use super::{Error, MigrationTaskStatus};

pub fn setup_migration_table(conn: &mut rusqlite::Connection) -> Result<(), Error> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS __libsql_migration_tasks (
            job_id INTEGER PRIMARY KEY,
            status INTEGER,
            migration TEXT NOT NULL,
            error TEXT
        )",
        (),
    )?;

    Ok(())
}

pub fn has_pending_migration_task(conn: &rusqlite::Connection) -> Result<bool, Error> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM __libsql_migration_tasks WHERE status != ? AND status != ?",
        (
            MigrationTaskStatus::Success as u64,
            MigrationTaskStatus::Failure as u64,
        ),
        |row| {
            let count: i64 = row.get(0)?;
            Ok(count > 0)
        },
    )?)
}

pub fn enqueue_migration_task(
    conn: &rusqlite::Connection,
    task: &MigrationTask,
    migration: &Program,
) -> Result<(), Error> {
    let migration = serde_json::to_string(migration).unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO __libsql_migration_tasks (job_id, status, migration) VALUES (?, ?, ?)",
        (task.job_id(), *task.status() as u64, &migration),
    )?;

    Ok(())
}

/// set the task status to `Run` if its current state is `DryRunSuccess`
pub fn step_migration_task_run(conn: &rusqlite::Connection, job_id: i64) -> Result<(), Error> {
    conn.execute(
        "
            UPDATE __libsql_migration_tasks
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
        "SELECT status, migration, error FROM __libsql_migration_tasks WHERE job_id = ?",
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
            // TODO: use proper builder
            let (ret, new_status) = perform_migration(
                txn,
                migration.as_ref().unwrap(),
                current_state.is_enqueued(),
                IgnoreResult,
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
) -> (Result<B, Error>, MigrationTaskStatus) {
    // todo error handling is sketchy, improve
    let mut savepoint = txn.savepoint().unwrap();
    match try_perform_migration(&mut savepoint, migration, builder) {
        Ok(b) => {
            let status = if dry_run {
                savepoint.rollback().unwrap();
                drop(savepoint);
                MigrationTaskStatus::DryRunSuccess
            } else {
                savepoint.commit().unwrap();
                MigrationTaskStatus::Success
            };
            (Ok(b), status)
        }
        Err(e) => {
            let status = if dry_run {
                savepoint.rollback().unwrap();
                drop(savepoint);
                MigrationTaskStatus::DryRunFailure
            } else {
                savepoint.commit().unwrap();
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
        "UPDATE __libsql_migration_tasks SET status = ?, error = ? WHERE job_id = ?",
        (status as u64, error, job_id),
    )?;

    Ok(())
}

fn try_perform_migration<B: QueryResultBuilder>(
    savepoint: &mut Savepoint,
    migration: &Program,
    builder: B,
) -> Result<B, Error> {
    let mut vm = Vm::new(
        builder, // todo use proper builder
        migration,
        |_| (false, None),
        |_, _, _| (),
    );

    while !vm.finished() {
        vm.step(savepoint).unwrap(); // return migration error
    }

    Ok(vm.into_builder())
}

#[cfg(test)]
mod test {
    use insta::assert_debug_snapshot;
    use libsql_sys::wal::Sqlite3WalManager;
    use tempfile::tempdir;

    use crate::connection::libsql::open_conn_active_checkpoint;
    use crate::namespace::NamespaceName;

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
        enqueue_migration_task(&conn, &task, &Program::seq(&["create table test (x)"])).unwrap();
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
        enqueue_migration_task(&conn, &task, &Program::seq(&["create table test (x)"])).unwrap();

        let task = MigrationTask {
            namespace: NamespaceName::default(),
            status: MigrationTaskStatus::Enqueued,
            job_id: 1,
            task_id: 1,
        };
        enqueue_migration_task(&conn, &task, &Program::seq(&["create table test (x)"])).unwrap();
        let (status, _, _) = get_task_infos(&conn, 1).unwrap();
        assert_eq!(status, MigrationTaskStatus::Success);
    }
}
