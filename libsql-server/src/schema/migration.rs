use rusqlite::Savepoint;

use crate::connection::program::{Program, Vm};
use crate::namespace::meta_store::MigrationTask;
use crate::query_result_builder::QueryResultBuilder;

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

pub fn enqueue_migration_task(
    conn: &rusqlite::Connection,
    task: &MigrationTask,
    migration: &Program,
) -> Result<(), Error> {
    let migration = serde_json::to_string(migration).unwrap();
    conn.execute(
        "INSERT INTO __libsql_migration_tasks (job_id, status, migration) VALUES (?, ?, ?)",
        (task.job_id(), *task.status() as u64, &migration),
    )?;
    Ok(())
}

pub fn step_migration_task_run(
    conn: &rusqlite::Connection,
    task: &MigrationTask,
) -> Result<(), Error> {
    conn.execute(
        "
        UPDATE __libsql_migration_tasks
        SET status = ?
        WHERE job_id = ? AND status = ?
        ",
        (
            task.job_id(),
            MigrationTaskStatus::Run as u64,
            MigrationTaskStatus::DryRunSuccess as u64,
        ),
    )?;

    Ok(())
}

pub fn perform_migration<B: QueryResultBuilder>(
    conn: &mut rusqlite::Transaction,
    migration: &Program,
    dry_run: bool,
    builder: B,
) -> (Result<B, Error>, MigrationTaskStatus) {
    // todo error handling is sketchy, improve
    let mut savepoint = conn.savepoint().unwrap();
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

pub(super) fn update_task_status(
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
