//! This module contains code related to schema migration.
//! When an query that performs writes is sent to a schema database (`shared_schema: true`), then
//! this query is executed exactly one on every database that uses that schema database as a source
//! of truth (`shared_schema_name: '<my_schema_db>`), by way of this schema migration process:
//! - The migration is first performed against the schema db in a dry run (we create a transaction,
//! execute the migration and rollback and roll it back).
//! - If the schema db migration was successfull then we enqueue a `MigrationJob` in the job queue.
//! Also, for every database referring to the schema, we enqueue a `MigrationTask` that refers to
//! the job.
//! - The `Scheduler` is in charge of stepping jobs and tasks to completion.
//! - In the initial state of the migration, all the tasks are in `MigrationTaskStatus::Enqueue` state,
//! and the corresponding job is in `MigrationJobStatus::WaitingForDryRun`.
//! - The scheduler gets a batch of task in the `Enqueued` state from the database, and schedules
//! them on for a dry-run on the its worker pool.
//! - When a task is in `Enqueued` state, it is first enqueued to the database's own queue (a table
//! named `sqlite3_libsql_tasks`), and it is set to `Enqueued`. The scheduler then performs a
//! dry run on the migration on that database, setting the outcome of the dry-run to the databases
//! queue in the same transaction. If the dry-run was successfull, the status is set to
//! `DryRunSuccessfull`, otherwise, it is set to `DryRunFailure`, and the error message is saved.
//! By atomically changing the status of the task as we perform the migration, we ensure that in
//! case of failure, we'll always either safely re-perform the task, or collect the result of a
//! prior execution.
//! - The scheduler attempts to drive all tasks to the state `DryRunSuccessfull` before stepping
//! the job's status. If one tasks fails the dry-run, then the migration is aborted.
//! - If all the tasks sucessfully performed the dry-run, then the job is stepped to
//! `DryRunSuccessfull`, at which point, potential waiter are notified.
//! - The scheduler then steps the job to the `WaitingRun` status, and the task status is updated
//! to `Run`. Each task now perform the migration for real, and report their status, in the same
//! manner as the dry-run, except for the fact that the migration is actually committed.
//! - If all tasks are successfull, then the scheduler performs the migration on the schema, and
//! update the job's state to it's final state, `RunSuccess`.
pub(crate) mod db;
mod error;
mod handle;
mod message;
mod migration;
mod result_builder;
mod scheduler;
mod status;

pub use db::{get_migration_details, get_migrations_summary};
pub use error::Error;
pub use handle::SchedulerHandle;
pub use message::SchedulerMessage;
pub use migration::*;
pub use scheduler::Scheduler;
pub use status::{MigrationDetails, MigrationJobStatus, MigrationSummary, MigrationTaskStatus};

use crate::connection::program::Program;
use crate::query_analysis::StmtKind;

// validate program is valid for migration, and return whether foreign keys should be disabled
pub fn validate_migration(migration: &mut Program) -> Result<bool, Error> {
    let mut steps = migration.steps_mut().unwrap().iter_mut().peekable();
    let mut explicit_tx = false;
    let mut disable_foreign_key = false;
    // skip pragmas prologue
    while steps.next_if(|s| s.query.stmt.is_pragma()).is_some() {
        disable_foreign_key = true;
    }

    // first step can be a BEGIN
    if let Some(step) = steps.next() {
        if matches!(step.query.stmt.kind, StmtKind::TxnBegin) {
            // neutralize step
            step.query.stmt.stmt = r#"SELECT 'neutralized txn begin'"#.into();
            explicit_tx = true;
        }
    }

    // skip all steps that are not tx items
    while steps.next_if(|s| !s.query.stmt.kind.is_txn()).is_some() {}

    // last stmt can be a tx commit
    while let Some(step) = steps.next_if(|s| s.query.stmt.kind.is_txn()) {
        if matches!(step.query.stmt.kind, StmtKind::TxnEnd) {
            if !explicit_tx {
                // transaction is closed but was never opened
                return Err(Error::MigrationContainsTransactionStatements);
            }
            // neutralize step
            step.query.stmt.stmt = r#"SELECT 'neutralized txn component'"#.into();
        }
    }

    // validate pragma epilogue
    if steps.by_ref().any(|s| !s.query.stmt.is_pragma()) {
        // only accept pragmas after tx end
        return Err(Error::MigrationContainsTransactionStatements);
    }

    Ok(disable_foreign_key)
}
