use std::sync::Arc;

use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use crate::connection::program::Program;
use crate::connection::MakeConnection;
use crate::namespace::meta_store::{MigrationJob, MigrationTask};
use crate::namespace::{NamespaceName, NamespaceStore};
use crate::query_result_builder::IgnoreResult;
use crate::schema::{step_migration_task_run, MigrationJobStatus};

use super::error::Error;
use super::migration::enqueue_migration_task;
use super::{perform_migration, MigrationTaskStatus, SchedulerMessage};

enum WorkResult {
    Task {
        old_status: MigrationTaskStatus,
        task: MigrationTask,
        error: Option<String>,
    },
    Schema {
        job_id: i64,
    },
}

pub struct Scheduler {
    namespace_store: NamespaceStore,
    workers: JoinSet<WorkResult>,
    current_batch: Vec<MigrationTask>,
    // job id of the curently processing job
    current_job: Option<MigrationJob>,
    has_work: bool,
}

impl Scheduler {
    pub async fn run(mut self, mut receiver: mpsc::Receiver<SchedulerMessage>) {
        let tasks_permits = Arc::new(Semaphore::new(10));
        loop {
            tokio::select! {
                Some(msg) = receiver.recv() => {
                    self.handle_msg(msg).await;
                }
                // There is work to do, and a worker slot to perform it
                Ok(permit) = tasks_permits.clone().acquire_owned(), if self.has_work => {
                    self.enqueue_work(permit).await;
                }
                Some(res) = self.workers.join_next(), if !self.workers.is_empty() => {
                    self.has_work = true;
                    match res {
                        Ok(WorkResult::Schema { job_id }) => {
                            let job = self.current_job.take().unwrap();
                            assert_eq!(job.job_id(), job_id);
                            self.namespace_store.meta_store().update_job_status(job_id, MigrationJobStatus::RunSuccess).await.unwrap();
                        }
                        Ok(WorkResult::Task { old_status, task, error }) => {
                            let new_status = *task.status();
                            self.namespace_store
                                .meta_store()
                                .update_task_status(task, error).await.unwrap();
                            let current_job = self.current_job
                                .as_mut()
                                .expect("processing task result, but job is missing");

                            *current_job.progress_mut(old_status) -= 1;
                            *current_job.progress_mut(new_status) += 1;
                        }
                        Err(_e) => {
                            todo!("migration task panicked");
                        }
                    }
                }
                else => break,
            }
        }

        tracing::info!("all scheduler handles dropped: exiting.");
    }

    async fn handle_msg(&mut self, msg: SchedulerMessage) {
        match msg {
            SchedulerMessage::ScheduleMigration {
                schema,
                migration,
                ret,
            } => {
                let res = self.register_migration_task(schema, migration).await;
                let _ = ret.send(res);
                self.has_work = true;
            }
        }
    }

    // TODO: refactor this function it's turning into a mess. Not so simple, because of borrow
    // constraints
    async fn enqueue_work(&mut self, permit: OwnedSemaphorePermit) {
        let job = match self.current_job {
            Some(ref mut job) => job,
            None => {
                match self
                    .namespace_store
                    .meta_store()
                    .get_next_pending_job()
                    .await
                    .unwrap()
                {
                    Some(job) => self.current_job.insert(job),
                    None => {
                        self.has_work = false;
                        return;
                    }
                }
            }
        };

        // try to step the current job
        match *job.status() {
            MigrationJobStatus::WaitingDryRun => {
                // there was a dry run failure, abort the task
                if job.progress(MigrationTaskStatus::DryRunFailure) != 0 {
                    todo!("abort job");
                }
                if job.progress_all(MigrationTaskStatus::DryRunSuccess) {
                    // ready to move to the run phase
                    // report dry run succes
                    // todo: it may be worthwhile to check that all the db state reflects our
                    // vision of the tasks progress here
                    // also if we wanted to abort before the run, now would be the right place

                    *job = self
                        .namespace_store
                        .meta_store()
                        .job_step_dry_run_success(job.clone())
                        .await
                        .unwrap();
                    if matches!(job.status(), MigrationJobStatus::DryRunSuccess) {
                        // todo!("notify dry run success")
                    }
                }
            }
            MigrationJobStatus::DryRunSuccess => {
                self.namespace_store
                    .meta_store()
                    .update_job_status(job.job_id(), MigrationJobStatus::WaitingRun)
                    .await
                    .unwrap();
                *job.status_mut() = MigrationJobStatus::WaitingRun;
            }
            MigrationJobStatus::DryRunFailure => todo!(),
            MigrationJobStatus::WaitingRun => {
                // there was a dry run failure, abort the task
                if job.progress(MigrationTaskStatus::Failure) != 0 {
                    todo!("that shouldn't happen!");
                }
                if job.progress_all(MigrationTaskStatus::Success) {
                    // todo: perform more robust check, like in waiting dry run

                    let connection_maker = self
                        .namespace_store
                        .with(job.schema(), |ns| {
                            ns.db
                                .as_schema()
                                .expect("expected database to be a schema database")
                                .connection_maker()
                                .clone()
                        })
                        .await
                        .unwrap();

                    let connection = connection_maker.create().await.unwrap();
                    // todo: make sure that the next job is not a job for the same namesapce:
                    // prevent job to be enqueue for a schema if there is still onging work for
                    // that schema
                    let job_id = job.job_id();
                    let migration = job.migration();
                    self.workers.spawn_blocking(move || {
                        let _perm = permit;
                        connection.connection().with_raw(|conn| {
                            let mut txn = conn.transaction().unwrap();
                            let schema_version = txn
                                .query_row("PRAGMA schema_version", (), |row| row.get::<_, i64>(0))
                                .unwrap();

                            if schema_version != job_id {
                                // todo: use proper builder and collect errors
                                let (ret, _status) =
                                    perform_migration(&mut txn, &migration, false, IgnoreResult);
                                let _error = ret.err().map(|e| e.to_string());
                                txn.pragma_update(None, "schema_version", job_id).unwrap();
                                // update schema version to job_id?
                                txn.commit().unwrap();
                            }

                            WorkResult::Schema { job_id }
                        })
                    });
                    // do not enqueue anything until the schema migration is complete
                    self.has_work = false;
                    return;
                }
            }
            MigrationJobStatus::RunSuccess => unreachable!(),
            MigrationJobStatus::RunFailure => todo!("handle run failure"),
        }

        // fill the current batch if necessary
        if self.current_batch.is_empty() {
            match job.status() {
                MigrationJobStatus::WaitingDryRun => {
                    // get a batch of enqueued tasks
                    self.current_batch = self
                        .namespace_store
                        .meta_store()
                        .get_next_pending_migration_tasks_batch(
                            job.job_id(),
                            MigrationTaskStatus::Enqueued,
                            50, // TODO: make that configurable maybe?
                        )
                        .await
                        .unwrap();
                }
                MigrationJobStatus::WaitingRun => {
                    // get a batch of enqueued tasks
                    self.current_batch = self
                        .namespace_store
                        .meta_store()
                        .get_next_pending_migration_tasks_batch(
                            job.job_id(),
                            MigrationTaskStatus::DryRunSuccess,
                            50, // TODO: make that configurable maybe?
                        )
                        .await
                        .unwrap();
                }
                _ => (),
            };
        }

        // enqueue some work
        if let Some(mut task) = self.current_batch.pop() {
            let connection_maker = self
                .namespace_store
                .with(task.namespace(), |ns| {
                    ns.db
                        .as_primary()
                        .expect("attempting to perform schema migration on non-primary database")
                        .connection_maker()
                        .clone()
                })
                .await
                .unwrap();

            let connection = connection_maker.create().await.unwrap();

            let migration = job.migration();
            let job_status = *job.status();
            self.workers.spawn_blocking(move || {
                let old_status = *task.status();
                // move the permit inside of the closure, so that it gets dropped when the work is done.
                let _permit = permit;
                connection.with_raw(move |conn| {
                    let mut txn = conn.transaction().unwrap();

                    let is_dry_run = match task.status() {
                        MigrationTaskStatus::Enqueued => {
                            enqueue_migration_task(&txn, &task, &migration).unwrap();
                            true
                        }
                        MigrationTaskStatus::DryRunSuccess if job_status.is_waiting_run() => {
                            step_migration_task_run(&txn, &task).unwrap();
                            false
                        }
                        _ => unreachable!("expected task status to be `enqueued` or `run`"),
                    };
                    let (ret, status) =
                        perform_migration(&mut txn, &migration, is_dry_run, IgnoreResult);
                    let error = ret.err().map(|e| e.to_string());
                    super::migration::update_task_status(
                        &txn,
                        task.job_id(),
                        status,
                        error.as_deref(),
                    )
                    .unwrap();
                    *task.status_mut() = status;
                    txn.commit().unwrap();
                    WorkResult::Task {
                        old_status,
                        task,
                        error,
                    }
                })
            });
        } else {
            // there is still job, but the queue is empty, it means that we are waiting for the
            // remaining jobs to report status. just wait.
            self.has_work = false;
        }
    }

    pub async fn register_migration_task(
        &self,
        schema: NamespaceName,
        migration: Arc<Program>,
    ) -> Result<i64, Error> {
        let job_id = self
            .namespace_store
            .meta_store()
            .register_schema_migration(schema, migration)
            .await
            .map_err(|e| Error::Registration(Box::new(e)))?;
        Ok(job_id)
    }

    pub(crate) fn new(namespace_store: NamespaceStore) -> Self {
        Self {
            namespace_store,
            workers: Default::default(),
            current_batch: Vec::new(),
            current_job: None,
            // initialized to true to kickoff the queue
            has_work: true,
        }
    }
}
