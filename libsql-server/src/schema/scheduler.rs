use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use crate::connection::program::Program;
use crate::connection::MakeConnection;
use crate::namespace::meta_store::MetaStoreConnection;
use crate::namespace::{NamespaceName, NamespaceStore};
use crate::query_result_builder::{IgnoreResult, QueryBuilderConfig};
use crate::schema::db::{update_job_status, update_meta_task_status};
use crate::schema::{step_migration_task_run, MigrationJobStatus};

use super::db::{
    get_next_pending_migration_job, get_next_pending_migration_tasks_batch,
    job_step_dry_run_success, register_schema_migration_job, setup_schema,
};
use super::error::Error;
use super::migration::enqueue_migration_task;
use super::status::{MigrationJob, MigrationTask};
use super::{perform_migration, step_task, MigrationTaskStatus, SchedulerMessage};

const MAX_CONCCURENT_JOBS: usize = 10;

pub struct Scheduler {
    namespace_store: NamespaceStore,
    /// this is a connection to the meta store db, but it's used for migration operations
    migration_db: Arc<Mutex<MetaStoreConnection>>,
    workers: JoinSet<WorkResult>,
    /// A batch of tasks for the current job (if any)
    current_batch: Vec<MigrationTask>,
    /// Currently processing job
    current_job: Option<MigrationJob>,
    has_work: bool,
    permits: Arc<Semaphore>,
}

impl Scheduler {
    pub(crate) fn new(
        namespace_store: NamespaceStore,
        mut conn: MetaStoreConnection,
    ) -> crate::Result<Self> {
        setup_schema(&mut conn)?;
        Ok(Self {
            namespace_store,
            workers: Default::default(),
            current_batch: Vec::new(),
            current_job: None,
            // initialized to true to kickoff the queue
            has_work: true,
            migration_db: Arc::new(Mutex::new(conn)),
            permits: Arc::new(Semaphore::new(MAX_CONCCURENT_JOBS)),
        })
    }

    pub async fn run(mut self, mut receiver: mpsc::Receiver<SchedulerMessage>) {
        while self.step(&mut receiver).await {}

        tracing::info!("all scheduler handles dropped: exiting.");
    }

    #[inline]
    async fn step(&mut self, receiver: &mut mpsc::Receiver<SchedulerMessage>) -> bool {
        tokio::select! {
            Some(msg) = receiver.recv() => {
                self.handle_msg(msg).await;
            }
            // There is work to do, and a worker slot to perform it
            // TODO: optim: we could try enqueue more work in a go by try_acquiring more
            // permits here
            Ok(permit) = self.permits.clone().acquire_owned(), if self.has_work => {
                self.enqueue_work(permit).await;
            }
            Some(res) = self.workers.join_next(), if !self.workers.is_empty() => {
                match res {
                    // TODO: handle schema update failure
                    Ok(WorkResult::Schema { job_id }) => {
                        let job = self.current_job.take().unwrap();
                        assert_eq!(job.job_id(), job_id);
                        with_conn_async(self.migration_db.clone(), move |conn| {
                            update_job_status(conn, job_id, MigrationJobStatus::RunSuccess)
                        })
                        .await
                            .unwrap();
                        // the current job is finished, try to pop next one out of the queue:
                        self.has_work = true;
                    }
                    Ok(WorkResult::Task { old_status, mut task, error }) => {
                        if let Err(_backup_err) = task.wait_for_backup().await {
                            *task.status_mut() = MigrationTaskStatus::DryRunFailure
                            // todo: any other actions?
                        };
                        let new_status = *task.status();
                        with_conn_async(self.migration_db.clone(), move |conn| {
                            update_meta_task_status(conn, task, error)
                        })
                        .await
                        .unwrap();
                        let current_job = self.current_job
                            .as_mut()
                            .expect("processing task result, but job is missing");

                        *current_job.progress_mut(old_status) -= 1;
                        *current_job.progress_mut(new_status) += 1;

                        // we have more work if:
                        // - the current batch has more tasks to enqueue
                        // - the remaining number of pending tasks is greater than the amount of in-flight tasks: we can enqueue more
                        // - there's no more in-flight nor pending tasks for the job: we need to step the job
                        let in_flight = MAX_CONCCURENT_JOBS - self.permits.available_permits();
                        let pending_tasks = current_job.count_pending_tasks();
                        self.has_work = !self.current_batch.is_empty() || (pending_tasks == 0 && in_flight == 0) || pending_tasks > in_flight;
                    }
                    Err(_e) => {
                        todo!("migration task panicked");
                    }
                }
            }
            else => return false,
        }

        true
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
                // it not necessary to raise the flag if we are currently processing a job: it
                // prevents spurious wakeups, and the job will be picked up anyway.
                self.has_work = self.current_job.is_none();
            }
        }
    }

    // TODO: refactor this function it's turning into a mess. Not so simple, because of borrow
    // constraints
    async fn enqueue_work(&mut self, permit: OwnedSemaphorePermit) {
        let job = match self.current_job {
            Some(ref mut job) => job,
            None => {
                let maybe_next_job = with_conn_async(self.migration_db.clone(), move |conn| {
                    get_next_pending_migration_job(conn)
                })
                .await
                .unwrap();
                match maybe_next_job {
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

                    *job = with_conn_async(self.migration_db.clone(), {
                        let job = job.clone();
                        move |conn| job_step_dry_run_success(conn, job)
                    })
                    .await
                    .unwrap();

                    if matches!(job.status(), MigrationJobStatus::DryRunSuccess) {
                        // todo!("notify dry run success")
                        // nothing more to do in this call, return early and let next call enqueue
                        // step the job
                        return;
                    }
                }
            }
            MigrationJobStatus::DryRunSuccess => {
                let job_id = job.job_id();
                with_conn_async(self.migration_db.clone(), move |conn| {
                    update_job_status(conn, job_id, MigrationJobStatus::WaitingRun)
                })
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
                                let (ret, _status) = perform_migration(
                                    &mut txn,
                                    &migration,
                                    false,
                                    IgnoreResult,
                                    &QueryBuilderConfig::default(),
                                );
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
                    *job.status_mut() = MigrationJobStatus::WaitingSchemaUpdate;
                    return;
                }
            }
            MigrationJobStatus::WaitingSchemaUpdate => {
                // just wait for schema update to return
                // this is a transient state, and it's not persisted. It's only necessary to make
                // the code more robust when there are spurious wakups that would cause to this
                // function being called;
                self.has_work = false;
                return;
            }
            MigrationJobStatus::RunSuccess => unreachable!(),
            MigrationJobStatus::RunFailure => todo!("handle run failure"),
        }

        // fill the current batch if necessary
        if self.current_batch.is_empty()
            && matches!(
                *job.status(),
                MigrationJobStatus::WaitingDryRun | MigrationJobStatus::WaitingRun
            )
        {
            // get a batch of enqueued tasks
            let job_id = job.job_id();
            let status = match job.status() {
                MigrationJobStatus::WaitingDryRun => MigrationTaskStatus::Enqueued,
                MigrationJobStatus::WaitingRun => MigrationTaskStatus::DryRunSuccess,
                _ => unreachable!(),
            };

            self.current_batch = with_conn_async(self.migration_db.clone(), move |conn| {
                get_next_pending_migration_tasks_batch(conn, job_id, status, 50)
            })
            .await
            .unwrap();
        }

        // enqueue some work
        if let Some(mut task) = self.current_batch.pop() {
            let status = task.status().clone();
            let (connection_maker, block_writes, backup) =
                self.namespace_store
                    .with(task.namespace(), move |ns| {
                        let db = ns.db.as_primary().expect(
                            "attempting to perform schema migration on non-primary database",
                        );
                        let backup = if matches!(status, MigrationTaskStatus::Enqueued) {
                            // Task is enqueued for dry run and no other database changes are
                            // allowed until dry run completes. Snapshot a current state of backup
                            // progress and save an awaiter.
                            db.backup_savepoint().unwrap()
                        } else {
                            None
                        };
                        (
                            db.connection_maker().clone(),
                            db.block_writes.clone(),
                            backup,
                        )
                    })
                    .await
                    .unwrap();

            task.backup_sync = backup;
            let connection = connection_maker.create().await.unwrap();
            let migration = job.migration();
            let job_status = *job.status();

            // we block the writes before enqueuing the task, it makes testing predictable
            if *task.status() == MigrationTaskStatus::Enqueued {
                block_writes.store(true, std::sync::atomic::Ordering::SeqCst);
            }

            self.workers.spawn_blocking(move || {
                let old_status = *task.status();
                // move the permit inside of the closure, so that it gets dropped when the work is done.
                let _permit = permit;
                connection.with_raw(move |conn| {
                    let mut txn = conn.transaction().unwrap();

                    match task.status() {
                        MigrationTaskStatus::Enqueued => {
                            enqueue_migration_task(&txn, &task, &migration).unwrap();
                        }
                        MigrationTaskStatus::DryRunSuccess if job_status.is_waiting_run() => {
                            step_migration_task_run(&txn, task.job_id()).unwrap();
                        }
                        _ => unreachable!("expected task status to be `enqueued` or `run`"),
                    }

                    let (new_status, error) = step_task(&mut txn, task.job_id()).unwrap();
                    txn.commit().unwrap();

                    *task.status_mut() = new_status;

                    if task.status().is_finished() {
                        block_writes.store(false, std::sync::atomic::Ordering::SeqCst);
                    }

                    WorkResult::Task {
                        old_status,
                        task,
                        error,
                    }
                })
            });
        } else {
            // there is still a job, but the queue is empty, it means that we are waiting for the
            // remaining jobs to report status. just wait.
            self.has_work = false;
        }
    }

    pub async fn register_migration_task(
        &self,
        schema: NamespaceName,
        migration: Arc<Program>,
    ) -> Result<i64, Error> {
        with_conn_async(self.migration_db.clone(), move |conn| {
            register_schema_migration_job(conn, &schema, &migration)
        })
        .await
    }
}

async fn with_conn_async<T: Send + 'static>(
    conn: Arc<Mutex<MetaStoreConnection>>,
    f: impl FnOnce(&mut rusqlite::Connection) -> Result<T, Error> + Send + 'static,
) -> Result<T, Error> {
    tokio::task::spawn_blocking(move || {
        let mut conn = conn.lock();
        f(&mut *conn)
    })
    .await
    .expect("migration db task panicked")
}

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

#[cfg(test)]
mod test {
    use std::path::Path;

    use tempfile::tempdir;

    use crate::connection::config::DatabaseConfig;
    use crate::database::DatabaseKind;
    use crate::namespace::meta_store::{metastore_connection_maker, MetaStore};
    use crate::namespace::{NamespaceConfig, RestoreOption};
    use crate::schema::SchedulerHandle;

    use super::*;

    // FIXME: lots of coupling here, there whoudl be an easier way to test this.
    #[tokio::test]
    async fn writes_blocked_while_writing() {
        let tmp = tempdir().unwrap();
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let conn = maker().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path(), conn, manager)
            .await
            .unwrap();
        let (sender, mut receiver) = mpsc::channel(100);
        let config = make_config(sender.clone().into(), tmp.path());
        let store = NamespaceStore::new(false, false, 10, config, meta_store)
            .await
            .unwrap();
        let mut scheduler = Scheduler::new(store.clone(), maker().unwrap()).unwrap();

        store
            .create(
                "schema".into(),
                RestoreOption::Latest,
                DatabaseConfig {
                    is_shared_schema: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        store
            .create(
                "ns".into(),
                RestoreOption::Latest,
                DatabaseConfig {
                    shared_schema_name: Some("schema".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let block_write = store
            .with("ns".into(), |ns| {
                ns.db.as_primary().unwrap().block_writes.clone()
            })
            .await
            .unwrap();

        let (snd, mut rcv) = tokio::sync::oneshot::channel();
        sender
            .send(SchedulerMessage::ScheduleMigration {
                schema: "schema".into(),
                migration: Program::seq(&["create table test (c)"]).into(),
                ret: snd,
            })
            .await
            .unwrap();

        // step until we get a response
        loop {
            tokio::select! {
                _ = &mut rcv => break,
                _ = scheduler.step(&mut receiver) => {},
            }
        }

        // this is right before the task gets enqueued
        assert!(!block_write.load(std::sync::atomic::Ordering::Relaxed));
        // next step should enqueue the task
        scheduler.step(&mut receiver).await;
        assert!(block_write.load(std::sync::atomic::Ordering::Relaxed));

        while scheduler.current_job.is_some() {
            scheduler.step(&mut receiver).await;
        }

        assert!(!block_write.load(std::sync::atomic::Ordering::Relaxed));
    }

    fn make_config(migration_scheduler: SchedulerHandle, path: &Path) -> NamespaceConfig {
        NamespaceConfig {
            db_kind: DatabaseKind::Primary,
            base_path: path.to_path_buf().into(),
            max_log_size: 1000000000,
            db_is_dirty: false,
            max_log_duration: None,
            extensions: Arc::new([]),
            stats_sender: tokio::sync::mpsc::channel(1).0,
            max_response_size: 100000000000000,
            max_total_response_size: 100000000000,
            checkpoint_interval: None,
            max_concurrent_connections: Arc::new(Semaphore::new(10)),
            max_concurrent_requests: 10000,
            encryption_config: None,
            channel: None,
            uri: None,
            bottomless_replication: None,
            scripted_backup: None,
            migration_scheduler,
        }
    }

    #[tokio::test]
    async fn ns_loaded_with_pending_tasks_writes_is_blocked() {
        let tmp = tempdir().unwrap();
        {
            let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
            let conn = maker().unwrap();
            let meta_store = MetaStore::new(Default::default(), tmp.path(), conn, manager)
                .await
                .unwrap();
            let (sender, mut receiver) = mpsc::channel(100);
            let config = make_config(sender.clone().into(), tmp.path());
            let store = NamespaceStore::new(false, false, 10, config, meta_store)
                .await
                .unwrap();
            let mut scheduler = Scheduler::new(store.clone(), maker().unwrap()).unwrap();

            store
                .create(
                    "schema".into(),
                    RestoreOption::Latest,
                    DatabaseConfig {
                        is_shared_schema: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            store
                .create(
                    "ns".into(),
                    RestoreOption::Latest,
                    DatabaseConfig {
                        shared_schema_name: Some("schema".into()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

            let block_write = store
                .with("ns".into(), |ns| {
                    ns.db.as_primary().unwrap().block_writes.clone()
                })
                .await
                .unwrap();

            let (snd, mut rcv) = tokio::sync::oneshot::channel();
            sender
                .send(SchedulerMessage::ScheduleMigration {
                    schema: "schema".into(),
                    migration: Program::seq(&["create table test (c)"]).into(),
                    ret: snd,
                })
                .await
                .unwrap();

            // step until we get a response
            loop {
                tokio::select! {
                    _ = &mut rcv => break,
                    _ = scheduler.step(&mut receiver) => {},
                }
            }

            // this is right before the task gets enqueued
            assert!(!block_write.load(std::sync::atomic::Ordering::Relaxed));
            // next step should enqueue the task
            scheduler.step(&mut receiver).await;
            // task returns and result is collected
            scheduler.step(&mut receiver).await;
            assert!(block_write.load(std::sync::atomic::Ordering::Relaxed));

            // at this point we drop everything and recreated the store (simultes a restart mid-task)
        }

        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let conn = maker().unwrap();
        let meta_store = MetaStore::new(Default::default(), tmp.path(), conn, manager)
            .await
            .unwrap();
        let (sender, _receiver) = mpsc::channel(100);
        let config = make_config(sender.clone().into(), tmp.path());
        let store = NamespaceStore::new(false, false, 10, config, meta_store)
            .await
            .unwrap();

        store
            .with("ns".into(), |ns| {
                assert!(ns
                    .db
                    .as_primary()
                    .unwrap()
                    .block_writes
                    .load(std::sync::atomic::Ordering::Relaxed));
            })
            .await
            .unwrap();
    }
}
