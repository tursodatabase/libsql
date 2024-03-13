use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use futures_core::Future;
use parking_lot::Mutex;
use rusqlite::TransactionBehavior;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task;
use tokio::task::JoinSet;

use crate::connection::program::Program;
use crate::connection::MakeConnection;
use crate::database::PrimaryConnectionMaker;
use crate::namespace::meta_store::{MetaStore, MetaStoreConnection};
use crate::namespace::{NamespaceName, NamespaceStore};
use crate::query_result_builder::{IgnoreResult, QueryBuilderConfig};
use crate::schema::db::{get_unfinished_task_batch, update_job_status, update_meta_task_status};
use crate::schema::{step_migration_task_run, MigrationJobStatus};

use super::db::{
    get_next_pending_migration_job, get_next_pending_migration_tasks_batch,
    job_step_dry_run_success, register_schema_migration_job, setup_schema,
};
use super::error::Error;
use super::handle::JobHandle;
use super::migration::enqueue_migration_task;
use super::status::{MigrationJob, MigrationTask};
use super::{
    abort_migration_task, perform_migration, step_task, MigrationTaskStatus, SchedulerMessage,
};

const MAX_CONCURRENT: usize = 10;

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
    event_notifier: tokio::sync::broadcast::Sender<(i64, MigrationJobStatus)>,
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
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT)),
            event_notifier: tokio::sync::broadcast::Sender::new(32),
        })
    }

    pub async fn run(mut self, mut receiver: mpsc::Receiver<SchedulerMessage>) {
        const MAX_ERROR_RETRIES: usize = 10;
        let mut tries = 0;
        loop {
            match self.step(&mut receiver).await {
                Ok(true) => {
                    tries = 0;
                }
                Ok(false) => {
                    tracing::info!("all scheduler handles dropped: exiting.");
                    break;
                }
                Err(e) => {
                    if tries >= MAX_ERROR_RETRIES {
                        tracing::error!("scheduler could not make progress after {MAX_ERROR_RETRIES}, exiting: {e}");
                        break;
                    } else {
                        tracing::error!("an error occured while stepping the scheduler, {} tries remaining: {e}", MAX_ERROR_RETRIES - tries);
                        tries += 1;
                    }
                }
            }
        }
    }

    #[inline]
    async fn step(
        &mut self,
        receiver: &mut mpsc::Receiver<SchedulerMessage>,
    ) -> Result<bool, Error> {
        tokio::select! {
            Some(msg) = receiver.recv() => {
                self.handle_msg(msg).await;
            }
            // There is work to do, and a worker slot to perform it
            // TODO: optim: we could try enqueue more work in a go by try_acquiring more
            // permits here
            Ok(permit) = self.permits.clone().acquire_owned(), if self.has_work => {
                self.enqueue_work(permit).await?;
            }
            Some(res) = self.workers.join_next(), if !self.workers.is_empty() => {
                match res {
                    Ok(WorkResult::Task { old_status, task, error }) => {
                        let new_status = *task.status();
                        let current_job = self.current_job
                            .as_mut()
                            .expect("processing task result, but job is missing");

                        *current_job.progress_mut(old_status) -= 1;
                        *current_job.progress_mut(new_status) += 1;
                        if current_job.task_error.is_none() && error.is_some() {
                            current_job.task_error = error.map(|e| (task.task_id, e, task.namespace()));
                        }

                        // we have more work if:
                        // - the current batch has more tasks to enqueue
                        // - the remaining number of pending tasks is greater than the amount of in-flight tasks: we can enqueue more
                        // - there's no more in-flight nor pending tasks for the job: we need to step the job
                        let in_flight = MAX_CONCURRENT - self.permits.available_permits();
                        let pending_tasks = current_job.count_pending_tasks();
                        self.has_work = !self.current_batch.is_empty() || (pending_tasks == 0 && in_flight == 0) || pending_tasks > in_flight;
                    }
                    Ok(WorkResult::Job { status }) => {
                        let job_id = if status.is_finished() {
                            let job = self.current_job.take().unwrap();
                            job.job_id
                        } else {
                            let current_job = self.current_job
                                .as_mut()
                                .expect("job is missing, but got status update for that job");
                            *current_job.status_mut() = status;
                            current_job.job_id()
                        };

                        let _ = self.event_notifier.send((job_id, status));

                        self.has_work = true;
                    }
                    Err(e) => {
                        todo!("migration task panicked: {e}");
                    }
                }
            }
            else => return Ok(false),
        }

        Ok(true)
    }

    async fn handle_msg(&mut self, msg: SchedulerMessage) {
        match msg {
            SchedulerMessage::ScheduleMigration {
                schema,
                migration,
                ret,
            } => {
                let res = self.register_migration_job(schema, migration).await;
                let _ = ret.send(res.map(|id| JobHandle::new(id, self.event_notifier.subscribe())));
                // it not necessary to raise the flag if we are currently processing a job: it
                // prevents spurious wakeups, and the job will be picked up anyway.
                self.has_work = self.current_job.is_none();
            }
            SchedulerMessage::GetJobStatus { job_id, ret } => {
                let res = self.get_job_status(job_id).await;
                let _ = ret.send(res);
            }
        }
    }

    async fn maybe_step_job(
        &mut self,
        permit: OwnedSemaphorePermit,
    ) -> Result<Option<OwnedSemaphorePermit>, Error> {
        let job = match self.current_job {
            Some(ref mut job) => job,
            None => {
                let maybe_next_job = with_conn_async(self.migration_db.clone(), move |conn| {
                    get_next_pending_migration_job(conn)
                })
                .await?;
                match maybe_next_job {
                    Some(job) => self.current_job.insert(job),
                    None => {
                        self.has_work = false;
                        return Ok(None);
                    }
                }
            }
        };

        // try to step the current job
        match *job.status() {
            MigrationJobStatus::WaitingDryRun => {
                // there was a dry run failure, abort the task
                if job.progress(MigrationTaskStatus::DryRunFailure) != 0 {
                    let error = job
                        .task_error
                        .clone()
                        .expect("task error reported, but error is missing");
                    self.workers.spawn(step_job_dry_run_failure(
                        permit,
                        self.migration_db.clone(),
                        job.job_id(),
                        self.namespace_store.clone(),
                        MigrationJobStatus::WaitingDryRun,
                        error,
                    ));
                    *job.status_mut() = MigrationJobStatus::WaitingTransition;
                    self.has_work = false;
                    return Ok(None);
                }

                // all tasks reported a successful dry run, we are ready to step the job state
                if job.progress_all(MigrationTaskStatus::DryRunSuccess) {
                    self.workers.spawn(step_job_dry_run_success(
                        permit,
                        self.migration_db.clone(),
                        job.job_id(),
                        self.namespace_store.clone(),
                    ));
                    *job.status_mut() = MigrationJobStatus::WaitingTransition;
                    self.has_work = false;
                    return Ok(None);
                }
            }
            MigrationJobStatus::DryRunSuccess => {
                self.workers.spawn(step_job_waiting_run(
                    permit,
                    self.migration_db.clone(),
                    job.job_id(),
                    self.namespace_store.clone(),
                ));
                *job.status_mut() = MigrationJobStatus::WaitingTransition;
                self.has_work = false;
                return Ok(None);
            }
            MigrationJobStatus::DryRunFailure => {
                if job.progress_all(MigrationTaskStatus::Failure) {
                    self.workers.spawn(step_job_failure(
                        permit,
                        self.migration_db.clone(),
                        job.job_id(),
                        self.namespace_store.clone(),
                    ));
                    *job.status_mut() = MigrationJobStatus::WaitingTransition;
                    self.has_work = false;
                    return Ok(None);
                }
            }
            MigrationJobStatus::WaitingRun => {
                // there was a dry run failure, abort the task
                if job.progress(MigrationTaskStatus::Failure) != 0 {
                    todo!("that shouldn't happen! retry");
                }

                if job.progress_all(MigrationTaskStatus::Success) {
                    self.workers.spawn(step_job_run_success(
                        permit,
                        job.schema(),
                        job.migration(),
                        job.job_id(),
                        self.namespace_store.clone(),
                        self.migration_db.clone(),
                    ));
                    // do not enqueue anything until the schema migration is complete
                    self.has_work = false;
                    *job.status_mut() = MigrationJobStatus::WaitingTransition;
                    return Ok(None);
                }
            }
            MigrationJobStatus::WaitingTransition => {
                // just wait for schema update to return
                // this is a transient state, and it's not persisted. It's only necessary to make
                // the code more robust when there are spurious wakups that would cause to this
                // function being called;
                self.has_work = false;
                return Ok(None);
            }
            MigrationJobStatus::RunSuccess => unreachable!(),
            MigrationJobStatus::RunFailure => todo!("handle run failure"),
        }

        Ok(Some(permit))
    }

    async fn enqueue_task(&mut self, permit: OwnedSemaphorePermit) -> Result<(), Error> {
        let Some(ref job) = self.current_job else {
            return Ok(());
        };
        if self.current_batch.is_empty()
            && matches!(
                *job.status(),
                MigrationJobStatus::WaitingDryRun
                    | MigrationJobStatus::WaitingRun
                    | MigrationJobStatus::DryRunFailure
            )
        {
            const MAX_BATCH_SIZE: usize = 50;
            // get a batch of enqueued tasks
            let job_id = job.job_id();
            self.current_batch = match *job.status() {
                MigrationJobStatus::WaitingDryRun => {
                    with_conn_async(self.migration_db.clone(), move |conn| {
                        get_next_pending_migration_tasks_batch(
                            conn,
                            job_id,
                            MigrationTaskStatus::Enqueued,
                            MAX_BATCH_SIZE,
                        )
                    })
                    .await?
                }
                MigrationJobStatus::WaitingRun => {
                    with_conn_async(self.migration_db.clone(), move |conn| {
                        get_next_pending_migration_tasks_batch(
                            conn,
                            job_id,
                            MigrationTaskStatus::DryRunSuccess,
                            MAX_BATCH_SIZE,
                        )
                    })
                    .await?
                }
                MigrationJobStatus::DryRunFailure => {
                    // in case of dry run failure we are failing all the tasks
                    with_conn_async(self.migration_db.clone(), move |conn| {
                        get_unfinished_task_batch(conn, job_id, MAX_BATCH_SIZE)
                    })
                    .await?
                }
                _ => unreachable!(),
            };
        }

        // enqueue some work
        if let Some(task) = self.current_batch.pop() {
            let (connection_maker, block_writes) =
                self.namespace_store
                    .with(task.namespace(), move |ns| {
                        let db = ns.db.as_primary().expect(
                            "attempting to perform schema migration on non-primary database",
                        );
                        (db.connection_maker().clone(), db.block_writes.clone())
                    })
                    .await
                    .map_err(|e| Error::NamespaceLoad(Box::new(e)))?;

            // we block the writes before enqueuing the task, it makes testing predictable
            if *task.status() == MigrationTaskStatus::Enqueued {
                block_writes.store(true, std::sync::atomic::Ordering::SeqCst);
            }

            self.workers.spawn(try_step_task(
                permit,
                self.namespace_store.clone(),
                self.migration_db.clone(),
                connection_maker,
                *job.status(),
                job.migration.clone(),
                task,
                block_writes,
            ));
        } else {
            // there is still a job, but the queue is empty, it means that we are waiting for the
            // remaining jobs to report status. just wait.
            self.has_work = false;
        }

        Ok(())
    }

    // TODO: refactor this function it's turning into a mess. Not so simple, because of borrow
    // constraints
    async fn enqueue_work(&mut self, permit: OwnedSemaphorePermit) -> Result<(), Error> {
        let Some(permit) = self.maybe_step_job(permit).await? else {
            return Ok(());
        };
        // fill the current batch if necessary
        self.enqueue_task(permit).await?;

        Ok(())
    }

    pub async fn register_migration_job(
        &self,
        schema: NamespaceName,
        migration: Arc<Program>,
    ) -> Result<i64, Error> {
        // acquire an exclusive lock to the schema before enqueueing to ensure that no namespaces
        // are still being created before we register the migration
        let _lock = self
            .namespace_store
            .schema_locks()
            .acquire_exlusive(schema.clone())
            .await;
        with_conn_async(self.migration_db.clone(), move |conn| {
            register_schema_migration_job(conn, &schema, &migration)
        })
        .await
    }

    async fn get_job_status(
        &self,
        job_id: i64,
    ) -> Result<(MigrationJobStatus, Option<String>), Error> {
        with_conn_async(self.migration_db.clone(), move |conn| {
            super::db::get_job_status(conn, job_id)
        })
        .await
    }
}

async fn try_step_task(
    _permit: OwnedSemaphorePermit,
    namespace_store: NamespaceStore,
    migration_db: Arc<Mutex<MetaStoreConnection>>,
    connection_maker: Arc<PrimaryConnectionMaker>,
    job_status: MigrationJobStatus,
    migration: Arc<Program>,
    mut task: MigrationTask,
    block_writes: Arc<AtomicBool>,
) -> WorkResult {
    let old_status = *task.status();
    let error = match try_step_task_inner(
        namespace_store,
        connection_maker,
        job_status,
        migration,
        &task,
        block_writes,
    )
    .await
    {
        Ok((status, error)) => {
            *task.status_mut() = status;
            error
        }
        Err(e) => {
            tracing::error!(
                "error processing task {} for {}, rescheduling for later: {e}",
                task.task_id(),
                task.namespace()
            );
            None
        }
    };

    {
        let mut conn = migration_db.lock();
        if let Err(e) = update_meta_task_status(&mut conn, &task, error.as_deref()) {
            tracing::error!("failed to update task status, retryng later: {e}");
            *task.status_mut() = old_status;
        }
    }

    WorkResult::Task {
        old_status,
        task,
        error,
    }
}

async fn try_step_task_inner(
    namespace_store: NamespaceStore,
    connection_maker: Arc<PrimaryConnectionMaker>,
    job_status: MigrationJobStatus,
    migration: Arc<Program>,
    task: &MigrationTask,
    block_writes: Arc<AtomicBool>,
) -> Result<(MigrationTaskStatus, Option<String>), Error> {
    let status = *task.status();
    let mut db_connection = connection_maker
        .create()
        .await
        .map_err(|e| Error::FailedToConnect(task.namespace(), Box::new(e)))?;
    if task.status().is_enqueued() {
        // once writes are blocked, we first make sure that
        // there are no ongoing transactions...
        db_connection = task::spawn_blocking(move || -> Result<_, Error> {
            db_connection.with_raw(|conn| -> Result<_, Error> {
                conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                Ok(())
            })?;
            Ok(db_connection)
        })
        .await
        .expect("task panicked")?;
    }

    let job_id = task.job_id();
    let (status, error) = tokio::task::spawn_blocking(move || -> Result<_, Error> {
        db_connection.with_raw(move |conn| {
            let mut txn = conn.transaction()?;

            match status {
                _ if job_status.is_dry_run_failure() => {
                    abort_migration_task(&txn, job_id)?;
                }
                MigrationTaskStatus::Enqueued => {
                    enqueue_migration_task(&txn, job_id, status, &migration)?;
                }
                MigrationTaskStatus::DryRunSuccess if job_status.is_waiting_run() => {
                    step_migration_task_run(&txn, job_id)?;
                }
                _ => unreachable!("expected task status to be `enqueued` or `run`"),
            }

            let (new_status, error) = step_task(&mut txn, job_id)?;
            txn.commit()?;

            if new_status.is_finished() {
                block_writes.store(false, std::sync::atomic::Ordering::SeqCst);
            }

            Ok((new_status, error))
        })
    })
    .await
    .expect("task panicked")?;

    // ... then we're good to go and make sure that the current database state is
    // in the backup
    backup_namespace(&namespace_store, task.namespace()).await?;

    Ok((status, error))
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
    Job {
        status: MigrationJobStatus,
    },
}

async fn backup_meta_store(meta: &MetaStore) -> Result<(), Error> {
    if let Some(mut savepoint) = meta.backup_savepoint() {
        if let Err(e) = savepoint.confirmed().await {
            tracing::error!("failed to backup meta store: {e}");
            // do not step the job, and schedule for retry.
            // TODO: backoff?
            // TODO: this is fine if we don't manage to get a backup here,
            // then we'll restart in the previous state in case of restore,
            // however, in case of restart we may not have a backup.

            return Err(Error::MetaStoreBackupFailure);
        }
    }

    Ok(())
}

async fn backup_namespace(store: &NamespaceStore, ns: NamespaceName) -> Result<(), Error> {
    let savepoint = store
        .with(ns.clone(), |ns| ns.db.backup_savepoint())
        .await
        .map_err(|e| Error::NamespaceLoad(Box::new(e)))?;

    if let Some(mut savepoint) = savepoint {
        if let Err(e) = savepoint.confirmed().await {
            return Err(Error::NamespaceBackupFailure(ns, e.into()));
        }
    }

    Ok(())
}

async fn try_step_job(
    fallback_state: MigrationJobStatus,
    f: impl Future<Output = Result<MigrationJobStatus, Error>>,
) -> WorkResult {
    let status = match f.await {
        Ok(status) => status,
        Err(e) => {
            tracing::error!("error while stepping job, falling back to previous state: {e}");
            fallback_state
        }
    };

    WorkResult::Job { status }
}

async fn step_job_failure(
    _ermit: OwnedSemaphorePermit,
    migration_db: Arc<Mutex<MetaStoreConnection>>,
    job_id: i64,
    namespace_store: NamespaceStore,
) -> WorkResult {
    try_step_job(MigrationJobStatus::DryRunFailure, async move {
        with_conn_async(migration_db, move |conn| {
            // TODO ensure here that this transition is valid
            // the error must already be there from when we stepped to DryRunFailure
            update_job_status(conn, job_id, MigrationJobStatus::RunFailure, None)
        })
        .await?;

        backup_meta_store(namespace_store.meta_store()).await?;

        Ok(MigrationJobStatus::RunFailure)
    })
    .await
}

async fn step_job_waiting_run(
    _permit: OwnedSemaphorePermit,
    migration_db: Arc<Mutex<MetaStoreConnection>>,
    job_id: i64,
    namespace_store: NamespaceStore,
) -> WorkResult {
    try_step_job(MigrationJobStatus::DryRunSuccess, async move {
        with_conn_async(migration_db, move |conn| {
            // TODO ensure here that this transition is valid
            update_job_status(conn, job_id, MigrationJobStatus::WaitingRun, None)
        })
        .await?;

        backup_meta_store(namespace_store.meta_store()).await?;

        Ok(MigrationJobStatus::WaitingRun)
    })
    .await
}

async fn step_job_dry_run_failure(
    _permit: OwnedSemaphorePermit,
    migration_db: Arc<Mutex<MetaStoreConnection>>,
    job_id: i64,
    namespace_store: NamespaceStore,
    status: MigrationJobStatus,
    (task_id, error, ns): (i64, String, NamespaceName),
) -> WorkResult {
    try_step_job(status, async move {
        with_conn_async(migration_db, move |conn| {
            let error = format!("task {task_id} for namespace `{ns}` failed with error: {error}");
            update_job_status(
                conn,
                job_id,
                MigrationJobStatus::DryRunFailure,
                Some(&error),
            )
        })
        .await?;

        backup_meta_store(namespace_store.meta_store()).await?;
        Ok(MigrationJobStatus::DryRunFailure)
    })
    .await
}

async fn step_job_dry_run_success(
    _permit: OwnedSemaphorePermit,
    migration_db: Arc<Mutex<MetaStoreConnection>>,
    job_id: i64,
    namespace_store: NamespaceStore,
) -> WorkResult {
    try_step_job(MigrationJobStatus::WaitingDryRun, async move {
        with_conn_async(migration_db, move |conn| {
            job_step_dry_run_success(conn, job_id)
        })
        .await?;

        backup_meta_store(namespace_store.meta_store()).await?;

        Ok(MigrationJobStatus::DryRunSuccess)
    })
    .await
}

async fn step_job_run_success(
    _permit: OwnedSemaphorePermit,
    schema: NamespaceName,
    migration: Arc<Program>,
    job_id: i64,
    namespace_store: NamespaceStore,
    migration_db: Arc<Mutex<MetaStoreConnection>>,
) -> WorkResult {
    try_step_job(MigrationJobStatus::WaitingRun, async move {
        // TODO: check that all tasks actually reported success before migration
        let connection_maker = namespace_store
            .with(schema.clone(), |ns| {
                ns.db
                    .as_schema()
                    .expect("expected database to be a schema database")
                    .connection_maker()
                    .clone()
            })
            .await
            .map_err(|e| Error::NamespaceLoad(Box::new(e)))?;

        let connection = connection_maker
            .create()
            .await
            .map_err(|e| Error::FailedToConnect(schema.clone(), e.into()))?;
        tokio::task::spawn_blocking(move || -> Result<(), Error> {
            connection
                .connection()
                .with_raw(|conn| -> Result<(), Error> {
                    let mut txn = conn.transaction()?;
                    let schema_version =
                        txn.query_row("PRAGMA schema_version", (), |row| row.get::<_, i64>(0))?;

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
                        txn.pragma_update(None, "schema_version", job_id)?;
                        // update schema version to job_id?
                        txn.commit()?;
                    }

                    Ok(())
                })
        })
        .await
        .expect("task panicked")?;

        // backup the schema
        backup_namespace(&namespace_store, schema).await?;

        tokio::task::spawn_blocking(move || {
            let mut conn = migration_db.lock();
            update_job_status(&mut conn, job_id, MigrationJobStatus::RunSuccess, None)
        })
        .await
        .expect("task panicked")?;

        backup_meta_store(namespace_store.meta_store()).await?;
        Ok(MigrationJobStatus::RunSuccess)
    })
    .await
}

#[cfg(test)]
mod test {
    use insta::assert_debug_snapshot;
    use std::path::Path;
    use tempfile::tempdir;

    use crate::connection::config::DatabaseConfig;
    use crate::database::DatabaseKind;
    use crate::namespace::meta_store::{metastore_connection_maker, MetaStore};
    use crate::namespace::{NamespaceConfig, RestoreOption};
    use crate::schema::SchedulerHandle;

    use super::super::migration::has_pending_migration_task;
    use super::*;

    // FIXME: lots of coupling here, there whoudl be an easier way to test this.
    #[tokio::test]
    async fn writes_blocked_while_performing_migration() {
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

        let (block_write, ns_conn_maker) = store
            .with("ns".into(), |ns| {
                (
                    ns.db.as_primary().unwrap().block_writes.clone(),
                    ns.db.as_primary().unwrap().connection_maker(),
                )
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
            scheduler.step(&mut receiver).await.unwrap();
            if rcv.try_recv().is_ok() {
                break;
            }
        }

        // this is right before the task gets enqueued
        assert!(!block_write.load(std::sync::atomic::Ordering::Relaxed));
        // next step should enqueue the task
        let conn = ns_conn_maker.create().await.unwrap();

        assert!(!block_write.load(std::sync::atomic::Ordering::Relaxed));
        while conn.with_raw(|conn| !has_pending_migration_task(&conn).unwrap()) {
            scheduler.step(&mut receiver).await.unwrap();
        }
        assert!(block_write.load(std::sync::atomic::Ordering::Relaxed));

        while scheduler.current_job.is_some() {
            scheduler.step(&mut receiver).await.unwrap();
        }

        assert!(!block_write.load(std::sync::atomic::Ordering::Relaxed));
    }

    fn make_config(migration_scheduler: SchedulerHandle, path: &Path) -> NamespaceConfig {
        NamespaceConfig {
            db_kind: DatabaseKind::Primary,
            base_path: path.to_path_buf().into(),
            max_log_size: 1000000000,
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

            let (block_write, ns_conn_maker) = store
                .with("ns".into(), |ns| {
                    (
                        ns.db.as_primary().unwrap().block_writes.clone(),
                        ns.db.as_primary().unwrap().connection_maker(),
                    )
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
                scheduler.step(&mut receiver).await.unwrap();
                if rcv.try_recv().is_ok() {
                    break;
                }
            }

            let conn = ns_conn_maker.create().await.unwrap();

            assert!(!block_write.load(std::sync::atomic::Ordering::Relaxed));
            while conn.with_raw(|conn| !has_pending_migration_task(&conn).unwrap()) {
                scheduler.step(&mut receiver).await.unwrap();
            }
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

    #[tokio::test]
    async fn cant_delete_namespace_while_pending_job() {
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

        let (snd, _rcv) = tokio::sync::oneshot::channel();
        sender
            .send(SchedulerMessage::ScheduleMigration {
                schema: "schema".into(),
                migration: Program::seq(&["create table test (c)"]).into(),
                ret: snd,
            })
            .await
            .unwrap();

        while !super::super::db::has_pending_migration_jobs(
            &scheduler.migration_db.lock(),
            &"schema".into(),
        )
        .unwrap()
        {
            scheduler.step(&mut receiver).await.unwrap();
        }

        assert_debug_snapshot!(store.destroy("ns".into(), true).await.unwrap_err());

        while super::super::db::has_pending_migration_jobs(
            &scheduler.migration_db.lock(),
            &"schema".into(),
        )
        .unwrap()
        {
            scheduler.step(&mut receiver).await.unwrap();
        }

        store.destroy("ns".into(), true).await.unwrap();
    }

    #[tokio::test]
    async fn schema_locks() {
        let tmp = tempdir().unwrap();
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
        let scheduler = Scheduler::new(store.clone(), maker().unwrap()).unwrap();

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

        {
            let _lock = store.schema_locks().acquire_shared("schema".into()).await;
            let fut = scheduler.register_migration_job(
                "schema".into(),
                Program::seq(&["create table test (x)"]).into(),
            );
            // we can't acquire the lock
            assert!(tokio::time::timeout(std::time::Duration::from_secs(1), fut)
                .await
                .is_err());
        }

        {
            // simulate an ongoing migration registration
            let _lock = store.schema_locks().acquire_exlusive("schema".into()).await;

            let fut = store.create(
                "some_namespace".into(),
                RestoreOption::Latest,
                DatabaseConfig {
                    shared_schema_name: Some("schema".into()),
                    ..Default::default()
                },
            );
            // we can't acquire the lock
            assert!(tokio::time::timeout(std::time::Duration::from_secs(1), fut)
                .await
                .is_err());
        }
    }
}
