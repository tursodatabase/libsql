use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{connection::program::Program, namespace::NamespaceName};

#[derive(Debug)]
pub struct MigrationTask {
    pub(crate) namespace: NamespaceName,
    pub(crate) status: MigrationTaskStatus,
    pub(crate) job_id: i64,
    pub(crate) task_id: i64,
}

impl MigrationTask {
    pub(crate) fn namespace(&self) -> NamespaceName {
        self.namespace.clone()
    }

    pub(crate) fn job_id(&self) -> i64 {
        self.job_id
    }

    pub(crate) fn status(&self) -> &MigrationTaskStatus {
        &self.status
    }

    pub(crate) fn status_mut(&mut self) -> &mut MigrationTaskStatus {
        &mut self.status
    }

    pub fn task_id(&self) -> i64 {
        self.task_id
    }
}

#[derive(Debug, Clone)]
pub struct MigrationJob {
    pub(super) schema: NamespaceName,
    pub(super) status: MigrationJobStatus,
    pub(super) job_id: i64,
    pub(super) migration: Arc<Program>,
    pub(super) progress: [usize; MigrationTaskStatus::num_variants()],
    /// error info for the task that failed the job
    pub(super) task_error: Option<(i64, String, NamespaceName)>,
}

impl MigrationJob {
    /// Returns the number of tasks in the given progress state
    pub(crate) fn progress(&self, status: MigrationTaskStatus) -> usize {
        self.progress[status as usize]
    }

    pub(crate) fn progress_mut(&mut self, status: MigrationTaskStatus) -> &mut usize {
        &mut self.progress[status as usize]
    }

    /// Returns true if all the tasks are in the given status
    pub(crate) fn progress_all(&self, status: MigrationTaskStatus) -> bool {
        for (i, count) in self.progress.iter().enumerate() {
            if i != status as usize && *count > 0 {
                return false;
            }
        }

        true
    }

    pub(crate) fn job_id(&self) -> i64 {
        self.job_id
    }

    pub(crate) fn migration(&self) -> Arc<Program> {
        self.migration.clone()
    }

    pub(crate) fn status(&self) -> &MigrationJobStatus {
        &self.status
    }

    pub(crate) fn status_mut(&mut self) -> &mut MigrationJobStatus {
        &mut self.status
    }

    pub(crate) fn schema(&self) -> NamespaceName {
        self.schema.clone()
    }

    pub(super) fn count_pending_tasks(&self) -> usize {
        match self.status() {
            MigrationJobStatus::WaitingDryRun => self.progress(MigrationTaskStatus::Enqueued),
            MigrationJobStatus::WaitingRun => {
                self.progress(MigrationTaskStatus::DryRunSuccess)
                    + self.progress(MigrationTaskStatus::Run)
            }
            MigrationJobStatus::DryRunSuccess => 0,
            MigrationJobStatus::DryRunFailure => {
                self.progress.iter().sum::<usize>() - self.progress(MigrationTaskStatus::Failure)
            }
            MigrationJobStatus::RunSuccess => 0,
            MigrationJobStatus::RunFailure => 0,
            MigrationJobStatus::WaitingTransition => 0,
        }
    }
}

/// Represents the status of a migration task
#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
#[repr(u64)]
pub enum MigrationTaskStatus {
    /// The task was enqueued, and should perform a dry run
    Enqueued = 0,
    /// The dry run was successfull
    DryRunSuccess = 1,
    /// The dry run failed
    DryRunFailure = 2,
    /// The migration task should be performed
    Run = 3,
    /// The migration task was a success
    Success = 4,
    /// The migration task was a failure
    Failure = 5,
}

impl MigrationTaskStatus {
    pub fn from_int(i: u64) -> Self {
        match i {
            0 => Self::Enqueued,
            1 => Self::DryRunSuccess,
            2 => Self::DryRunFailure,
            3 => Self::Run,
            4 => Self::Success,
            5 => Self::Failure,
            _ => panic!(),
        }
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, Self::DryRunFailure | Self::Failure)
    }

    const fn num_variants() -> usize {
        // the only use of this is to create a compile error if someone adds a variant
        match Self::Enqueued {
            MigrationTaskStatus::Enqueued => (),
            MigrationTaskStatus::DryRunSuccess => (),
            MigrationTaskStatus::DryRunFailure => (),
            MigrationTaskStatus::Run => (),
            MigrationTaskStatus::Success => (),
            MigrationTaskStatus::Failure => (),
        }

        // don't forget to update that!
        6
    }

    /// Returns `true` if the migration task status is [`Enqueued`].
    ///
    /// [`Enqueued`]: MigrationTaskStatus::Enqueued
    #[must_use]
    pub fn is_enqueued(&self) -> bool {
        matches!(self, Self::Enqueued)
    }

    pub(super) fn finished_states() -> &'static [Self] {
        &[Self::Success, Self::Failure]
    }

    pub(super) fn is_finished(&self) -> bool {
        Self::finished_states().contains(self)
    }
}

/// Represents the status of a migration job
#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
#[repr(u64)]
pub enum MigrationJobStatus {
    /// Waiting for all tasks to report the dry run status
    WaitingDryRun = 0,
    /// All tasks reported a successfull dry run status
    DryRunSuccess = 1,
    /// One or more task reported an unsuccessful dry drun
    DryRunFailure = 2,
    /// Waiting for all the tasks to return a successful migration run
    WaitingRun = 3,
    /// All tasks retuned successfully, and teh schema was updated successfully
    RunSuccess = 4,
    /// something fucked up
    RunFailure = 5,
    /// transient state: waiting for state transitionning
    WaitingTransition = 6,
}

impl MigrationJobStatus {
    pub(crate) fn from_int(i: u64) -> Self {
        match i {
            0 => Self::WaitingDryRun,
            1 => Self::DryRunSuccess,
            2 => Self::DryRunFailure,
            3 => Self::WaitingRun,
            4 => Self::RunSuccess,
            5 => Self::RunFailure,
            6 => Self::WaitingTransition,
            _ => panic!(),
        }
    }

    /// Returns a list of MigrationJobStatus considered finished states.
    pub(crate) fn finished_states() -> &'static [Self] {
        &[Self::RunSuccess, Self::RunFailure]
    }

    pub(crate) fn is_finished(&self) -> bool {
        Self::finished_states().contains(self)
    }

    /// Returns `true` if the migration job status is [`WaitingRun`].
    ///
    /// [`WaitingRun`]: MigrationJobStatus::WaitingRun
    #[must_use]
    pub fn is_waiting_run(&self) -> bool {
        matches!(self, Self::WaitingRun)
    }

    /// Returns `true` if the migration job status is [`DryRunSuccess`].
    ///
    /// [`DryRunSuccess`]: MigrationJobStatus::DryRunSuccess
    #[must_use]
    pub fn is_dry_run_success(&self) -> bool {
        matches!(self, Self::DryRunSuccess)
    }

    /// Returns `true` if the migration job status is [`DryRunFailure`].
    ///
    /// [`DryRunFailure`]: MigrationJobStatus::DryRunFailure
    #[must_use]
    pub fn is_dry_run_failure(&self) -> bool {
        matches!(self, Self::DryRunFailure)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MigrationSummary {
    pub schema_version: i64,
    pub migrations: Vec<MigrationJobSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MigrationJobSummary {
    pub job_id: u64,
    pub status: Option<MigrationJobStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MigrationDetails {
    pub job_id: u64,
    pub status: MigrationJobStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub progress: Vec<MigrationJobProgress>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MigrationJobProgress {
    pub namespace: String,
    pub status: Option<MigrationJobStatus>,
    pub error: Option<String>,
}
