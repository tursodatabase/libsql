use serde::{Deserialize, Serialize};

/// Represents the status of a migration task
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
#[repr(u64)]
pub enum MigrationTaskStatus {
    /// The task was enqueued, and shoudl perform a dry run
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

    pub const fn num_variants() -> usize {
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
}

impl MigrationJobStatus {
    pub fn from_int(i: u64) -> Self {
        match i {
            0 => Self::WaitingDryRun,
            1 => Self::DryRunSuccess,
            2 => Self::DryRunFailure,
            3 => Self::WaitingRun,
            4 => Self::RunSuccess,
            5 => Self::RunFailure,
            _ => panic!(),
        }
    }

    /// Returns `true` if the migration job status is [`WaitingRun`].
    ///
    /// [`WaitingRun`]: MigrationJobStatus::WaitingRun
    #[must_use]
    pub fn is_waiting_run(&self) -> bool {
        matches!(self, Self::WaitingRun)
    }
}
