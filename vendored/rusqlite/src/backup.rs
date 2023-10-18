//! Online SQLite backup API.
//!
//! To create a [`Backup`], you must have two distinct [`Connection`]s - one
//! for the source (which can be used while the backup is running) and one for
//! the destination (which cannot).  A [`Backup`] handle exposes three methods:
//! [`step`](Backup::step) will attempt to back up a specified number of pages,
//! [`progress`](Backup::progress) gets the current progress of the backup as of
//! the last call to [`step`](Backup::step), and
//! [`run_to_completion`](Backup::run_to_completion) will attempt to back up the
//! entire source database, allowing you to specify how many pages are backed up
//! at a time and how long the thread should sleep between chunks of pages.
//!
//! The following example is equivalent to "Example 2: Online Backup of a
//! Running Database" from [SQLite's Online Backup API
//! documentation](https://www.sqlite.org/backup.html).
//!
//! ```rust,no_run
//! # use rusqlite::{backup, Connection, Result};
//! # use std::path::Path;
//! # use std::time;
//!
//! fn backup_db<P: AsRef<Path>>(
//!     src: &Connection,
//!     dst: P,
//!     progress: fn(backup::Progress),
//! ) -> Result<()> {
//!     let mut dst = Connection::open(dst)?;
//!     let backup = backup::Backup::new(src, &mut dst)?;
//!     backup.run_to_completion(5, time::Duration::from_millis(250), Some(progress))
//! }
//! ```

use std::marker::PhantomData;
use std::path::Path;
use std::ptr;

use std::os::raw::c_int;
use std::thread;
use std::time::Duration;

use crate::ffi;

use crate::error::error_from_handle;
use crate::{Connection, DatabaseName, Result};

impl Connection {
    /// Back up the `name` database to the given
    /// destination path.
    ///
    /// If `progress` is not `None`, it will be called periodically
    /// until the backup completes.
    ///
    /// For more fine-grained control over the backup process (e.g.,
    /// to sleep periodically during the backup or to back up to an
    /// already-open database connection), see the `backup` module.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the destination path cannot be opened
    /// or if the backup fails.
    pub fn backup<P: AsRef<Path>>(
        &self,
        name: DatabaseName<'_>,
        dst_path: P,
        progress: Option<fn(Progress)>,
    ) -> Result<()> {
        use self::StepResult::{Busy, Done, Locked, More};
        let mut dst = Connection::open(dst_path)?;
        let backup = Backup::new_with_names(self, name, &mut dst, DatabaseName::Main)?;

        let mut r = More;
        while r == More {
            r = backup.step(100)?;
            if let Some(f) = progress {
                f(backup.progress());
            }
        }

        match r {
            Done => Ok(()),
            Busy => Err(unsafe { error_from_handle(ptr::null_mut(), ffi::SQLITE_BUSY) }),
            Locked => Err(unsafe { error_from_handle(ptr::null_mut(), ffi::SQLITE_LOCKED) }),
            More => unreachable!(),
        }
    }

    /// Restore the given source path into the
    /// `name` database. If `progress` is not `None`, it will be
    /// called periodically until the restore completes.
    ///
    /// For more fine-grained control over the restore process (e.g.,
    /// to sleep periodically during the restore or to restore from an
    /// already-open database connection), see the `backup` module.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the destination path cannot be opened
    /// or if the restore fails.
    pub fn restore<P: AsRef<Path>, F: Fn(Progress)>(
        &mut self,
        name: DatabaseName<'_>,
        src_path: P,
        progress: Option<F>,
    ) -> Result<()> {
        use self::StepResult::{Busy, Done, Locked, More};
        let src = Connection::open(src_path)?;
        let restore = Backup::new_with_names(&src, DatabaseName::Main, self, name)?;

        let mut r = More;
        let mut busy_count = 0_i32;
        'restore_loop: while r == More || r == Busy {
            r = restore.step(100)?;
            if let Some(ref f) = progress {
                f(restore.progress());
            }
            if r == Busy {
                busy_count += 1;
                if busy_count >= 3 {
                    break 'restore_loop;
                }
                thread::sleep(Duration::from_millis(100));
            }
        }

        match r {
            Done => Ok(()),
            Busy => Err(unsafe { error_from_handle(ptr::null_mut(), ffi::SQLITE_BUSY) }),
            Locked => Err(unsafe { error_from_handle(ptr::null_mut(), ffi::SQLITE_LOCKED) }),
            More => unreachable!(),
        }
    }
}

/// Possible successful results of calling
/// [`Backup::step`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StepResult {
    /// The backup is complete.
    Done,

    /// The step was successful but there are still more pages that need to be
    /// backed up.
    More,

    /// The step failed because appropriate locks could not be acquired. This is
    /// not a fatal error - the step can be retried.
    Busy,

    /// The step failed because the source connection was writing to the
    /// database. This is not a fatal error - the step can be retried.
    Locked,
}

/// Struct specifying the progress of a backup. The
/// percentage completion can be calculated as `(pagecount - remaining) /
/// pagecount`. The progress of a backup is as of the last call to
/// [`step`](Backup::step) - if the source database is modified after a call to
/// [`step`](Backup::step), the progress value will become outdated and
/// potentially incorrect.
#[derive(Copy, Clone, Debug)]
pub struct Progress {
    /// Number of pages in the source database that still need to be backed up.
    pub remaining: c_int,
    /// Total number of pages in the source database.
    pub pagecount: c_int,
}

/// A handle to an online backup.
pub struct Backup<'a, 'b> {
    phantom_from: PhantomData<&'a Connection>,
    to: &'b Connection,
    b: *mut ffi::sqlite3_backup,
}

impl Backup<'_, '_> {
    /// Attempt to create a new handle that will allow backups from `from` to
    /// `to`. Note that `to` is a `&mut` - this is because SQLite forbids any
    /// API calls on the destination of a backup while the backup is taking
    /// place.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying `sqlite3_backup_init` call returns
    /// `NULL`.
    #[inline]
    pub fn new<'a, 'b>(from: &'a Connection, to: &'b mut Connection) -> Result<Backup<'a, 'b>> {
        Backup::new_with_names(from, DatabaseName::Main, to, DatabaseName::Main)
    }

    /// Attempt to create a new handle that will allow backups from the
    /// `from_name` database of `from` to the `to_name` database of `to`. Note
    /// that `to` is a `&mut` - this is because SQLite forbids any API calls on
    /// the destination of a backup while the backup is taking place.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying `sqlite3_backup_init` call returns
    /// `NULL`.
    pub fn new_with_names<'a, 'b>(
        from: &'a Connection,
        from_name: DatabaseName<'_>,
        to: &'b mut Connection,
        to_name: DatabaseName<'_>,
    ) -> Result<Backup<'a, 'b>> {
        let to_name = to_name.as_cstring()?;
        let from_name = from_name.as_cstring()?;

        let to_db = to.db.borrow_mut().db;

        let b = unsafe {
            let b = ffi::sqlite3_backup_init(
                to_db,
                to_name.as_ptr(),
                from.db.borrow_mut().db,
                from_name.as_ptr(),
            );
            if b.is_null() {
                return Err(error_from_handle(to_db, ffi::sqlite3_errcode(to_db)));
            }
            b
        };

        Ok(Backup {
            phantom_from: PhantomData,
            to,
            b,
        })
    }

    /// Gets the progress of the backup as of the last call to
    /// [`step`](Backup::step).
    #[inline]
    #[must_use]
    pub fn progress(&self) -> Progress {
        unsafe {
            Progress {
                remaining: ffi::sqlite3_backup_remaining(self.b),
                pagecount: ffi::sqlite3_backup_pagecount(self.b),
            }
        }
    }

    /// Attempts to back up the given number of pages. If `num_pages` is
    /// negative, will attempt to back up all remaining pages. This will hold a
    /// lock on the source database for the duration, so it is probably not
    /// what you want for databases that are currently active (see
    /// [`run_to_completion`](Backup::run_to_completion) for a better
    /// alternative).
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying `sqlite3_backup_step` call returns
    /// an error code other than `DONE`, `OK`, `BUSY`, or `LOCKED`. `BUSY` and
    /// `LOCKED` are transient errors and are therefore returned as possible
    /// `Ok` values.
    #[inline]
    pub fn step(&self, num_pages: c_int) -> Result<StepResult> {
        use self::StepResult::{Busy, Done, Locked, More};

        let rc = unsafe { ffi::sqlite3_backup_step(self.b, num_pages) };
        match rc {
            ffi::SQLITE_DONE => Ok(Done),
            ffi::SQLITE_OK => Ok(More),
            ffi::SQLITE_BUSY => Ok(Busy),
            ffi::SQLITE_LOCKED => Ok(Locked),
            _ => self.to.decode_result(rc).map(|_| More),
        }
    }

    /// Attempts to run the entire backup. Will call
    /// [`step(pages_per_step)`](Backup::step) as many times as necessary,
    /// sleeping for `pause_between_pages` between each call to give the
    /// source database time to process any pending queries. This is a
    /// direct implementation of "Example 2: Online Backup of a Running
    /// Database" from [SQLite's Online Backup API documentation](https://www.sqlite.org/backup.html).
    ///
    /// If `progress` is not `None`, it will be called after each step with the
    /// current progress of the backup. Note that is possible the progress may
    /// not change if the step returns `Busy` or `Locked` even though the
    /// backup is still running.
    ///
    /// # Failure
    ///
    /// Will return `Err` if any of the calls to [`step`](Backup::step) return
    /// `Err`.
    pub fn run_to_completion(
        &self,
        pages_per_step: c_int,
        pause_between_pages: Duration,
        progress: Option<fn(Progress)>,
    ) -> Result<()> {
        use self::StepResult::{Busy, Done, Locked, More};

        assert!(pages_per_step > 0, "pages_per_step must be positive");

        loop {
            let r = self.step(pages_per_step)?;
            if let Some(progress) = progress {
                progress(self.progress());
            }
            match r {
                More | Busy | Locked => thread::sleep(pause_between_pages),
                Done => return Ok(()),
            }
        }
    }
}

impl Drop for Backup<'_, '_> {
    #[inline]
    fn drop(&mut self) {
        unsafe { ffi::sqlite3_backup_finish(self.b) };
    }
}

#[cfg(test)]
mod test {
    use super::Backup;
    use crate::{Connection, DatabaseName, Result};
    use std::time::Duration;

    #[test]
    fn test_backup() -> Result<()> {
        let src = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(42);
                   END;";
        src.execute_batch(sql)?;

        let mut dst = Connection::open_in_memory()?;

        {
            let backup = Backup::new(&src, &mut dst)?;
            backup.step(-1)?;
        }

        let the_answer: i64 = dst.one_column("SELECT x FROM foo")?;
        assert_eq!(42, the_answer);

        src.execute_batch("INSERT INTO foo VALUES(43)")?;

        {
            let backup = Backup::new(&src, &mut dst)?;
            backup.run_to_completion(5, Duration::from_millis(250), None)?;
        }

        let the_answer: i64 = dst.one_column("SELECT SUM(x) FROM foo")?;
        assert_eq!(42 + 43, the_answer);
        Ok(())
    }

    #[test]
    fn test_backup_temp() -> Result<()> {
        let src = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TEMPORARY TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(42);
                   END;";
        src.execute_batch(sql)?;

        let mut dst = Connection::open_in_memory()?;

        {
            let backup =
                Backup::new_with_names(&src, DatabaseName::Temp, &mut dst, DatabaseName::Main)?;
            backup.step(-1)?;
        }

        let the_answer: i64 = dst.one_column("SELECT x FROM foo")?;
        assert_eq!(42, the_answer);

        src.execute_batch("INSERT INTO foo VALUES(43)")?;

        {
            let backup =
                Backup::new_with_names(&src, DatabaseName::Temp, &mut dst, DatabaseName::Main)?;
            backup.run_to_completion(5, Duration::from_millis(250), None)?;
        }

        let the_answer: i64 = dst.one_column("SELECT SUM(x) FROM foo")?;
        assert_eq!(42 + 43, the_answer);
        Ok(())
    }

    #[test]
    fn test_backup_attached() -> Result<()> {
        let src = Connection::open_in_memory()?;
        let sql = "ATTACH DATABASE ':memory:' AS my_attached;
                   BEGIN;
                   CREATE TABLE my_attached.foo(x INTEGER);
                   INSERT INTO my_attached.foo VALUES(42);
                   END;";
        src.execute_batch(sql)?;

        let mut dst = Connection::open_in_memory()?;

        {
            let backup = Backup::new_with_names(
                &src,
                DatabaseName::Attached("my_attached"),
                &mut dst,
                DatabaseName::Main,
            )?;
            backup.step(-1)?;
        }

        let the_answer: i64 = dst.one_column("SELECT x FROM foo")?;
        assert_eq!(42, the_answer);

        src.execute_batch("INSERT INTO foo VALUES(43)")?;

        {
            let backup = Backup::new_with_names(
                &src,
                DatabaseName::Attached("my_attached"),
                &mut dst,
                DatabaseName::Main,
            )?;
            backup.run_to_completion(5, Duration::from_millis(250), None)?;
        }

        let the_answer: i64 = dst.one_column("SELECT SUM(x) FROM foo")?;
        assert_eq!(42 + 43, the_answer);
        Ok(())
    }
}
