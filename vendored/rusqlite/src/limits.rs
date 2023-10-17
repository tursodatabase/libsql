//! Run-Time Limits

use crate::{ffi, Connection};
use std::os::raw::c_int;

/// Run-Time limit categories, for use with [`Connection::limit`] and
/// [`Connection::set_limit`].
///
/// See the official documentation for more information:
/// - <https://www.sqlite.org/c3ref/c_limit_attached.html>
/// - <https://www.sqlite.org/limits.html>
#[repr(i32)]
#[non_exhaustive]
#[allow(clippy::upper_case_acronyms, non_camel_case_types)]
#[cfg_attr(docsrs, doc(cfg(feature = "limits")))]
pub enum Limit {
    /// The maximum size of any string or BLOB or table row, in bytes.
    SQLITE_LIMIT_LENGTH = ffi::SQLITE_LIMIT_LENGTH,
    /// The maximum length of an SQL statement, in bytes.
    SQLITE_LIMIT_SQL_LENGTH = ffi::SQLITE_LIMIT_SQL_LENGTH,
    /// The maximum number of columns in a table definition or in the result set
    /// of a SELECT or the maximum number of columns in an index or in an
    /// ORDER BY or GROUP BY clause.
    SQLITE_LIMIT_COLUMN = ffi::SQLITE_LIMIT_COLUMN,
    /// The maximum depth of the parse tree on any expression.
    SQLITE_LIMIT_EXPR_DEPTH = ffi::SQLITE_LIMIT_EXPR_DEPTH,
    /// The maximum number of terms in a compound SELECT statement.
    SQLITE_LIMIT_COMPOUND_SELECT = ffi::SQLITE_LIMIT_COMPOUND_SELECT,
    /// The maximum number of instructions in a virtual machine program used to
    /// implement an SQL statement.
    SQLITE_LIMIT_VDBE_OP = ffi::SQLITE_LIMIT_VDBE_OP,
    /// The maximum number of arguments on a function.
    SQLITE_LIMIT_FUNCTION_ARG = ffi::SQLITE_LIMIT_FUNCTION_ARG,
    /// The maximum number of attached databases.
    SQLITE_LIMIT_ATTACHED = ffi::SQLITE_LIMIT_ATTACHED,
    /// The maximum length of the pattern argument to the LIKE or GLOB
    /// operators.
    SQLITE_LIMIT_LIKE_PATTERN_LENGTH = ffi::SQLITE_LIMIT_LIKE_PATTERN_LENGTH,
    /// The maximum index number of any parameter in an SQL statement.
    SQLITE_LIMIT_VARIABLE_NUMBER = ffi::SQLITE_LIMIT_VARIABLE_NUMBER,
    /// The maximum depth of recursion for triggers.
    SQLITE_LIMIT_TRIGGER_DEPTH = ffi::SQLITE_LIMIT_TRIGGER_DEPTH,
    /// The maximum number of auxiliary worker threads that a single prepared
    /// statement may start.
    SQLITE_LIMIT_WORKER_THREADS = ffi::SQLITE_LIMIT_WORKER_THREADS,
}

impl Connection {
    /// Returns the current value of a [`Limit`].
    #[inline]
    #[cfg_attr(docsrs, doc(cfg(feature = "limits")))]
    pub fn limit(&self, limit: Limit) -> i32 {
        let c = self.db.borrow();
        unsafe { ffi::sqlite3_limit(c.db(), limit as c_int, -1) }
    }

    /// Changes the [`Limit`] to `new_val`, returning the prior
    /// value of the limit.
    #[inline]
    #[cfg_attr(docsrs, doc(cfg(feature = "limits")))]
    pub fn set_limit(&self, limit: Limit, new_val: i32) -> i32 {
        let c = self.db.borrow_mut();
        unsafe { ffi::sqlite3_limit(c.db(), limit as c_int, new_val) }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Connection, Result};

    #[test]
    fn test_limit_values() {
        assert_eq!(Limit::SQLITE_LIMIT_LENGTH as i32, ffi::SQLITE_LIMIT_LENGTH,);
        assert_eq!(
            Limit::SQLITE_LIMIT_SQL_LENGTH as i32,
            ffi::SQLITE_LIMIT_SQL_LENGTH,
        );
        assert_eq!(Limit::SQLITE_LIMIT_COLUMN as i32, ffi::SQLITE_LIMIT_COLUMN,);
        assert_eq!(
            Limit::SQLITE_LIMIT_EXPR_DEPTH as i32,
            ffi::SQLITE_LIMIT_EXPR_DEPTH,
        );
        assert_eq!(
            Limit::SQLITE_LIMIT_COMPOUND_SELECT as i32,
            ffi::SQLITE_LIMIT_COMPOUND_SELECT,
        );
        assert_eq!(
            Limit::SQLITE_LIMIT_VDBE_OP as i32,
            ffi::SQLITE_LIMIT_VDBE_OP,
        );
        assert_eq!(
            Limit::SQLITE_LIMIT_FUNCTION_ARG as i32,
            ffi::SQLITE_LIMIT_FUNCTION_ARG,
        );
        assert_eq!(
            Limit::SQLITE_LIMIT_ATTACHED as i32,
            ffi::SQLITE_LIMIT_ATTACHED,
        );
        assert_eq!(
            Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH as i32,
            ffi::SQLITE_LIMIT_LIKE_PATTERN_LENGTH,
        );
        assert_eq!(
            Limit::SQLITE_LIMIT_VARIABLE_NUMBER as i32,
            ffi::SQLITE_LIMIT_VARIABLE_NUMBER,
        );
        #[cfg(feature = "bundled")]
        assert_eq!(
            Limit::SQLITE_LIMIT_TRIGGER_DEPTH as i32,
            ffi::SQLITE_LIMIT_TRIGGER_DEPTH,
        );
        #[cfg(feature = "bundled")]
        assert_eq!(
            Limit::SQLITE_LIMIT_WORKER_THREADS as i32,
            ffi::SQLITE_LIMIT_WORKER_THREADS,
        );
    }

    #[test]
    fn test_limit() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.set_limit(Limit::SQLITE_LIMIT_LENGTH, 1024);
        assert_eq!(1024, db.limit(Limit::SQLITE_LIMIT_LENGTH));

        db.set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, 1024);
        assert_eq!(1024, db.limit(Limit::SQLITE_LIMIT_SQL_LENGTH));

        db.set_limit(Limit::SQLITE_LIMIT_COLUMN, 64);
        assert_eq!(64, db.limit(Limit::SQLITE_LIMIT_COLUMN));

        db.set_limit(Limit::SQLITE_LIMIT_EXPR_DEPTH, 256);
        assert_eq!(256, db.limit(Limit::SQLITE_LIMIT_EXPR_DEPTH));

        db.set_limit(Limit::SQLITE_LIMIT_COMPOUND_SELECT, 32);
        assert_eq!(32, db.limit(Limit::SQLITE_LIMIT_COMPOUND_SELECT));

        db.set_limit(Limit::SQLITE_LIMIT_FUNCTION_ARG, 32);
        assert_eq!(32, db.limit(Limit::SQLITE_LIMIT_FUNCTION_ARG));

        db.set_limit(Limit::SQLITE_LIMIT_ATTACHED, 2);
        assert_eq!(2, db.limit(Limit::SQLITE_LIMIT_ATTACHED));

        db.set_limit(Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 128);
        assert_eq!(128, db.limit(Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH));

        db.set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 99);
        assert_eq!(99, db.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER));

        // SQLITE_LIMIT_TRIGGER_DEPTH was added in SQLite 3.6.18.
        if crate::version_number() >= 3_006_018 {
            db.set_limit(Limit::SQLITE_LIMIT_TRIGGER_DEPTH, 32);
            assert_eq!(32, db.limit(Limit::SQLITE_LIMIT_TRIGGER_DEPTH));
        }

        // SQLITE_LIMIT_WORKER_THREADS was added in SQLite 3.8.7.
        if crate::version_number() >= 3_008_007 {
            db.set_limit(Limit::SQLITE_LIMIT_WORKER_THREADS, 2);
            assert_eq!(2, db.limit(Limit::SQLITE_LIMIT_WORKER_THREADS));
        }
        Ok(())
    }
}
