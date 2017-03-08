//! Run-Time Limits

use std::os::raw::c_int;

use ffi;
pub use ffi::Limit;

use Connection;

impl Connection {
    /// Returns the current value of a limit.
    pub fn limit(&self, limit: Limit) -> i32 {
        let c = self.db.borrow();
        unsafe { ffi::sqlite3_limit(c.db(), limit as c_int, -1) }
    }

    /// Changes the limit to `new_val`, returning the prior value of the limit.
    pub fn set_limit(&self, limit: Limit, new_val: i32) -> i32 {
        let c = self.db.borrow_mut();
        unsafe { ffi::sqlite3_limit(c.db(), limit as c_int, new_val) }
    }
}

#[cfg(test)]
mod test {
    use ffi::Limit;
    use Connection;

    #[test]
    fn test_limit() {
        let db = Connection::open_in_memory().unwrap();
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
        if ::version_number() >= 3006018 {
            db.set_limit(Limit::SQLITE_LIMIT_TRIGGER_DEPTH, 32);
            assert_eq!(32, db.limit(Limit::SQLITE_LIMIT_TRIGGER_DEPTH));
        }

        // SQLITE_LIMIT_WORKER_THREADS was added in SQLite 3.8.7.
        if ::version_number() >= 3008007 {
            db.set_limit(Limit::SQLITE_LIMIT_WORKER_THREADS, 2);
            assert_eq!(2, db.limit(Limit::SQLITE_LIMIT_WORKER_THREADS));
        }
    }
}
