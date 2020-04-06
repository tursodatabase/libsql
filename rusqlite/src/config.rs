//! Configure database connections

use std::os::raw::c_int;

use crate::ffi;
use crate::{Connection, Result};

/// Database Connection Configuration Options
#[repr(i32)]
#[allow(non_snake_case, non_camel_case_types)]
#[non_exhaustive]
pub enum DbConfig {
    //SQLITE_DBCONFIG_MAINDBNAME = 1000, /* const char* */
    //SQLITE_DBCONFIG_LOOKASIDE = 1001,  /* void* int int */
    SQLITE_DBCONFIG_ENABLE_FKEY = 1002,
    SQLITE_DBCONFIG_ENABLE_TRIGGER = 1003,
    SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER = 1004, // 3.12.0
    //SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION = 1005,
    SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE = 1006, // 3.16.2
    SQLITE_DBCONFIG_ENABLE_QPSG = 1007,      // 3.20.0
    SQLITE_DBCONFIG_TRIGGER_EQP = 1008,      // 3.22.0
    //SQLITE_DBCONFIG_RESET_DATABASE = 1009,
    SQLITE_DBCONFIG_DEFENSIVE = 1010, // 3.26.0
    #[cfg(feature = "modern_sqlite")]
    SQLITE_DBCONFIG_WRITABLE_SCHEMA = 1011, // 3.28.0
    #[cfg(feature = "modern_sqlite")]
    SQLITE_DBCONFIG_LEGACY_ALTER_TABLE = 1012, // 3.29
    #[cfg(feature = "modern_sqlite")]
    SQLITE_DBCONFIG_DQS_DML = 1013, // 3.29.0
    #[cfg(feature = "modern_sqlite")]
    SQLITE_DBCONFIG_DQS_DDL = 1014, // 3.29.0
    #[cfg(feature = "modern_sqlite")]
    SQLITE_DBCONFIG_ENABLE_VIEW = 1015, // 3.30.0
    #[cfg(feature = "modern_sqlite")]
    SQLITE_DBCONFIG_LEGACY_FILE_FORMAT = 1016, // 3.31.0
    #[cfg(feature = "modern_sqlite")]
    SQLITE_DBCONFIG_TRUSTED_SCHEMA = 1017, // 3.31.0
}

impl Connection {
    /// Returns the current value of a `config`.
    ///
    /// - SQLITE_DBCONFIG_ENABLE_FKEY: return `false` or `true` to indicate
    ///   whether FK enforcement is off or on
    /// - SQLITE_DBCONFIG_ENABLE_TRIGGER: return `false` or `true` to indicate
    ///   whether triggers are disabled or enabled
    /// - SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER: return `false` or `true` to
    ///   indicate whether fts3_tokenizer are disabled or enabled
    /// - SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE: return `false` to indicate
    ///   checkpoints-on-close are not disabled or `true` if they are
    /// - SQLITE_DBCONFIG_ENABLE_QPSG: return `false` or `true` to indicate
    ///   whether the QPSG is disabled or enabled
    /// - SQLITE_DBCONFIG_TRIGGER_EQP: return `false` to indicate
    ///   output-for-trigger are not disabled or `true` if it is
    pub fn db_config(&self, config: DbConfig) -> Result<bool> {
        let c = self.db.borrow();
        unsafe {
            let mut val = 0;
            check!(ffi::sqlite3_db_config(
                c.db(),
                config as c_int,
                -1,
                &mut val
            ));
            Ok(val != 0)
        }
    }

    /// Make configuration changes to a database connection
    ///
    /// - SQLITE_DBCONFIG_ENABLE_FKEY: `false` to disable FK enforcement, `true`
    ///   to enable FK enforcement
    /// - SQLITE_DBCONFIG_ENABLE_TRIGGER: `false` to disable triggers, `true` to
    ///   enable triggers
    /// - SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER: `false` to disable
    ///   fts3_tokenizer(), `true` to enable fts3_tokenizer()
    /// - SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE: `false` (the default) to enable
    ///   checkpoints-on-close, `true` to disable them
    /// - SQLITE_DBCONFIG_ENABLE_QPSG: `false` to disable the QPSG, `true` to
    ///   enable QPSG
    /// - SQLITE_DBCONFIG_TRIGGER_EQP: `false` to disable output for trigger
    ///   programs, `true` to enable it
    pub fn set_db_config(&self, config: DbConfig, new_val: bool) -> Result<bool> {
        let c = self.db.borrow_mut();
        unsafe {
            let mut val = 0;
            check!(ffi::sqlite3_db_config(
                c.db(),
                config as c_int,
                if new_val { 1 } else { 0 },
                &mut val
            ));
            Ok(val != 0)
        }
    }
}

#[cfg(test)]
mod test {
    use super::DbConfig;
    use crate::Connection;

    #[test]
    fn test_db_config() {
        let db = Connection::open_in_memory().unwrap();

        let opposite = !db.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY).unwrap();
        assert_eq!(
            db.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, opposite),
            Ok(opposite)
        );
        assert_eq!(
            db.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY),
            Ok(opposite)
        );

        let opposite = !db
            .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)
            .unwrap();
        assert_eq!(
            db.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, opposite),
            Ok(opposite)
        );
        assert_eq!(
            db.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER),
            Ok(opposite)
        );
    }
}
