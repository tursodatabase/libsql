//! Commit, Data Change and Rollback Notification Callbacks
#![allow(non_camel_case_types)]

use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, RefUnwindSafe};
use std::ptr;

use crate::ffi;

use crate::{Connection, InnerConnection};

/// Action Codes
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
#[non_exhaustive]
#[allow(clippy::upper_case_acronyms)]
pub enum Action {
    /// Unsupported / unexpected action
    UNKNOWN = -1,
    /// DELETE command
    SQLITE_DELETE = ffi::SQLITE_DELETE,
    /// INSERT command
    SQLITE_INSERT = ffi::SQLITE_INSERT,
    /// UPDATE command
    SQLITE_UPDATE = ffi::SQLITE_UPDATE,
}

impl From<i32> for Action {
    #[inline]
    fn from(code: i32) -> Action {
        match code {
            ffi::SQLITE_DELETE => Action::SQLITE_DELETE,
            ffi::SQLITE_INSERT => Action::SQLITE_INSERT,
            ffi::SQLITE_UPDATE => Action::SQLITE_UPDATE,
            _ => Action::UNKNOWN,
        }
    }
}

/// The context received by an authorizer hook.
///
/// See <https://sqlite.org/c3ref/set_authorizer.html> for more info.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthContext<'c> {
    /// The action to be authorized.
    pub action: AuthAction<'c>,

    /// The database name, if applicable.
    pub database_name: Option<&'c str>,

    /// The inner-most trigger or view responsible for the access attempt.
    /// `None` if the access attempt was made by top-level SQL code.
    pub accessor: Option<&'c str>,
}

/// Actions and arguments found within a statement during
/// preparation.
///
/// See <https://sqlite.org/c3ref/c_alter_table.html> for more info.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum AuthAction<'c> {
    /// This variant is not normally produced by SQLite. You may encounter it
    // if you're using a different version than what's supported by this library.
    Unknown {
        /// The unknown authorization action code.
        code: i32,
        /// The third arg to the authorizer callback.
        arg1: Option<&'c str>,
        /// The fourth arg to the authorizer callback.
        arg2: Option<&'c str>,
    },
    CreateIndex {
        index_name: &'c str,
        table_name: &'c str,
    },
    CreateTable {
        table_name: &'c str,
    },
    CreateTempIndex {
        index_name: &'c str,
        table_name: &'c str,
    },
    CreateTempTable {
        table_name: &'c str,
    },
    CreateTempTrigger {
        trigger_name: &'c str,
        table_name: &'c str,
    },
    CreateTempView {
        view_name: &'c str,
    },
    CreateTrigger {
        trigger_name: &'c str,
        table_name: &'c str,
    },
    CreateView {
        view_name: &'c str,
    },
    Delete {
        table_name: &'c str,
    },
    DropIndex {
        index_name: &'c str,
        table_name: &'c str,
    },
    DropTable {
        table_name: &'c str,
    },
    DropTempIndex {
        index_name: &'c str,
        table_name: &'c str,
    },
    DropTempTable {
        table_name: &'c str,
    },
    DropTempTrigger {
        trigger_name: &'c str,
        table_name: &'c str,
    },
    DropTempView {
        view_name: &'c str,
    },
    DropTrigger {
        trigger_name: &'c str,
        table_name: &'c str,
    },
    DropView {
        view_name: &'c str,
    },
    Insert {
        table_name: &'c str,
    },
    Pragma {
        pragma_name: &'c str,
        /// The pragma value, if present (e.g., `PRAGMA name = value;`).
        pragma_value: Option<&'c str>,
    },
    Read {
        table_name: &'c str,
        column_name: &'c str,
    },
    Select,
    Transaction {
        operation: TransactionOperation,
    },
    Update {
        table_name: &'c str,
        column_name: &'c str,
    },
    Attach {
        filename: &'c str,
    },
    Detach {
        database_name: &'c str,
    },
    AlterTable {
        database_name: &'c str,
        table_name: &'c str,
    },
    Reindex {
        index_name: &'c str,
    },
    Analyze {
        table_name: &'c str,
    },
    CreateVtable {
        table_name: &'c str,
        module_name: &'c str,
    },
    DropVtable {
        table_name: &'c str,
        module_name: &'c str,
    },
    Function {
        function_name: &'c str,
    },
    Savepoint {
        operation: TransactionOperation,
        savepoint_name: &'c str,
    },
    Recursive,
}

impl<'c> AuthAction<'c> {
    fn from_raw(code: i32, arg1: Option<&'c str>, arg2: Option<&'c str>) -> Self {
        match (code, arg1, arg2) {
            (ffi::SQLITE_CREATE_INDEX, Some(index_name), Some(table_name)) => Self::CreateIndex {
                index_name,
                table_name,
            },
            (ffi::SQLITE_CREATE_TABLE, Some(table_name), _) => Self::CreateTable { table_name },
            (ffi::SQLITE_CREATE_TEMP_INDEX, Some(index_name), Some(table_name)) => {
                Self::CreateTempIndex {
                    index_name,
                    table_name,
                }
            }
            (ffi::SQLITE_CREATE_TEMP_TABLE, Some(table_name), _) => {
                Self::CreateTempTable { table_name }
            }
            (ffi::SQLITE_CREATE_TEMP_TRIGGER, Some(trigger_name), Some(table_name)) => {
                Self::CreateTempTrigger {
                    trigger_name,
                    table_name,
                }
            }
            (ffi::SQLITE_CREATE_TEMP_VIEW, Some(view_name), _) => {
                Self::CreateTempView { view_name }
            }
            (ffi::SQLITE_CREATE_TRIGGER, Some(trigger_name), Some(table_name)) => {
                Self::CreateTrigger {
                    trigger_name,
                    table_name,
                }
            }
            (ffi::SQLITE_CREATE_VIEW, Some(view_name), _) => Self::CreateView { view_name },
            (ffi::SQLITE_DELETE, Some(table_name), None) => Self::Delete { table_name },
            (ffi::SQLITE_DROP_INDEX, Some(index_name), Some(table_name)) => Self::DropIndex {
                index_name,
                table_name,
            },
            (ffi::SQLITE_DROP_TABLE, Some(table_name), _) => Self::DropTable { table_name },
            (ffi::SQLITE_DROP_TEMP_INDEX, Some(index_name), Some(table_name)) => {
                Self::DropTempIndex {
                    index_name,
                    table_name,
                }
            }
            (ffi::SQLITE_DROP_TEMP_TABLE, Some(table_name), _) => {
                Self::DropTempTable { table_name }
            }
            (ffi::SQLITE_DROP_TEMP_TRIGGER, Some(trigger_name), Some(table_name)) => {
                Self::DropTempTrigger {
                    trigger_name,
                    table_name,
                }
            }
            (ffi::SQLITE_DROP_TEMP_VIEW, Some(view_name), _) => Self::DropTempView { view_name },
            (ffi::SQLITE_DROP_TRIGGER, Some(trigger_name), Some(table_name)) => Self::DropTrigger {
                trigger_name,
                table_name,
            },
            (ffi::SQLITE_DROP_VIEW, Some(view_name), _) => Self::DropView { view_name },
            (ffi::SQLITE_INSERT, Some(table_name), _) => Self::Insert { table_name },
            (ffi::SQLITE_PRAGMA, Some(pragma_name), pragma_value) => Self::Pragma {
                pragma_name,
                pragma_value,
            },
            (ffi::SQLITE_READ, Some(table_name), Some(column_name)) => Self::Read {
                table_name,
                column_name,
            },
            (ffi::SQLITE_SELECT, ..) => Self::Select,
            (ffi::SQLITE_TRANSACTION, Some(operation_str), _) => Self::Transaction {
                operation: TransactionOperation::from_str(operation_str),
            },
            (ffi::SQLITE_UPDATE, Some(table_name), Some(column_name)) => Self::Update {
                table_name,
                column_name,
            },
            (ffi::SQLITE_ATTACH, Some(filename), _) => Self::Attach { filename },
            (ffi::SQLITE_DETACH, Some(database_name), _) => Self::Detach { database_name },
            (ffi::SQLITE_ALTER_TABLE, Some(database_name), Some(table_name)) => Self::AlterTable {
                database_name,
                table_name,
            },
            (ffi::SQLITE_REINDEX, Some(index_name), _) => Self::Reindex { index_name },
            (ffi::SQLITE_ANALYZE, Some(table_name), _) => Self::Analyze { table_name },
            (ffi::SQLITE_CREATE_VTABLE, Some(table_name), Some(module_name)) => {
                Self::CreateVtable {
                    table_name,
                    module_name,
                }
            }
            (ffi::SQLITE_DROP_VTABLE, Some(table_name), Some(module_name)) => Self::DropVtable {
                table_name,
                module_name,
            },
            (ffi::SQLITE_FUNCTION, _, Some(function_name)) => Self::Function { function_name },
            (ffi::SQLITE_SAVEPOINT, Some(operation_str), Some(savepoint_name)) => Self::Savepoint {
                operation: TransactionOperation::from_str(operation_str),
                savepoint_name,
            },
            (ffi::SQLITE_RECURSIVE, ..) => Self::Recursive,
            (code, arg1, arg2) => Self::Unknown { code, arg1, arg2 },
        }
    }
}

pub(crate) type BoxedAuthorizer =
    Box<dyn for<'c> FnMut(AuthContext<'c>) -> Authorization + Send + 'static>;

/// A transaction operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum TransactionOperation {
    Unknown,
    Begin,
    Release,
    Rollback,
}

impl TransactionOperation {
    fn from_str(op_str: &str) -> Self {
        match op_str {
            "BEGIN" => Self::Begin,
            "RELEASE" => Self::Release,
            "ROLLBACK" => Self::Rollback,
            _ => Self::Unknown,
        }
    }
}

/// [`authorizer`](Connection::authorizer) return code
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Authorization {
    /// Authorize the action.
    Allow,
    /// Don't allow access, but don't trigger an error either.
    Ignore,
    /// Trigger an error.
    Deny,
}

impl Authorization {
    fn into_raw(self) -> c_int {
        match self {
            Self::Allow => ffi::SQLITE_OK,
            Self::Ignore => ffi::SQLITE_IGNORE,
            Self::Deny => ffi::SQLITE_DENY,
        }
    }
}

impl Connection {
    /// Register a callback function to be invoked whenever
    /// a transaction is committed.
    ///
    /// The callback returns `true` to rollback.
    #[inline]
    pub fn commit_hook<F>(&self, hook: Option<F>)
    where
        F: FnMut() -> bool + Send + 'static,
    {
        self.db.borrow_mut().commit_hook(hook);
    }

    /// Register a callback function to be invoked whenever
    /// a transaction is committed.
    #[inline]
    pub fn rollback_hook<F>(&self, hook: Option<F>)
    where
        F: FnMut() + Send + 'static,
    {
        self.db.borrow_mut().rollback_hook(hook);
    }

    /// Register a callback function to be invoked whenever
    /// a row is updated, inserted or deleted in a rowid table.
    ///
    /// The callback parameters are:
    ///
    /// - the type of database update (`SQLITE_INSERT`, `SQLITE_UPDATE` or
    /// `SQLITE_DELETE`),
    /// - the name of the database ("main", "temp", ...),
    /// - the name of the table that is updated,
    /// - the ROWID of the row that is updated.
    #[inline]
    pub fn update_hook<F>(&self, hook: Option<F>)
    where
        F: FnMut(Action, &str, &str, i64) + Send + 'static,
    {
        self.db.borrow_mut().update_hook(hook);
    }

    /// Register a query progress callback.
    ///
    /// The parameter `num_ops` is the approximate number of virtual machine
    /// instructions that are evaluated between successive invocations of the
    /// `handler`. If `num_ops` is less than one then the progress handler
    /// is disabled.
    ///
    /// If the progress callback returns `true`, the operation is interrupted.
    pub fn progress_handler<F>(&self, num_ops: c_int, handler: Option<F>)
    where
        F: FnMut() -> bool + Send + RefUnwindSafe + 'static,
    {
        self.db.borrow_mut().progress_handler(num_ops, handler);
    }

    /// Register an authorizer callback that's invoked
    /// as a statement is being prepared.
    #[inline]
    pub fn authorizer<'c, F>(&self, hook: Option<F>)
    where
        F: for<'r> FnMut(AuthContext<'r>) -> Authorization + Send + RefUnwindSafe + 'static,
    {
        self.db.borrow_mut().authorizer(hook);
    }
}

impl InnerConnection {
    #[inline]
    pub fn remove_hooks(&mut self) {
        self.update_hook(None::<fn(Action, &str, &str, i64)>);
        self.commit_hook(None::<fn() -> bool>);
        self.rollback_hook(None::<fn()>);
        self.progress_handler(0, None::<fn() -> bool>);
        self.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    }

    fn commit_hook<F>(&mut self, hook: Option<F>)
    where
        F: FnMut() -> bool + Send + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<F>(p_arg: *mut c_void) -> c_int
        where
            F: FnMut() -> bool,
        {
            let r = catch_unwind(|| {
                let boxed_hook: *mut F = p_arg.cast::<F>();
                (*boxed_hook)()
            });
            c_int::from(r.unwrap_or_default())
        }

        // unlike `sqlite3_create_function_v2`, we cannot specify a `xDestroy` with
        // `sqlite3_commit_hook`. so we keep the `xDestroy` function in
        // `InnerConnection.free_boxed_hook`.
        let free_commit_hook = if hook.is_some() {
            Some(free_boxed_hook::<F> as unsafe fn(*mut c_void))
        } else {
            None
        };

        let previous_hook = match hook {
            Some(hook) => {
                let boxed_hook: *mut F = Box::into_raw(Box::new(hook));
                unsafe {
                    ffi::sqlite3_commit_hook(
                        self.db(),
                        Some(call_boxed_closure::<F>),
                        boxed_hook.cast(),
                    )
                }
            }
            _ => unsafe { ffi::sqlite3_commit_hook(self.db(), None, ptr::null_mut()) },
        };
        if !previous_hook.is_null() {
            if let Some(free_boxed_hook) = self.free_commit_hook {
                unsafe { free_boxed_hook(previous_hook) };
            }
        }
        self.free_commit_hook = free_commit_hook;
    }

    fn rollback_hook<F>(&mut self, hook: Option<F>)
    where
        F: FnMut() + Send + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<F>(p_arg: *mut c_void)
        where
            F: FnMut(),
        {
            drop(catch_unwind(|| {
                let boxed_hook: *mut F = p_arg.cast::<F>();
                (*boxed_hook)();
            }));
        }

        let free_rollback_hook = if hook.is_some() {
            Some(free_boxed_hook::<F> as unsafe fn(*mut c_void))
        } else {
            None
        };

        let previous_hook = match hook {
            Some(hook) => {
                let boxed_hook: *mut F = Box::into_raw(Box::new(hook));
                unsafe {
                    ffi::sqlite3_rollback_hook(
                        self.db(),
                        Some(call_boxed_closure::<F>),
                        boxed_hook.cast(),
                    )
                }
            }
            _ => unsafe { ffi::sqlite3_rollback_hook(self.db(), None, ptr::null_mut()) },
        };
        if !previous_hook.is_null() {
            if let Some(free_boxed_hook) = self.free_rollback_hook {
                unsafe { free_boxed_hook(previous_hook) };
            }
        }
        self.free_rollback_hook = free_rollback_hook;
    }

    fn update_hook<F>(&mut self, hook: Option<F>)
    where
        F: FnMut(Action, &str, &str, i64) + Send + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<F>(
            p_arg: *mut c_void,
            action_code: c_int,
            p_db_name: *const c_char,
            p_table_name: *const c_char,
            row_id: i64,
        ) where
            F: FnMut(Action, &str, &str, i64),
        {
            let action = Action::from(action_code);
            drop(catch_unwind(|| {
                let boxed_hook: *mut F = p_arg.cast::<F>();
                (*boxed_hook)(
                    action,
                    expect_utf8(p_db_name, "database name"),
                    expect_utf8(p_table_name, "table name"),
                    row_id,
                );
            }));
        }

        let free_update_hook = if hook.is_some() {
            Some(free_boxed_hook::<F> as unsafe fn(*mut c_void))
        } else {
            None
        };

        let previous_hook = match hook {
            Some(hook) => {
                let boxed_hook: *mut F = Box::into_raw(Box::new(hook));
                unsafe {
                    ffi::sqlite3_update_hook(
                        self.db(),
                        Some(call_boxed_closure::<F>),
                        boxed_hook.cast(),
                    )
                }
            }
            _ => unsafe { ffi::sqlite3_update_hook(self.db(), None, ptr::null_mut()) },
        };
        if !previous_hook.is_null() {
            if let Some(free_boxed_hook) = self.free_update_hook {
                unsafe { free_boxed_hook(previous_hook) };
            }
        }
        self.free_update_hook = free_update_hook;
    }

    fn progress_handler<F>(&mut self, num_ops: c_int, handler: Option<F>)
    where
        F: FnMut() -> bool + Send + RefUnwindSafe + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<F>(p_arg: *mut c_void) -> c_int
        where
            F: FnMut() -> bool,
        {
            let r = catch_unwind(|| {
                let boxed_handler: *mut F = p_arg.cast::<F>();
                (*boxed_handler)()
            });
            c_int::from(r.unwrap_or_default())
        }

        if let Some(handler) = handler {
            let boxed_handler = Box::new(handler);
            unsafe {
                ffi::sqlite3_progress_handler(
                    self.db(),
                    num_ops,
                    Some(call_boxed_closure::<F>),
                    &*boxed_handler as *const F as *mut _,
                );
            }
            self.progress_handler = Some(boxed_handler);
        } else {
            unsafe { ffi::sqlite3_progress_handler(self.db(), num_ops, None, ptr::null_mut()) }
            self.progress_handler = None;
        };
    }

    fn authorizer<'c, F>(&'c mut self, authorizer: Option<F>)
    where
        F: for<'r> FnMut(AuthContext<'r>) -> Authorization + Send + RefUnwindSafe + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<'c, F>(
            p_arg: *mut c_void,
            action_code: c_int,
            param1: *const c_char,
            param2: *const c_char,
            db_name: *const c_char,
            trigger_or_view_name: *const c_char,
        ) -> c_int
        where
            F: FnMut(AuthContext<'c>) -> Authorization + Send + 'static,
        {
            catch_unwind(|| {
                let action = AuthAction::from_raw(
                    action_code,
                    expect_optional_utf8(param1, "authorizer param 1"),
                    expect_optional_utf8(param2, "authorizer param 2"),
                );
                let auth_ctx = AuthContext {
                    action,
                    database_name: expect_optional_utf8(db_name, "database name"),
                    accessor: expect_optional_utf8(
                        trigger_or_view_name,
                        "accessor (inner-most trigger or view)",
                    ),
                };
                let boxed_hook: *mut F = p_arg.cast::<F>();
                (*boxed_hook)(auth_ctx)
            })
            .map_or_else(|_| ffi::SQLITE_ERROR, Authorization::into_raw)
        }

        let callback_fn = authorizer
            .as_ref()
            .map(|_| call_boxed_closure::<'c, F> as unsafe extern "C" fn(_, _, _, _, _, _) -> _);
        let boxed_authorizer = authorizer.map(Box::new);

        match unsafe {
            ffi::sqlite3_set_authorizer(
                self.db(),
                callback_fn,
                boxed_authorizer
                    .as_ref()
                    .map_or_else(ptr::null_mut, |f| &**f as *const F as *mut _),
            )
        } {
            ffi::SQLITE_OK => {
                self.authorizer = boxed_authorizer.map(|ba| ba as _);
            }
            err_code => {
                // The only error that `sqlite3_set_authorizer` returns is `SQLITE_MISUSE`
                // when compiled with `ENABLE_API_ARMOR` and the db pointer is invalid.
                // This library does not allow constructing a null db ptr, so if this branch
                // is hit, something very bad has happened. Panicking instead of returning
                // `Result` keeps this hook's API consistent with the others.
                panic!("unexpectedly failed to set_authorizer: {}", unsafe {
                    crate::error::error_from_handle(self.db(), err_code)
                });
            }
        }
    }
}

unsafe fn free_boxed_hook<F>(p: *mut c_void) {
    drop(Box::from_raw(p.cast::<F>()));
}

unsafe fn expect_utf8<'a>(p_str: *const c_char, description: &'static str) -> &'a str {
    expect_optional_utf8(p_str, description)
        .unwrap_or_else(|| panic!("received empty {}", description))
}

unsafe fn expect_optional_utf8<'a>(
    p_str: *const c_char,
    description: &'static str,
) -> Option<&'a str> {
    if p_str.is_null() {
        return None;
    }
    std::str::from_utf8(std::ffi::CStr::from_ptr(p_str).to_bytes())
        .unwrap_or_else(|_| panic!("received non-utf8 string as {}", description))
        .into()
}

#[cfg(test)]
mod test {
    use super::Action;
    use crate::{Connection, Result};
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn test_commit_hook() -> Result<()> {
        let db = Connection::open_in_memory()?;

        static CALLED: AtomicBool = AtomicBool::new(false);
        db.commit_hook(Some(|| {
            CALLED.store(true, Ordering::Relaxed);
            false
        }));
        db.execute_batch("BEGIN; CREATE TABLE foo (t TEXT); COMMIT;")?;
        assert!(CALLED.load(Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn test_fn_commit_hook() -> Result<()> {
        let db = Connection::open_in_memory()?;

        fn hook() -> bool {
            true
        }

        db.commit_hook(Some(hook));
        db.execute_batch("BEGIN; CREATE TABLE foo (t TEXT); COMMIT;")
            .unwrap_err();
        Ok(())
    }

    #[test]
    fn test_rollback_hook() -> Result<()> {
        let db = Connection::open_in_memory()?;

        static CALLED: AtomicBool = AtomicBool::new(false);
        db.rollback_hook(Some(|| {
            CALLED.store(true, Ordering::Relaxed);
        }));
        db.execute_batch("BEGIN; CREATE TABLE foo (t TEXT); ROLLBACK;")?;
        assert!(CALLED.load(Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn test_update_hook() -> Result<()> {
        let db = Connection::open_in_memory()?;

        static CALLED: AtomicBool = AtomicBool::new(false);
        db.update_hook(Some(|action, db: &str, tbl: &str, row_id| {
            assert_eq!(Action::SQLITE_INSERT, action);
            assert_eq!("main", db);
            assert_eq!("foo", tbl);
            assert_eq!(1, row_id);
            CALLED.store(true, Ordering::Relaxed);
        }));
        db.execute_batch("CREATE TABLE foo (t TEXT)")?;
        db.execute_batch("INSERT INTO foo VALUES ('lisa')")?;
        assert!(CALLED.load(Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn test_progress_handler() -> Result<()> {
        let db = Connection::open_in_memory()?;

        static CALLED: AtomicBool = AtomicBool::new(false);
        db.progress_handler(
            1,
            Some(|| {
                CALLED.store(true, Ordering::Relaxed);
                false
            }),
        );
        db.execute_batch("BEGIN; CREATE TABLE foo (t TEXT); COMMIT;")?;
        assert!(CALLED.load(Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn test_progress_handler_interrupt() -> Result<()> {
        let db = Connection::open_in_memory()?;

        fn handler() -> bool {
            true
        }

        db.progress_handler(1, Some(handler));
        db.execute_batch("BEGIN; CREATE TABLE foo (t TEXT); COMMIT;")
            .unwrap_err();
        Ok(())
    }

    #[test]
    fn test_authorizer() -> Result<()> {
        use super::{AuthAction, AuthContext, Authorization};

        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (public TEXT, private TEXT)")
            .unwrap();

        let authorizer = move |ctx: AuthContext<'_>| match ctx.action {
            AuthAction::Read { column_name, .. } if column_name == "private" => {
                Authorization::Ignore
            }
            AuthAction::DropTable { .. } => Authorization::Deny,
            AuthAction::Pragma { .. } => panic!("shouldn't be called"),
            _ => Authorization::Allow,
        };

        db.authorizer(Some(authorizer));
        db.execute_batch(
            "BEGIN TRANSACTION; INSERT INTO foo VALUES ('pub txt', 'priv txt'); COMMIT;",
        )
        .unwrap();
        db.query_row_and_then("SELECT * FROM foo", [], |row| -> Result<()> {
            assert_eq!(row.get::<_, String>("public")?, "pub txt");
            assert!(row.get::<_, Option<String>>("private")?.is_none());
            Ok(())
        })
        .unwrap();
        db.execute_batch("DROP TABLE foo").unwrap_err();

        db.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
        db.execute_batch("PRAGMA user_version=1").unwrap(); // Disallowed by first authorizer, but it's now removed.

        Ok(())
    }
}
