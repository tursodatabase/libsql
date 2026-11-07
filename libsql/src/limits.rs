/// Run-time limits that can be read or changed per connection with
/// [`Connection::limit`](crate::Connection::limit) and
/// [`Connection::set_limit`](crate::Connection::set_limit).
///
/// The values match SQLite's `SQLITE_LIMIT_*` constants. See
/// <https://sqlite.org/c3ref/c_limit_attached.html> for details.
#[repr(i32)]
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Limit {
    /// The maximum size of any string or BLOB or table row, in bytes.
    Length = 0,
    /// The maximum length of an SQL statement, in bytes.
    SqlLength = 1,
    /// The maximum number of columns in a table definition or in the result set
    /// of a SELECT or the maximum number of columns in an index or in an
    /// ORDER BY or GROUP BY clause.
    Column = 2,
    /// The maximum depth of the parse tree on any expression.
    ExprDepth = 3,
    /// The maximum number of terms in the compound SELECT statement.
    CompoundSelect = 4,
    /// The maximum number of instructions in a virtual machine program used to
    /// implement an SQL statement.
    VdbeOp = 5,
    /// The maximum number of arguments on a function.
    FunctionArg = 6,
    /// The maximum number of attached databases.
    Attached = 7,
    /// The maximum length of the pattern argument to the LIKE or GLOB
    /// operators.
    LikePatternLength = 8,
    /// The maximum index number of any parameter in an SQL statement.
    VariableNumber = 9,
    /// The maximum depth of recursion for triggers.
    TriggerDepth = 10,
    /// The maximum number of auxiliary worker threads that a single prepared
    /// statement may start.
    WorkerThreads = 11,
}

#[cfg(all(test, feature = "core"))]
mod tests {
    use super::Limit;
    use libsql_sys::ffi;

    #[test]
    fn limit_values_match_sqlite() {
        let pairs = [
            (Limit::Length, ffi::SQLITE_LIMIT_LENGTH),
            (Limit::SqlLength, ffi::SQLITE_LIMIT_SQL_LENGTH),
            (Limit::Column, ffi::SQLITE_LIMIT_COLUMN),
            (Limit::ExprDepth, ffi::SQLITE_LIMIT_EXPR_DEPTH),
            (Limit::CompoundSelect, ffi::SQLITE_LIMIT_COMPOUND_SELECT),
            (Limit::VdbeOp, ffi::SQLITE_LIMIT_VDBE_OP),
            (Limit::FunctionArg, ffi::SQLITE_LIMIT_FUNCTION_ARG),
            (Limit::Attached, ffi::SQLITE_LIMIT_ATTACHED),
            (
                Limit::LikePatternLength,
                ffi::SQLITE_LIMIT_LIKE_PATTERN_LENGTH,
            ),
            (Limit::VariableNumber, ffi::SQLITE_LIMIT_VARIABLE_NUMBER),
            (Limit::TriggerDepth, ffi::SQLITE_LIMIT_TRIGGER_DEPTH),
            (Limit::WorkerThreads, ffi::SQLITE_LIMIT_WORKER_THREADS),
        ];
        for (limit, expected) in pairs {
            assert_eq!(limit as i32, expected as i32, "{limit:?}");
        }
    }
}
