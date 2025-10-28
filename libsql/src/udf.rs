use std::sync::Arc;

use crate::Value;

/// A Rust callback implementing a user-defined scalar SQL function.
pub type ScalarFunctionCallback = Arc<dyn Fn(Vec<Value>) -> anyhow::Result<Value>>;

/// A scalar user-defined SQL function definition.
pub struct ScalarFunctionDef {
    /// The name of the SQL function to be created or redefined. The length of the name is limited
    /// to 255 bytes. Note that the name length limit is in UTF-8 bytes, not characters. Any attempt
    /// to create a function with a longer name will result in a SQLite misuse error.
    pub name: String,
    /// The number of arguments that the SQL function or aggregate takes. If this parameter is -1,
    /// then the SQL function or aggregate may take any number of arguments between 0 and the limit
    /// set by sqlite3_limit(SQLITE_LIMIT_FUNCTION_ARG). If the third parameter is less than -1 or
    /// greater than 127 then the behavior is undefined.
    pub num_args: i32,
    /// Set to true to signal that the function will always return the same result given the same
    /// inputs within a single SQL statement. Most SQL functions are deterministic. The built-in
    /// random() SQL function is an example of a function that is not deterministic. The SQLite query
    /// planner is able to perform additional optimizations on deterministic functions, so use of the
    /// deterministic flag is recommended where possible.
    pub deterministic: bool,
    /// The `innocuous` flag means that the function is unlikely to cause problems even if misused.
    /// An innocuous function should have no side effects and should not depend on any values other
    /// than its input parameters. The `abs()` function is an example of an innocuous function. The
    /// load_extension() SQL function is not innocuous because of its side effects.
    /// 
    /// `innocuous` is similar to `deterministic`, but is not exactly the same. The random()
    /// function is an example of a function that is innocuous but not deterministic.
    /// 
    /// Some heightened security settings (SQLITE_DBCONFIG_TRUSTED_SCHEMA and PRAGMA
    /// trusted_schema=OFF) disable the use of SQL functions inside views and triggers and in schema
    /// structures such as CHECK constraints, DEFAULT clauses, expression indexes, partial indexes,
    /// and generated columns unless the function is tagged with `innocuous`. Most built-in
    /// functions are innocuous. Developers are advised to avoid using the `innocuous` flag for
    /// application-defined functions unless the function has been carefully audited and found to be
    /// free of potentially security-adverse side-effects and information-leaks.
    pub innocuous: bool,
    /// When set, prevents the function from being invoked from within VIEWs, TRIGGERs, CHECK
    /// constraints, generated column expressions, index expressions, or the WHERE clause of partial
    /// indexes.
    /// 
    /// For best security, the `direct_only` flag is recommended for all application-defined SQL
    /// functions that do not need to be used inside of triggers, views, CHECK constraints, or other
    /// elements of the database schema. This flag is especially recommended for SQL functions that
    /// have side effects or reveal internal application state. Without this flag, an attacker might
    /// be able to modify the schema of a database file to include invocations of the function with
    /// parameters chosen by the attacker, which the application will then execute when the database
    /// file is opened and read.
    pub direct_only: bool,
    /// The Rust callback that will be called to implement the function.
    pub callback: ScalarFunctionCallback,
}
