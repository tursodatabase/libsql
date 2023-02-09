//! `Statement` represents an SQL statement,
//! which can be later sent to a database.

use super::Value;

/// SQL statement, possibly with bound parameters
pub struct Statement {
    q: String,
    params: Vec<Value>,
}

impl Statement {
    /// Creates a new simple statement without bound parameters
    ///
    /// # Examples
    ///
    /// ```
    /// let stmt = libsql_client::Statement::new("SELECT * FROM sqlite_master");
    /// ```
    pub fn new(q: impl Into<String>) -> Statement {
        Self {
            q: q.into(),
            params: vec![],
        }
    }

    /// Creates a statement with bound parameters
    ///
    /// # Examples
    ///
    /// ```
    /// let stmt = libsql_client::Statement::with_params("UPDATE t SET x = ? WHERE key = ?", &[3, 8]);
    /// ```
    pub fn with_params(q: impl Into<String>, params: &[impl Into<Value> + Clone]) -> Statement {
        Self {
            q: q.into(),
            params: params.iter().map(|p| p.clone().into()).collect(),
        }
    }
}

impl From<String> for Statement {
    fn from(q: String) -> Statement {
        Statement { q, params: vec![] }
    }
}

impl From<&str> for Statement {
    fn from(val: &str) -> Self {
        val.to_string().into()
    }
}

impl From<&&str> for Statement {
    fn from(val: &&str) -> Self {
        val.to_string().into()
    }
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.params.is_empty() {
            write!(f, "\"{}\"", self.q)
        } else {
            let params: Vec<String> = self.params.iter().map(|p| p.to_string()).collect();
            write!(
                f,
                "{{\"q\": \"{}\", \"params\": [{}]}}",
                self.q,
                params.join(",")
            )
        }
    }
}
