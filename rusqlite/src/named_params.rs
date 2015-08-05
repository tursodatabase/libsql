//use std::collections::HashMap;
use std::ffi::{CString};
use libc::{c_int};

use super::ffi;

use {SqliteResult, SqliteError, SqliteConnection, SqliteStatement, SqliteRows};
use types::{ToSql};

impl SqliteConnection {
}

impl<'conn> SqliteStatement<'conn> {
    /*pub fn parameter_names(&self) -> HashMap<String, i32> {
        let n = unsafe { ffi::sqlite3_bind_parameter_count(self.stmt) };
        let mut index_by_name = HashMap::with_capacity(n as usize);
        for i in 1..n+1 {
            let c_name = unsafe { ffi::sqlite3_bind_parameter_name(self.stmt, i) };
            if !c_name.is_null() {
                let c_slice = unsafe { CStr::from_ptr(c_name).to_bytes() };
                index_by_name.insert(str::from_utf8(c_slice).unwrap().to_string(), n);
            }
        }
        index_by_name
    }*/

    /// Return the index of an SQL parameter given its name.
    /// Return None if `name` is invalid (NulError) or if no matching parameter is found.
    pub fn parameter_index(&self, name: &str) -> Option<i32> {
        unsafe {
            CString::new(name).ok().and_then(|c_name|
                match ffi::sqlite3_bind_parameter_index(self.stmt, c_name.as_ptr()) {
                    0 => None, // A zero is returned if no matching parameter is found.
                    n => Some(n)
                }
            )

        }
    }

    /// Execute the prepared statement with named parameter(s).
    ///
    /// On success, returns the number of rows that were changed or inserted or deleted (via
    /// `sqlite3_changes`).
    pub fn named_execute(&mut self, params: &[(&str, &ToSql)]) -> SqliteResult<c_int> {
        unsafe {
            try!(self.bind_named_parameters(params));
            self.execute_()
        }
    }

    /// Execute the prepared statement with named parameter(s), returning an iterator over the resulting rows.
    pub fn named_query<'a>(&'a mut self, params: &[(&str, &ToSql)]) -> SqliteResult<SqliteRows<'a>> {
        self.reset_if_needed();

        unsafe {
            try!(self.bind_named_parameters(params));
        }

        self.needs_reset = true;
        Ok(SqliteRows::new(self))
    }

    unsafe fn bind_named_parameters(&mut self, params: &[(&str, &ToSql)]) -> SqliteResult<()> {
        for &(name, value) in params {
            let i = try!(self.parameter_index(name).ok_or(SqliteError{
                code: ffi::SQLITE_MISUSE,
                message: format!("Invalid parameter name: {}", name)
            }));
            try!(self.conn.decode_result(value.bind_parameter(self.stmt, i)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use SqliteConnection;

   #[test]
    fn test_named_execute() {
        let db = SqliteConnection::open_in_memory().unwrap();
        let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER)";
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("INSERT INTO test (id, name, flag) VALUES (:id, :name, :flag)").unwrap();
        stmt.named_execute(&[(":name", &"one")]).unwrap();
    }

   #[test]
    fn test_named_query() {
        let db = SqliteConnection::open_in_memory().unwrap();
        let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER)";
        db.execute_batch(sql).unwrap();

        let mut stmt = db.prepare("SELECT * FROM test where name = :name").unwrap();
        stmt.named_query(&[(":name", &"one")]).unwrap();
    }
}