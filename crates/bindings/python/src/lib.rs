use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

#[pyfunction]
fn connect(url: String) -> PyResult<Connection> {
    let db = libsql_core::Database::open(url);
    let conn = libsql_core::Connection::connect(db.url.clone());
    Ok(Connection { db, conn })
}

#[pyclass]
pub struct Connection {
    db: libsql_core::Database,
    conn: libsql_core::Connection,
}

#[pymethods]
impl Connection {
    fn cursor(self_: PyRef<'_, Self>) -> PyResult<Cursor> {
      Ok(Cursor{})
    }
}

#[pyclass]
pub struct Cursor {
}

#[pymethods]
impl Cursor {
    fn execute(self_: PyRef<'_, Self>, sql: String) -> PyResult<Result> {
      Ok(Result{})
    }
}

#[pyclass]
pub struct Result {
}

#[pymethods]
impl Result {
    fn fetchone(self_: PyRef<'_, Self>) -> PyResult<()> {
      Ok(())
    }
}

#[pymodule]
fn libsql(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<Connection>()?;
    m.add_class::<Cursor>()?;
    m.add_class::<Result>()?;
    Ok(())
}
