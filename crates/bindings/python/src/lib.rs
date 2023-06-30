use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn to_py_err(error: libsql_core::errors::Error) -> PyErr {
    PyValueError::new_err(format!("{}", error))
}

#[pyfunction]
fn connect(url: String) -> PyResult<Connection> {
    let db = libsql_core::Database::open(url);
    let conn = db.connect().map_err(to_py_err)?;
    Ok(Connection {
        _db: db,
        _conn: conn,
    })
}

#[pyclass]
pub struct Connection {
    _db: libsql_core::Database,
    _conn: libsql_core::Connection,
}

#[pymethods]
impl Connection {
    fn cursor(_self: PyRef<'_, Self>) -> PyResult<Cursor> {
        Ok(Cursor {})
    }
}

#[pyclass]
pub struct Cursor {}

#[pymethods]
impl Cursor {
    fn execute(_self: PyRef<'_, Self>, _sql: String) -> PyResult<Result> {
        Ok(Result {})
    }
}

#[pyclass]
pub struct Result {}

#[pymethods]
impl Result {
    fn fetchone(_self: PyRef<'_, Self>) -> PyResult<()> {
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
