use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn to_py_err(error: libsql_core::errors::Error) -> PyErr {
    PyValueError::new_err(format!("{}", error))
}

#[pyfunction]
fn connect(url: String) -> PyResult<Connection> {
    let db = libsql_core::Database::open(url);
    Ok(Connection { db })
}

#[pyclass]
pub struct Connection {
    db: libsql_core::Database,
}

#[pymethods]
impl Connection {
    fn sync(self_: PyRef<'_, Self>) -> PyResult<()> {
        self_.db.sync().map_err(to_py_err)?;
        Ok(())
    }

    fn cursor(self_: PyRef<'_, Self>) -> PyResult<Cursor> {
        let conn = self_.db.connect().map_err(to_py_err)?;
        Ok(Cursor { conn })
    }
}

#[pyclass]
pub struct Cursor {
    conn: libsql_core::Connection,
}

#[pymethods]
impl Cursor {
    fn execute(self_: PyRef<'_, Self>, _sql: String) -> PyResult<Result> {
        self_.conn.execute(_sql).map_err(to_py_err)?;
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
