use pyo3::prelude::*;

#[pyfunction]
fn connect(url: String) -> PyResult<Connection> {
    Ok(Connection{})
}

#[pyclass]
pub struct Connection {
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
