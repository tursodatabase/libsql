use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

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
    fn execute(
        self_: PyRef<'_, Self>,
        sql: String,
        parameters: Option<&PyTuple>,
    ) -> PyResult<Result> {
        let params: libsql_core::Params = match parameters {
            Some(parameters) => {
                let mut params = vec![];
                for parameter in parameters.iter() {
                    let param = match parameter.extract::<i32>() {
                        Ok(value) => libsql_core::Value::Integer(value as i64),
                        Err(_) => match parameter.extract::<f64>() {
                            Ok(value) => libsql_core::Value::Float(value),
                            Err(_) => match parameter.extract::<&str>() {
                                Ok(value) => libsql_core::Value::Text(value.to_string()),
                                Err(_) => todo!(),
                            },
                        },
                    };
                    params.push(param);
                }
                libsql_core::Params::Positional(params)
            }
            None => libsql_core::Params::None,
        };
        let rows = self_.conn.execute(sql, params).map_err(to_py_err)?;
        Ok(Result { rows })
    }
}

#[pyclass]
pub struct Result {
    rows: Option<libsql_core::Rows>,
}

#[pymethods]
impl Result {
    fn fetchone(self_: PyRef<'_, Self>) -> PyResult<Option<&PyTuple>> {
        match self_.rows {
            Some(ref rows) => {
                let row = rows.next().map_err(to_py_err)?;
                match row {
                    Some(row) => {
                        let mut elements: Vec<Py<PyAny>> = vec![];
                        for col_idx in 0..rows.column_count() {
                            let col_type = row.column_type(col_idx).map_err(to_py_err)?;
                            let value = match col_type {
                                libsql_core::rows::ValueType::Integer => {
                                    let value = row.get::<i32>(col_idx).map_err(to_py_err)?;
                                    value.into_py(self_.py())
                                }
                                libsql_core::rows::ValueType::Float => todo!(),
                                libsql_core::rows::ValueType::Blob => todo!(),
                                libsql_core::rows::ValueType::Text => {
                                    let value = row.get::<&str>(col_idx).map_err(to_py_err)?;
                                    value.into_py(self_.py())
                                }
                                libsql_core::rows::ValueType::Null => todo!(),
                            };
                            elements.push(value);
                        }
                        Ok(Some(PyTuple::new(self_.py(), elements)))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
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
