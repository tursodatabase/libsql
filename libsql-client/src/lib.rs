use std::collections::HashMap;
use std::iter::IntoIterator;

use base64::Engine;
use worker::*;

/// Metadata of a request
#[derive(Clone, Debug, Default)]
pub struct Meta {
    pub duration: u64,
}

/// Value of a single database cell
#[derive(Clone, Debug)]
pub enum CellValue {
    Text(String),
    Float(f64),
    Number(i64),
    Bool(bool),
}

/// A database row
#[derive(Clone, Debug)]
pub struct Row {
    pub cells: HashMap<String, Option<CellValue>>,
}

/// Structure holding a set of rows returned from a query
/// and their corresponding column names
#[derive(Clone, Debug)]
pub struct Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

/// Result of a request - a set of rows or an error
#[derive(Clone, Debug)]
pub enum ResultSet {
    Error((String, Meta)),
    Success((Rows, Meta)),
}

/// Database connection. This is the main structure used to
/// communicate with the database.
#[derive(Clone, Debug)]
pub struct Connection {
    url: String,
    auth: String,
}

fn parse_columns(columns: Vec<serde_json::Value>, result_idx: usize) -> Result<Vec<String>> {
    let mut result = Vec::with_capacity(columns.len());
    for (idx, column) in columns.into_iter().enumerate() {
        match column {
            serde_json::Value::String(column) => result.push(column),
            _ => {
                return Err(worker::Error::from(format!(
                    "Result {result_idx} column name {idx} not a string",
                )))
            }
        }
    }
    Ok(result)
}

fn parse_value(
    cell: serde_json::Value,
    result_idx: usize,
    row_idx: usize,
    cell_idx: usize,
) -> Result<Option<CellValue>> {
    match cell {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Bool(v) => Ok(Some(CellValue::Bool(v))),
        serde_json::Value::Number(v) => match v.as_i64() {
            Some(v) => Ok(Some(CellValue::Number(v))),
            None => match v.as_f64() {
                Some(v) => Ok(Some(CellValue::Float(v))),
                None => Err(worker::Error::from(format!(
                    "Result {result_idx} row {row_idx} cell {cell_idx} had unknown number value: {v}",
                ))),
            },
        },
        serde_json::Value::String(v) => Ok(Some(CellValue::Text(v))),
        _ => Err(worker::Error::from(format!(
            "Result {result_idx} row {row_idx} cell {cell_idx} had unknown type",
        ))),
    }
}

fn parse_rows(
    rows: Vec<serde_json::Value>,
    columns: &Vec<String>,
    result_idx: usize,
) -> Result<Vec<Row>> {
    let mut result = Vec::with_capacity(rows.len());
    for (idx, row) in rows.into_iter().enumerate() {
        match row {
            serde_json::Value::Array(row) => {
                if row.len() != columns.len() {
                    return Err(worker::Error::from(format!(
                        "Result {result_idx} row {idx} had wrong number of cells",
                    )));
                }
                let mut cells = HashMap::with_capacity(columns.len());
                for (cell_idx, value) in row.into_iter().enumerate() {
                    cells.insert(
                        columns[cell_idx].clone(),
                        parse_value(value, result_idx, idx, cell_idx)?,
                    );
                }
                result.push(Row { cells })
            }
            _ => {
                return Err(worker::Error::from(format!(
                    "Result {result_idx} row {idx} was not an array",
                )))
            }
        }
    }
    Ok(result)
}

fn parse_result_set(result: serde_json::Value, idx: usize) -> Result<ResultSet> {
    match result {
        serde_json::Value::Object(obj) => {
            if let Some(err) = obj.get("error") {
                return match err {
                    serde_json::Value::Object(obj) => match obj.get("message") {
                        Some(serde_json::Value::String(msg)) => {
                            Ok(ResultSet::Error((msg.clone(), Meta::default())))
                        }
                        _ => Err(worker::Error::from(format!(
                            "Result {idx} error message was not a string",
                        ))),
                    },
                    _ => Err(worker::Error::from(format!(
                        "Result {idx} results was not an object",
                    ))),
                };
            }

            let results = obj.get("results");
            match results {
                Some(serde_json::Value::Object(obj)) => {
                    let columns = obj.get("columns").ok_or_else(|| {
                        worker::Error::from(format!("Result {idx} had no columns"))
                    })?;
                    let rows = obj
                        .get("rows")
                        .ok_or_else(|| worker::Error::from(format!("Result {idx} had no rows")))?;
                    match (rows, columns) {
                        (serde_json::Value::Array(rows), serde_json::Value::Array(columns)) => {
                            let columns = parse_columns(columns.to_vec(), idx)?;
                            let rows = parse_rows(rows.to_vec(), &columns, idx)?;
                            Ok(ResultSet::Success((
                                Rows { columns, rows },
                                Meta::default(),
                            )))
                        }
                        _ => Err(worker::Error::from(format!(
                            "Result {idx} had rows or columns that were not an array",
                        ))),
                    }
                }
                Some(_) => Err(worker::Error::from(format!(
                    "Result {idx} was not an object",
                ))),
                None => Err(worker::Error::from(format!(
                    "Result {idx} did not contain results or error",
                ))),
            }
        }
        _ => Err(worker::Error::from(format!(
            "Result {idx} was not an object",
        ))),
    }
}

impl Connection {
    /// Establishes a database connection.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `username` - database username
    /// * `pass` - user's password
    pub fn connect(
        url: impl Into<String>,
        username: impl Into<String>,
        pass: impl Into<String>,
    ) -> Self {
        let username = username.into();
        let pass = pass.into();
        Self {
            url: url.into(),
            auth: format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{pass}"))
            ),
        }
    }

    /// Establishes a database connection from Cloudflare Workers context.
    /// Expects the context to contain the following variables defined:
    /// * `LIBSQL_CLIENT_URL`
    /// * `LIBSQL_CLIENT_USER`
    /// * `LIBSQL_CLIENT_PASS`
    /// # Arguments
    /// * `ctx` - Cloudflare Workers route context
    pub fn connect_from_ctx<D>(ctx: &worker::RouteContext<D>) -> Result<Self> {
        Ok(Self::connect(
            ctx.var("LIBSQL_CLIENT_URL")?.to_string(),
            ctx.var("LIBSQL_CLIENT_USER")?.to_string(),
            ctx.var("LIBSQL_CLIENT_PASS")?.to_string(),
        ))
    }

    /// Executes a single SQL statement
    ///
    /// # Arguments
    /// * `stmt` - the SQL statement
    pub async fn execute(&self, stmt: impl Into<String>) -> Result<ResultSet> {
        let mut results = self.batch(std::iter::once(stmt)).await?;
        Ok(results.remove(0))
    }

    /// Executes a batch of SQL statements.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    pub async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Vec<ResultSet>> {
        let mut headers = Headers::new();
        headers.append("Authorization", &self.auth).ok();
        let stmts: Vec<String> = stmts
            .into_iter()
            .map(|s| format!("\"{}\"", s.into()))
            .collect();
        let request_init = RequestInit {
            body: Some(wasm_bindgen::JsValue::from_str(&format!(
                "{{\"statements\": [{}]}}",
                stmts.join(",")
            ))),
            headers,
            cf: CfProperties::new(),
            method: Method::Post,
            redirect: RequestRedirect::Follow,
        };
        let req = Request::new_with_init(&self.url, &request_init)?;
        let response = Fetch::Request(req).send().await;
        let resp: String = response?.text().await?;
        let response_json: serde_json::Value = serde_json::from_str(&resp)?;
        match response_json {
            serde_json::Value::Array(results) => {
                if results.len() != stmts.len() {
                    Err(worker::Error::from(format!(
                        "Response array did not contain expected {} results",
                        stmts.len()
                    )))
                } else {
                    let mut result_sets: Vec<ResultSet> = Vec::with_capacity(stmts.len());
                    for (idx, result) in results.into_iter().enumerate() {
                        result_sets.push(parse_result_set(result, idx)?);
                    }

                    Ok(result_sets)
                }
            }
            e => Err(worker::Error::from(format!(
                "Error: {} ({:?})",
                e, request_init.body
            ))),
        }
    }

    /// Executes an SQL transaction.
    /// Does not support nested transactions - do not use BEGIN or END
    /// inside a transaction.
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    pub async fn transaction(
        &self,
        stmts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Vec<ResultSet>> {
        // TODO: Vec is not a good fit for popping the first element,
        // let's return a templated collection instead and let the user
        // decide where to store the result.
        let mut ret: Vec<ResultSet> = self
            .batch(
                std::iter::once("BEGIN".to_string())
                    .chain(stmts.into_iter().map(|s| s.into()))
                    .chain(std::iter::once("END".to_string())),
            )
            .await?
            .into_iter()
            .skip(1)
            .collect();
        ret.pop();
        Ok(ret)
    }
}
