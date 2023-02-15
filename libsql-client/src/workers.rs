use async_trait::async_trait;
use base64::Engine;
use worker::*;

use super::{QueryResult, Statement};

/// Database connection. This is the main structure used to
/// communicate with the database.
#[derive(Clone, Debug)]
pub struct Connection {
    url: String,
    auth: String,
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
        let url = url.into();
        // Auto-update the URL to start with https:// if no protocol was specified
        let url = if !url.contains("://") {
            "https://".to_owned() + &url
        } else {
            url
        };
        Self {
            url,
            auth: format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{pass}"))
            ),
        }
    }

    /// Establishes a database connection, given a [`Url`]
    ///
    /// # Arguments
    /// * `url` - [`Url`] object of the database endpoint. This cannot be a relative URL;
    ///
    /// # Examples
    ///
    /// ```
    /// # use libsql_client::Connection;
    /// use url::Url;
    ///
    /// let url  = Url::parse("https://foo:bar@localhost:8080").unwrap();
    /// let db = Connection::connect_from_url(&url).unwrap();
    /// ```
    pub fn connect_from_url(url: &url::Url) -> anyhow::Result<Connection> {
        let username = url.username();
        let password = url.password().unwrap_or_default();
        let mut url = url.clone();
        url.set_username("")
            .map_err(|_| anyhow::anyhow!("Could not extract username from URL. Invalid URL?"))?;
        url.set_password(None)
            .map_err(|_| anyhow::anyhow!("Could not extract password from URL. Invalid URL?"))?;
        Ok(Connection::connect(url.as_str(), username, password))
    }

    /// Establishes a database connection from Cloudflare Workers context.
    /// Expects the context to contain the following secrets defined:
    /// * `LIBSQL_CLIENT_URL`
    /// * `LIBSQL_CLIENT_USER`
    /// * `LIBSQL_CLIENT_PASS`
    /// # Arguments
    /// * `ctx` - Cloudflare Workers route context
    pub fn connect_from_ctx<D>(ctx: &worker::RouteContext<D>) -> anyhow::Result<Self> {
        Ok(Self::connect(
            ctx.secret("LIBSQL_CLIENT_URL")
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .to_string(),
            ctx.secret("LIBSQL_CLIENT_USER")
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .to_string(),
            ctx.secret("LIBSQL_CLIENT_PASS")
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .to_string(),
        ))
    }

    /// Executes a batch of SQL statements.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn f() {
    /// let db = libsql_client::Connection::connect("https://example.com", "admin", "s3cr3tp4ss");
    /// let result = db
    ///     .batch(["CREATE TABLE t(id)", "INSERT INTO t VALUES (42)"])
    ///     .await;
    /// # }
    /// ```
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<Vec<QueryResult>> {
        let mut headers = Headers::new();
        headers.append("Authorization", &self.auth).ok();
        let (body, stmts_count) = crate::connection::statements_to_string(stmts);
        let request_init = RequestInit {
            body: Some(wasm_bindgen::JsValue::from_str(&body)),
            headers,
            cf: CfProperties::new(),
            method: Method::Post,
            redirect: RequestRedirect::Follow,
        };
        let req = Request::new_with_init(&self.url, &request_init)?;
        let response = Fetch::Request(req).send().await;
        let resp: String = response?.text().await?;
        let response_json: serde_json::Value = serde_json::from_str(&resp)?;
        super::connection::json_to_query_result(response_json, stmts_count)
            .map_err(|e| worker::Error::from(format!("Error: {} ({:?})", e, request_init.body)))
    }
}

#[async_trait(?Send)]
impl super::Connection for Connection {
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        self.batch(stmts).await.map_err(|e| anyhow::anyhow!("{e}"))
    }
}
