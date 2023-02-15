use async_trait::async_trait;
use base64::Engine;

use super::{QueryResult, Statement};

/// Database connection. This is the main structure used to
/// communicate with the database.
#[derive(Clone, Debug)]
pub struct Connection {
    base_url: String,
    url_for_queries: String,
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
        let base_url = if !url.contains("://") {
            "https://".to_owned() + &url
        } else {
            url
        };
        let url_for_queries = format!("{base_url}/queries");
        Self {
            base_url,
            url_for_queries,
            auth: format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{pass}"))
            ),
        }
    }

    /// Establishes a database connection, given a `Url`
    ///
    /// # Arguments
    /// * `url` - `Url` object of the database endpoint. This cannot be a relative URL;
    ///
    /// # Examples
    ///
    /// ```
    /// # use libsql_client::reqwest::Connection;
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

    pub fn connect_from_env() -> anyhow::Result<Connection> {
        let url = std::env::var("LIBSQL_CLIENT_URL").map_err(|_| {
            anyhow::anyhow!("LIBSQL_CLIENT_URL variable should point to your sqld database")
        })?;
        let user = match std::env::var("LIBSQL_CLIENT_USER") {
            Ok(user) => user,
            Err(_) => {
                return Ok(Connection::connect_from_url(&url::Url::parse(&url)?)?);
            }
        };
        let pass = std::env::var("LIBSQL_CLIENT_PASS").map_err(|_| {
            anyhow::anyhow!("LIBSQL_CLIENT_PASS variable should be set to your sqld password")
        })?;
        Ok(Connection::connect(url, user, pass))
    }
}

#[async_trait(?Send)]
impl super::Connection for Connection {
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        let (body, stmts_count) = crate::connection::statements_to_string(stmts);
        let client = reqwest::Client::new();
        let response = match client
            .post(&self.url_for_queries)
            .body(body.clone())
            .header("Authorization", &self.auth)
            .send()
            .await
        {
            Ok(resp) if resp.status() == reqwest::StatusCode::OK => resp,
            // Retry with the legacy route: "/"
            _ => {
                client
                    .post(&self.base_url)
                    .body(body)
                    .header("Authorization", &self.auth)
                    .send()
                    .await?
            }
        };
        let resp: String = response.text().await?;
        let response_json: serde_json::Value = serde_json::from_str(&resp)?;
        crate::connection::json_to_query_result(response_json, stmts_count)
    }
}
