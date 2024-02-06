//! Test hrana related functionalities
#![allow(deprecated)]

use libsql_server::config::UserApiConfig;
use tempfile::tempdir;

use crate::common::net::{init_tracing, SimServer, TestServer};
mod batch;
mod transaction;

async fn make_standalone_server() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let tmp = tempdir()?;
    let server = TestServer {
        path: tmp.path().to_owned().into(),
        user_api_config: UserApiConfig {
            hrana_ws_acceptor: None,
            ..Default::default()
        },
        ..Default::default()
    };

    server.start_sim(8080).await?;

    Ok(())
}
