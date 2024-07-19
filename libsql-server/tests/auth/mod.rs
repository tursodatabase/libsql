//! Test hrana related functionalities
#![allow(deprecated)]

use futures::SinkExt as _;
use libsql::Database;
use libsql_server::{
    auth::{user_auth_strategies, Auth},
    config::UserApiConfig,
};
use tempfile::tempdir;
use tokio_stream::StreamExt;
use tokio_tungstenite::{
    client_async,
    tungstenite::{self, client::IntoClientRequest},
};
use turmoil::net::TcpStream;

use crate::common::net::{init_tracing, SimServer, TestServer, TurmoilConnector};

const TEST_JWT_KEY: &str = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MjE2NTIwNTB9.5XhUDHQhtShszTssjjUzVuJA3r-031mT4inVkvYEYz64sOCxnNpZUZdVF-CmZ4t-JTSXFlm8ddscBgkhccBxDg";

async fn make_standalone_server() -> Result<(), Box<dyn std::error::Error>> {
    let jwt_pem = include_bytes!("jwt_key.pem");
    let jwt_keys = vec![jsonwebtoken::DecodingKey::from_ed_pem(jwt_pem).unwrap()];

    init_tracing();
    let tmp = tempdir()?;
    let server = TestServer {
        path: tmp.path().to_owned().into(),
        user_api_config: UserApiConfig {
            hrana_ws_acceptor: None,
            auth_strategy: Auth::new(user_auth_strategies::Jwt::new(jwt_keys)),
            ..Default::default()
        },
        ..Default::default()
    };

    server.start_sim(8080).await?;

    Ok(())
}

#[test]
fn http_hrana() {
    let mut sim = turmoil::Builder::new().build();
    sim.host("primary", make_standalone_server);
    sim.client("client", async {
        let db = Database::open_remote_with_connector(
            "http://primary:8080",
            TEST_JWT_KEY,
            TurmoilConnector,
        )?;
        let conn = db.connect()?;

        conn.execute("create table t(x text)", ()).await?;

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn embedded_replica() {
    let tmp_embedded = tempdir().unwrap();
    let tmp_embedded_path = tmp_embedded.path().to_owned();

    let mut sim = turmoil::Builder::new().build();
    sim.host("primary", make_standalone_server);
    sim.client("client", async move {
        let path = tmp_embedded_path.join("embedded");

        let db = Database::open_with_remote_sync_connector(
            path.to_str().unwrap(),
            "http://primary:8080",
            TEST_JWT_KEY,
            TurmoilConnector,
            false,
            None,
        )
        .await?;

        let conn = db.connect()?;

        conn.execute("create table t(x text)", ()).await?;

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn ws_hrana() {
    let mut sim = turmoil::Builder::new().build();
    sim.host("primary", make_standalone_server);
    sim.client("client", async {
        let url = "ws://primary:8080";

        let req = url.into_client_request().unwrap();

        let conn = TcpStream::connect("primary:8080").await.unwrap();

        let (mut ws, _) = client_async(req, conn).await.unwrap();

        #[derive(serde::Serialize, Debug)]
        #[serde(tag = "type", rename_all = "snake_case")]
        pub enum ClientMsg {
            Hello { jwt: Option<String> },
        }

        #[derive(serde::Deserialize, Debug)]
        #[serde(tag = "type", rename_all = "snake_case")]
        pub enum ServerMsg {
            HelloOk {},
        }

        let msg = ClientMsg::Hello {
            jwt: Some(TEST_JWT_KEY.to_string()),
        };

        let msg_data = serde_json::to_string(&msg).unwrap();

        ws.send(tungstenite::Message::Text(msg_data)).await.unwrap();

        let Some(tungstenite::Message::Text(msg)) = ws.try_next().await.unwrap() else {
            panic!("wrong message type");
        };

        serde_json::from_str::<ServerMsg>(&msg).unwrap();

        Ok(())
    });

    sim.run().unwrap();
}
