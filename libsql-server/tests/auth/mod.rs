//! Test hrana related functionalities
#![allow(deprecated)]

use futures::SinkExt as _;
use libsql::Database;
use libsql_server::{
    auth::{
        user_auth_strategies::{self, jwt::Token},
        Auth,
    },
    config::UserApiConfig,
};
use tempfile::tempdir;
use tokio_stream::StreamExt;
use tokio_tungstenite::{
    client_async,
    tungstenite::{self, client::IntoClientRequest},
};
use turmoil::net::TcpStream;

use crate::common::{
    auth::{encode, key_pair},
    net::{init_tracing, SimServer, TestServer, TurmoilConnector},
};

async fn make_standalone_server(auth_strategy: Auth) -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let tmp = tempdir()?;
    let server = TestServer {
        path: tmp.path().to_owned().into(),
        user_api_config: UserApiConfig {
            hrana_ws_acceptor: None,
            auth_strategy,
            ..Default::default()
        },
        ..Default::default()
    };

    server.start_sim(8080).await?;

    Ok(())
}

fn gen_test_jwt_auth() -> (Auth, String) {
    let (encoding_key, decoding_key) = key_pair();
    let jwt_keys = vec![jsonwebtoken::DecodingKey::from_ed_components(&decoding_key).unwrap()];

    let auth = Auth::new(user_auth_strategies::Jwt::new(jwt_keys));

    let claims = Token::default();

    let token = encode(&claims, &encoding_key);

    (auth, token)
}

#[test]
fn http_hrana() {
    let (auth, token) = gen_test_jwt_auth();

    let mut sim = turmoil::Builder::new().build();
    sim.host("primary", move || make_standalone_server(auth.clone()));
    sim.client("client", async {
        let db =
            Database::open_remote_with_connector("http://primary:8080", token, TurmoilConnector)?;
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

    let (auth, token) = gen_test_jwt_auth();

    let mut sim = turmoil::Builder::new().build();
    sim.host("primary", move || make_standalone_server(auth.clone()));

    sim.client("client", async move {
        let path = tmp_embedded_path.join("embedded");

        let db = Database::open_with_remote_sync_connector(
            path.to_str().unwrap(),
            "http://primary:8080",
            token,
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
    let (auth, token) = gen_test_jwt_auth();

    let mut sim = turmoil::Builder::new().build();
    sim.host("primary", move || make_standalone_server(auth.clone()));

    sim.client("client", async move {
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
            jwt: Some(token.to_string()),
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
