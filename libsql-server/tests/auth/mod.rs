//! Test hrana related functionalities
#![allow(deprecated)]

use futures::SinkExt as _;
use jsonwebtoken::{DecodingKey, EncodingKey};
use libsql::Database;
use libsql_server::{
    auth::{
        user_auth_strategies::{self, jwt::Token},
        Auth,
    },
    config::UserApiConfig,
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use tempfile::tempdir;
use tokio_stream::StreamExt;
use tokio_tungstenite::{
    client_async,
    tungstenite::{self, client::IntoClientRequest},
};
use turmoil::net::TcpStream;

use crate::common::net::{init_tracing, SimServer, TestServer, TurmoilConnector};

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
    let doc = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
    let encoding_key = EncodingKey::from_ed_der(doc.as_ref());

    let pair = Ed25519KeyPair::from_pkcs8(doc.as_ref()).unwrap();
    let decoding_key = DecodingKey::from_ed_der(pair.public_key().as_ref());

    let claims = Token::default();

    let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
    let token = jsonwebtoken::encode(&header, &claims, &encoding_key).unwrap();

    let jwt_keys = vec![decoding_key];

    let auth = Auth::new(user_auth_strategies::Jwt::new(jwt_keys));

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
