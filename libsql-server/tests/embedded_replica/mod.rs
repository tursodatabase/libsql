use std::path::PathBuf;

use crate::common::http::Client;
use crate::common::net::{init_tracing, TestServer, TurmoilAcceptor, TurmoilConnector};
use libsql::Database;
use serde_json::json;
use sqld::config::{AdminApiConfig, RpcServerConfig, UserApiConfig};
use turmoil::{Builder, Sim};
use uuid::Uuid;

fn make_primary(sim: &mut Sim, path: PathBuf) {
    init_tracing();
    sim.host("primary", move || {
        let path = path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                user_api_config: UserApiConfig {
                    http_acceptor: Some(TurmoilAcceptor::bind(([0, 0, 0, 0], 8080)).await?),
                    ..Default::default()
                },
                admin_api_config: Some(AdminApiConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                    connector: TurmoilConnector,
                    disable_metrics: false,
                }),
                rpc_server_config: Some(RpcServerConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await?,
                    tls_config: None,
                }),
                disable_namespaces: false,
                disable_default_namespace: true,
                ..Default::default()
            };

            server.start().await?;

            Ok(())
        }
    });
}

#[test]
fn embedded_replica() {
    let mut sim = Builder::new().build();

    let tmp_dir_name = Uuid::new_v4().simple().to_string();

    let tmp = std::env::temp_dir().join(tmp_dir_name);

    tracing::debug!("tmp dir: {:?}", tmp);

    // We need to ensure that libsql's init code runs before we do anything
    // with rusqlite in sqld. This is because libsql has saftey checks and
    // needs to configure the sqlite api. Thus if we init sqld first
    // it will fail. To work around this we open a temp db in memory db
    // to ensure we run libsql's init code first. This DB is not actually
    // used in the test only for its run once init code.
    //
    // This does change the serialization mode for sqld but because the mode
    // that we use in libsql is safer than the sqld one it is still safe.
    let db = Database::open_in_memory().unwrap();
    db.connect().unwrap();

    make_primary(&mut sim, tmp.to_path_buf());

    sim.client("client", async move {
        let client = Client::new();
        client
            .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
            .await?;

        let db = Database::open_with_remote_sync_connector(
            tmp.join("embedded").to_str().unwrap(),
            "http://foo.primary:8080",
            "",
            TurmoilConnector,
        )?;

        let n = db.sync().await?;
        assert_eq!(n, 0);

        let conn = db.connect()?;

        conn.execute("CREATE TABLE user (id INTEGER NOT NULL PRIMARY KEY)", ())
            .await?;

        let n = db.sync().await?;
        assert_eq!(n, 2);

        let err = conn
            .execute("INSERT INTO user(id) VALUES (1), (1)", ())
            .await
            .unwrap_err();

        let libsql::Error::RemoteSqliteFailure(code, extended_code, _) = err else {
            panic!()
        };

        assert_eq!(code, 3);
        assert_eq!(extended_code, 1555);

        Ok(())
    });

    sim.run().unwrap();
}
