use std::path::PathBuf;

use crate::common::http::Client;
use crate::common::net::{init_tracing, SimServer, TestServer, TurmoilAcceptor, TurmoilConnector};
use libsql::Database;
use serde_json::json;
use sqld::config::{AdminApiConfig, RpcServerConfig, UserApiConfig};
use tempfile::tempdir;
use turmoil::{Builder, Sim};

fn enable_libsql_logging() {
    use std::ffi::c_int;
    use std::sync::Once;
    static ONCE: Once = Once::new();

    fn libsql_log(code: c_int, msg: &str) {
        tracing::error!("sqlite error {code}: {msg}");
    }

    ONCE.call_once(|| unsafe {
        rusqlite::trace::config_log(Some(libsql_log)).unwrap();
    });
}

fn make_primary(sim: &mut Sim, path: PathBuf) {
    init_tracing();
    enable_libsql_logging();
    sim.host("primary", move || {
        let path = path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                user_api_config: UserApiConfig {
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

            server.start_sim(8080).await?;

            Ok(())
        }
    });
}

#[test]
fn embedded_replica() {
    let mut sim = Builder::new().build();

    let tmp_embedded = tempdir().unwrap();
    let tmp_host = tempdir().unwrap();
    let tmp_embedded_path = tmp_embedded.path().to_owned();
    let tmp_host_path = tmp_host.path().to_owned();

    make_primary(&mut sim, tmp_host_path.clone());

    sim.client("client", async move {
        let client = Client::new();
        client
            .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
            .await?;

        let path = tmp_embedded_path.join("embedded");
        let db = Database::open_with_remote_sync_connector(
            path.to_str().unwrap(),
            "http://foo.primary:8080",
            "",
            TurmoilConnector,
        )
        .await?;

        let n = db.sync().await?;
        assert_eq!(n, None);

        let conn = db.connect()?;

        conn.execute("CREATE TABLE user (id INTEGER NOT NULL PRIMARY KEY)", ())
            .await?;

        let n = db.sync().await?;
        assert_eq!(n, Some(1));

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

#[test]
fn execute_batch() {
    let mut sim = Builder::new().build();

    let tmp_embedded = tempdir().unwrap();
    let tmp_host = tempdir().unwrap();
    let tmp_embedded_path = tmp_embedded.path().to_owned();
    let tmp_host_path = tmp_host.path().to_owned();

    make_primary(&mut sim, tmp_host_path.clone());

    sim.client("client", async move {
        let client = Client::new();
        client
            .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
            .await?;

        let path = tmp_embedded_path.join("embedded");
        let db = Database::open_with_remote_sync_connector(
            path.to_str().unwrap(),
            "http://foo.primary:8080",
            "",
            TurmoilConnector,
        )
        .await?;

        let n = db.sync().await?;
        assert_eq!(n, None);

        let conn = db.connect()?;

        conn.execute("CREATE TABLE user (id INTEGER NOT NULL PRIMARY KEY)", ())
            .await?;

        let n = db.sync().await?;
        assert_eq!(n, Some(1));

        conn.execute_batch(
            "BEGIN;
            INSERT INTO user (id) VALUES (2);", // COMMIT;",
        )
        .await?;

        Ok(())
    });

    sim.run().unwrap();
}
