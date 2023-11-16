use std::path::PathBuf;
use std::sync::Arc;

use crate::common::http::Client;
use crate::common::net::{init_tracing, SimServer, TestServer, TurmoilAcceptor, TurmoilConnector};
use crate::common::snapshot_metrics;
use libsql::Database;
use serde_json::json;
use sqld::config::{AdminApiConfig, RpcServerConfig, UserApiConfig};
use tempfile::tempdir;
use tokio::sync::Notify;
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

        let snapshot = snapshot_metrics();

        for (key, (_, _, val)) in snapshot.snapshot() {
            if key.kind() == metrics_util::MetricKind::Counter
                && key.key().name() == "libsql_client_version"
            {
                assert_eq!(val, &metrics_util::debugging::DebugValue::Counter(6));
                let label = key.key().labels().next().unwrap();
                assert!(label.value().starts_with("libsql-rpc-"));
            }
        }

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

#[test]
fn replica_primary_reset() {
    let mut sim = Builder::new().build();
    let tmp = tempdir().unwrap();

    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();

    init_tracing();
    sim.host("primary", move || {
        let notify = notify_clone.clone();
        let path = tmp.path().to_path_buf();
        async move {
            let make_server = || async {
                TestServer {
                    path: path.clone().into(),
                    user_api_config: UserApiConfig {
                        ..Default::default()
                    },
                    admin_api_config: Some(AdminApiConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await.unwrap(),
                        connector: TurmoilConnector,
                        disable_metrics: true,
                    }),
                    rpc_server_config: Some(RpcServerConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await.unwrap(),
                        tls_config: None,
                    }),
                    ..Default::default()
                }
            };
            let server = make_server().await;
            let shutdown = server.shutdown.clone();

            let fut = async move { server.start_sim(8080).await };

            tokio::pin!(fut);

            loop {
                tokio::select! {
                    res =  &mut fut => {
                        res.unwrap();
                        break
                    }
                    _ = notify.notified() => {
                        shutdown.notify_waiters();
                    },
                }
            }
            // remove the wallog and start again
            tokio::fs::remove_file(path.join("dbs/default/wallog"))
                .await
                .unwrap();
            notify.notify_waiters();
            let server = make_server().await;
            server.start_sim(8080).await.unwrap();

            Ok(())
        }
    });

    sim.client("client", async move {
        let primary =
            Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = primary.connect()?;

        // insert a few valued into the primary
        conn.execute("create table test (x)", ()).await.unwrap();
        for _ in 0..50 {
            conn.execute("insert into test values (42)", ())
                .await
                .unwrap();
        }

        let tmp = tempdir().unwrap();
        let replica = Database::open_with_remote_sync_connector(
            tmp.path().join("data").display().to_string(),
            "http://primary:8080",
            "",
            TurmoilConnector,
        )
        .await
        .unwrap();
        let replica_index = replica.sync().await.unwrap().unwrap();
        let primary_index = Client::new()
            .get("http://primary:9090/v1/namespaces/default/stats")
            .await
            .unwrap()
            .json_value()
            .await
            .unwrap()["replication_index"]
            .clone()
            .as_u64()
            .unwrap();

        assert_eq!(replica_index, primary_index);

        let replica_count = *replica
            .connect()
            .unwrap()
            .query("select count(*) from test", ())
            .await
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap()
            .as_integer()
            .unwrap();
        let primary_count = *primary
            .connect()
            .unwrap()
            .query("select count(*) from test", ())
            .await
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap()
            .as_integer()
            .unwrap();
        assert_eq!(primary_count, replica_count);

        notify.notify_waiters();
        notify.notified().await;

        // drop the replica here, to make sure not to reuse an open connection.
        drop(replica);
        let replica = Database::open_with_remote_sync_connector(
            tmp.path().join("data").display().to_string(),
            "http://primary:8080",
            "",
            TurmoilConnector,
        )
        .await
        .unwrap();
        let replica_index = replica.sync().await.unwrap().unwrap();
        let primary_index = Client::new()
            .get("http://primary:9090/v1/namespaces/default/stats")
            .await
            .unwrap()
            .json_value()
            .await
            .unwrap()["replication_index"]
            .clone()
            .as_u64()
            .unwrap();

        assert_eq!(replica_index, primary_index);

        let replica_count = *replica
            .connect()
            .unwrap()
            .query("select count(*) from test", ())
            .await
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap()
            .as_integer()
            .unwrap();
        let primary_count = *primary
            .connect()
            .unwrap()
            .query("select count(*) from test", ())
            .await
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap()
            .as_integer()
            .unwrap();
        assert_eq!(primary_count, replica_count);

        Ok(())
    });

    sim.run().unwrap();
}
