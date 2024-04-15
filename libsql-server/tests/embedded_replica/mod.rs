#![allow(deprecated)]

mod local;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::http::Client;
use crate::common::net::{init_tracing, SimServer, TestServer, TurmoilAcceptor, TurmoilConnector};
use crate::common::snapshot_metrics;
use libsql::Database;
use libsql_server::config::{AdminApiConfig, DbConfig, RpcServerConfig, UserApiConfig};
use serde_json::json;
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
                db_config: DbConfig {
                    max_log_size: 1,
                    max_log_duration: Some(5.0),
                    ..Default::default()
                },
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
    let tmp_embedded = tempdir().unwrap();
    let tmp_host = tempdir().unwrap();
    let tmp_embedded_path = tmp_embedded.path().to_owned();
    let tmp_host_path = tmp_host.path().to_owned();

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

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
            false,
            None,
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

        snapshot.assert_counter("libsql_server_user_http_response", 6);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn execute_batch() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

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
            false,
            None,
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
#[cfg(feature = "test-encryption")]
fn embedded_replica_with_encryption() {
    use bytes::Bytes;

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

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
            false,
            Some(libsql::EncryptionConfig::new(
                libsql::Cipher::Aes256Cbc,
                Bytes::from_static(b"SecretKey"),
            )),
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

        snapshot.assert_counter("libsql_server_user_http_response", 6);

        conn.execute("INSERT INTO user(id) VALUES (1)", ())
            .await
            .unwrap();

        drop(conn);
        drop(db);

        let db = Database::open_with_remote_sync_connector(
            path.to_str().unwrap(),
            "http://foo.primary:8080",
            "",
            TurmoilConnector,
            false,
            Some(libsql::EncryptionConfig::new(
                libsql::Cipher::Aes256Cbc,
                Bytes::from_static(b"SecretKey"),
            )),
        )
        .await?;

        db.sync().await.unwrap();

        let conn = db.connect()?;
        let mut res = conn.query("SELECT id FROM user", ()).await?;
        let row = res.next().await?;
        assert!(row.is_some());

        assert_eq!(1, row.unwrap().get::<i32>(0)?);

        let row = res.next().await?;
        assert!(row.is_none());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn replica_primary_reset() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
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
            false,
            None,
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
            .await
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
            .await
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
            false,
            None,
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
            .await
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
            .await
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
#[test]
fn replica_no_resync_on_restart() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(600))
        .build();
    let tmp = tempdir().unwrap();

    init_tracing();
    sim.host("primary", move || {
        let path = tmp.path().to_path_buf();
        async move {
            let make_server = || async {
                TestServer {
                    path: path.clone().into(),
                    user_api_config: UserApiConfig {
                        ..Default::default()
                    },
                    ..Default::default()
                }
            };
            let server = make_server().await;
            server.start_sim(8080).await.unwrap();

            Ok(())
        }
    });

    sim.client("client", async {
        // seed database
        {
            let db =
                Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)
                    .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("create table test (x)", ()).await.unwrap();
            for _ in 0..500 {
                conn.execute("insert into test values (42)", ())
                    .await
                    .unwrap();
            }
        }

        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("data");
        let before = Instant::now();
        let first_sync_index = {
            let db = Database::open_with_remote_sync_connector(
                db_path.display().to_string(),
                "http://primary:8080",
                "",
                TurmoilConnector,
                false,
                None,
            )
            .await
            .unwrap();
            db.sync().await.unwrap().unwrap()
        };
        let first_sync = before.elapsed();

        let before = Instant::now();
        let second_sync_index = {
            let db = Database::open_with_remote_sync_connector(
                db_path.display().to_string(),
                "http://primary:8080",
                "",
                TurmoilConnector,
                false,
                None,
            )
            .await
            .unwrap();
            db.sync().await.unwrap().unwrap()
        };
        let second_sync = before.elapsed();

        assert_eq!(first_sync_index, second_sync_index);
        // very sketchy way of checking the the second sync was very fast, because it performed
        // only a handshake.
        assert!(second_sync.as_secs_f64() / first_sync.as_secs_f64() < 0.10);

        Ok(())
    });

    sim.run().unwrap()
}

#[test]
fn replicate_with_snapshots() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .tcp_capacity(200)
        .build();

    const ROW_COUNT: i64 = 200;
    let tmp = tempdir().unwrap();

    init_tracing();
    sim.host("primary", move || {
        let path = tmp.path().to_path_buf();
        async move {
            let server = TestServer {
                path: path.clone().into(),
                user_api_config: UserApiConfig {
                    ..Default::default()
                },
                db_config: DbConfig {
                    max_log_size: 1, // very small log size to force snapshot creation
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
            };
            server.start_sim(8080).await.unwrap();

            Ok(())
        }
    });

    sim.client("client", async {
        let client = Client::new();
        client
            .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
            .await?;

        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)
            .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("create table test (x)", ()).await.unwrap();
        // insert enough to trigger snapshot creation.
        for _ in 0..ROW_COUNT {
            conn.execute("INSERT INTO test values (randomblob(6000))", ())
                .await
                .unwrap();
        }

        let tmp = tempdir().unwrap();
        let db = Database::open_with_remote_sync_connector(
            tmp.path().join("data").display().to_string(),
            "http://primary:8080",
            "",
            TurmoilConnector,
            false,
            None,
        )
        .await
        .unwrap();

        db.sync().await.unwrap();

        let conn = db.connect().unwrap();

        let mut res = conn.query("select count(*) from test", ()).await.unwrap();
        assert_eq!(
            *res.next()
                .await
                .unwrap()
                .unwrap()
                .get_value(0)
                .unwrap()
                .as_integer()
                .unwrap(),
            ROW_COUNT
        );

        let stats = client
            .get("http://primary:9090/v1/namespaces/default/stats")
            .await?
            .json_value()
            .await
            .unwrap();

        let stat = stats
            .get("embedded_replica_frames_replicated")
            .unwrap()
            .as_u64()
            .unwrap();

        assert_eq!(stat, 427);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn read_your_writes() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

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
            true,
            None,
        )
        .await?;

        let conn = db.connect()?;

        conn.execute("CREATE TABLE user (id INTEGER NOT NULL PRIMARY KEY)", ())
            .await?;

        conn.execute("INSERT INTO user(id) VALUES (1)", ())
            .await
            .unwrap();

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn proxy_write_returning_row() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

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
            true,
            None,
        )
        .await?;

        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;

        let mut rows = conn
            .query("insert into test values (12) returning rowid as id", ())
            .await
            .unwrap();

        rows.next().await.unwrap().unwrap();

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn freeze() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(u64::MAX))
        .build();

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
            true,
            None,
        )
        .await?;

        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;

        for _ in 0..50 {
            conn.execute("insert into test values (12)", ())
                .await
                .unwrap();
        }

        drop(conn);
        drop(db);

        let db = Database::open_with_remote_sync_connector(
            path.to_str().unwrap(),
            "http://foo.primary:8080",
            "",
            TurmoilConnector,
            true,
            None,
        )
        .await?;

        db.sync().await.unwrap();

        let db = db.freeze().unwrap();

        let conn = db.connect().unwrap();

        let mut rows = conn.query("select count(*) from test", ()).await.unwrap();

        let row = rows.next().await.unwrap().unwrap();

        let count = row.get::<u64>(0).unwrap();

        assert_eq!(count, 50);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn sync_interval() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

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
        let db = libsql::Builder::new_remote_replica(
            path.to_str().unwrap(),
            "http://foo.primary:8080".to_string(),
            "".to_string(),
        )
        .connector(TurmoilConnector)
        .sync_interval(Duration::from_millis(100))
        .build()
        .await?;

        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;

        conn.execute("insert into test values (12)", ())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut rows = conn.query("select * from test", ()).await.unwrap();

        let row = rows.next().await.unwrap().unwrap();

        assert_eq!(row.get::<u64>(0).unwrap(), 12);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn errors_on_bad_replica() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(u64::MAX))
        .build();

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
        let db = libsql::Builder::new_remote_replica(
            path.to_str().unwrap(),
            "http://foo.primary:8080".to_string(),
            "".to_string(),
        )
        .connector(TurmoilConnector)
        .build()
        .await?;

        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;

        conn.execute("insert into test values (12)", ())
            .await
            .unwrap();

        db.sync().await.unwrap();

        drop(conn);
        drop(db);

        let wal_index_file = format!("{}-client_wal_index", path.to_str().unwrap());

        std::fs::remove_file(wal_index_file).unwrap();

        libsql::Builder::new_remote_replica(
            path.to_str().unwrap(),
            "http://foo.primary:8080".to_string(),
            "".to_string(),
        )
        .connector(TurmoilConnector)
        .build()
        .await
        .unwrap_err();

        std::fs::remove_file(&path).unwrap();

        libsql::Builder::new_remote_replica(
            path.to_str().unwrap(),
            "http://foo.primary:8080".to_string(),
            "".to_string(),
        )
        .connector(TurmoilConnector)
        .build()
        .await
        .unwrap();

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn malformed_database() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(u64::MAX))
        .build();

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
        let db = libsql::Builder::new_remote_replica(
            path.to_str().unwrap(),
            "http://foo.primary:8080".to_string(),
            "".to_string(),
        )
        .read_your_writes(true)
        .connector(TurmoilConnector)
        .build()
        .await?;

        let conn = db.connect()?;

        let dir = env!("CARGO_MANIFEST_DIR").to_string();

        let file = std::fs::read_to_string(dir + "/output.sql").unwrap();

        let sqls = file.lines();

        for sql in sqls {
            if !sql.starts_with("--") {
                conn.execute(sql, ()).await.unwrap();
            }
        }

        db.sync().await.unwrap();

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn txn_bug_issue_1283() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(u64::MAX))
        .build();

    let tmp_embedded = tempdir().unwrap();
    let tmp_host = tempdir().unwrap();
    let tmp_embedded_path = tmp_embedded.path().to_owned();
    let tmp_host_path = tmp_host.path().to_owned();

    make_primary(&mut sim, tmp_host_path.clone());

    sim.client("client", async move {
        let path = tmp_embedded_path.join("embedded");

        let client = Client::new();
        client
            .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
            .await?;

        let db_url = "http://foo.primary:8080";
        let replica = libsql::Builder::new_remote_replica(
            path.to_str().unwrap(),
            db_url.to_string(),
            String::new(),
        )
        .connector(TurmoilConnector)
        .build()
        .await
        .unwrap();

        let remote = libsql::Builder::new_remote(db_url.to_string(), String::new())
            .connector(TurmoilConnector)
            .build()
            .await
            .unwrap();

        let replica_conn_1 = replica.connect().unwrap();
        let replica_conn_2 = replica.connect().unwrap();

        // Not really an embedded replica test but good to check this for remote connections too
        let remote_conn_1 = remote.connect().unwrap();
        let remote_conn_2 = remote.connect().unwrap();

        let remote_task_1 = tokio::task::spawn(async move { db_work(remote_conn_1).await });
        let remote_task_2 = tokio::task::spawn(async move { db_work(remote_conn_2).await });

        let (task_1_res, task_2_res) = tokio::join!(remote_task_1, remote_task_2);
        let remote_task_1_res = task_1_res.unwrap();
        let remote_task_2_res = task_2_res.unwrap();

        // Everything works as expected in case of remote connections.
        assert!(remote_task_1_res.is_ok());
        assert!(remote_task_2_res.is_ok());

        let replica_task_1 = tokio::task::spawn(async move { db_work(replica_conn_1).await });
        let replica_task_2 = tokio::task::spawn(async move { db_work(replica_conn_2).await });

        let (task_1_res, task_2_res) = tokio::join!(replica_task_1, replica_task_2);
        let replica_task_1_res = task_1_res.unwrap();
        let replica_task_2_res = task_2_res.unwrap();

        if replica_task_1_res.is_err() {
            panic!("Task 1 failed: {:?}", replica_task_1_res);
        }
        if replica_task_2_res.is_err() {
            panic!("Task 2 failed: {:?}", replica_task_2_res);
        }

        // One of these concurrent tasks fail currently. Both tasks should succeed.
        assert!(replica_task_1_res.is_ok());
        assert!(replica_task_2_res.is_ok());

        Ok(())
    });

    async fn db_work(conn: libsql::Connection) -> Result<(), anyhow::Error> {
        let tx = conn.transaction().await?;
        // Some business logic here...
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        tx.execute("SELECT 1", ()).await?;
        tx.commit().await?;
        Ok(())
    }

    sim.run().unwrap();
}
