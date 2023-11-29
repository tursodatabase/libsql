//! Tests for standalone primary configuration

use crate::common::net::{SimServer, TurmoilAcceptor};
use crate::common::{http::Client, snapshot_metrics};

use super::common;

use std::{sync::Arc, time::Duration};

use insta::assert_debug_snapshot;
use libsql::{Database, Value};
use tempfile::tempdir;
use tokio::sync::Notify;

use libsql_server::config::{AdminApiConfig, UserApiConfig};

use common::net::{init_tracing, TestServer, TurmoilConnector};

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

#[test]
fn basic_query() {
    let mut sim = turmoil::Builder::new().build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;
        conn.execute("insert into test values (12)", ()).await?;

        let mut rows = conn.query("select count(*) from test", ()).await?;

        assert!(matches!(
            rows.next().unwrap().unwrap().get_value(0).unwrap(),
            libsql::Value::Integer(1)
        ));

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn basic_metrics() {
    let mut sim = turmoil::Builder::new().build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;
        conn.execute("insert into test values (12)", ()).await?;

        let mut rows = conn.query("select count(*) from test", ()).await?;

        assert!(matches!(
            rows.next().unwrap().unwrap().get_value(0).unwrap(),
            libsql::Value::Integer(1)
        ));

        tokio::time::sleep(Duration::from_secs(1)).await;

        let snapshot = snapshot_metrics();
        snapshot.assert_counter("libsql_server_libsql_execute_program", 3);
        snapshot.assert_counter("libsql_server_user_http_response", 3);

        for (key, (_, _, val)) in snapshot.snapshot() {
            if key.kind() == metrics_util::MetricKind::Counter
                && key.key().name() == "libsql_client_version"
            {
                assert_eq!(val, &metrics_util::debugging::DebugValue::Counter(3));
                let label = key.key().labels().next().unwrap();
                assert!(label.value().starts_with("libsql-remote-"));
            }
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn primary_serializability() {
    let mut sim = turmoil::Builder::new().build();

    sim.host("primary", make_standalone_server);
    let notify = Arc::new(Notify::new());

    sim.client("writer", {
        let notify = notify.clone();
        async move {
            let db =
                Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
            let conn = db.connect()?;
            conn.execute("create table test (x)", ()).await?;
            conn.execute("insert into test values (12)", ()).await?;

            notify.notify_waiters();

            Ok(())
        }
    });

    sim.client("reader", {
        async move {
            let db =
                Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
            let conn = db.connect()?;

            notify.notified().await;

            let mut rows = conn.query("select count(*) from test", ()).await?;

            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0).unwrap(),
                Value::Integer(1)
            ));

            Ok(())
        }
    });

    sim.run().unwrap();
}

#[test]
#[ignore = "transaction not yet implemented with the libsql client."]
fn execute_transaction() {
    let mut sim = turmoil::Builder::new().build();

    sim.host("primary", make_standalone_server);
    let notify = Arc::new(Notify::new());

    sim.client("writer", {
        let notify = notify.clone();
        async move {
            let db =
                Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
            let conn = db.connect()?;

            conn.execute("create table test (x)", ()).await?;

            let txn = conn.transaction().await?;
            txn.execute("insert into test values (42)", ()).await?;

            notify.notify_waiters();
            notify.notified().await;
            // we can read our write:
            let mut rows = txn.query("select count(*) from test", ()).await?;
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0).unwrap(),
                Value::Integer(1)
            ));
            txn.commit().await?;
            notify.notify_waiters();

            Ok(())
        }
    });

    sim.client("reader", {
        async move {
            let db =
                Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
            let conn = db.connect()?;

            notify.notified().await;
            // at this point we should not see the written row.
            let mut rows = conn.query("select count(*) from test", ()).await?;
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0).unwrap(),
                Value::Integer(0)
            ));
            notify.notify_waiters();

            let txn = conn.transaction().await?;
            txn.execute("insert into test values (42)", ()).await?;

            notify.notify_waiters();
            notify.notified().await;

            // now we can read the inserted row
            let mut rows = conn.query("select count(*) from test", ()).await?;
            assert!(matches!(
                rows.next().unwrap().unwrap().get_value(0).unwrap(),
                Value::Integer(1)
            ));
            notify.notify_waiters();

            Ok(())
        }
    });

    sim.run().unwrap();
}

#[test]
fn basic_query_fail() {
    let mut sim = turmoil::Builder::new().build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;
        conn.execute("create unique index test_index on test(x)", ())
            .await?;
        conn.execute("insert into test values (12)", ()).await?;
        assert_debug_snapshot!(conn
            .execute("insert into test values (12)", ())
            .await
            .unwrap_err());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn random_rowid() {
    let mut sim = turmoil::Builder::new().build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute(
            "CREATE TABLE shopping_list(item text, quantity int) RANDOM ROWID",
            (),
        )
        .await?;

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn dirty_startup_dont_prevent_namespace_creation() {
    let mut sim = turmoil::Builder::new().build();

    sim.host("primary", || async {
        init_tracing();
        let tmp = tempdir()?;
        let server = TestServer {
            path: tmp.path().to_owned().into(),
            user_api_config: UserApiConfig {
                hrana_ws_acceptor: None,
                ..Default::default()
            },
            admin_api_config: Some(AdminApiConfig {
                acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await.unwrap(),
                connector: TurmoilConnector,
                disable_metrics: true,
            }),
            disable_default_namespace: true,
            disable_namespaces: false,
            ..Default::default()
        };

        tokio::fs::File::create(tmp.path().join(".sentinel"))
            .await
            .unwrap();
        server.start_sim(8080).await?;

        Ok(())
    });

    sim.client("test", async {
        let client = Client::new();
        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/test/create",
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert!(resp.status().is_success());
        Ok(())
    });

    sim.run().unwrap();
}
