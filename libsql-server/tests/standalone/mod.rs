//! Tests for standalone primary configuration
#![allow(deprecated)]

use crate::common::net::{SimServer, TurmoilAcceptor};
use crate::common::{http::Client, snapshot_metrics};

use super::common;

use std::{sync::Arc, time::Duration};

use insta::assert_debug_snapshot;
use libsql::{params, Connection, Database, Value};
use tempfile::tempdir;
use tokio::sync::Notify;

use libsql_server::config::{AdminApiConfig, UserApiConfig};

use common::net::{init_tracing, TestServer, TurmoilConnector};

mod attach;
mod auth;

async fn make_standalone_server() -> Result<(), Box<dyn std::error::Error>> {
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
        disable_namespaces: false,
        ..Default::default()
    };

    server.start_sim(8080).await?;

    Ok(())
}

#[test]
fn basic_query() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;
        conn.execute("insert into test values (12)", ()).await?;

        let mut rows = conn.query("select count(*) from test", ()).await?;

        assert!(matches!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            libsql::Value::Integer(1)
        ));

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn basic_metrics() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;
        conn.execute("insert into test values (12)", ()).await?;

        let mut rows = conn.query("select count(*) from test", ()).await?;

        assert!(matches!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
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
                let label = key.key().labels().next().unwrap();
                assert!(label.value().starts_with("libsql-remote-"));
                assert_eq!(val, &metrics_util::debugging::DebugValue::Counter(3));
            }
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn primary_serializability() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

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
                rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
                Value::Integer(1)
            ));

            Ok(())
        }
    });

    sim.run().unwrap();
}

#[test]
fn basic_query_fail() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;
        conn.execute("create unique index test_index on test(x)", ())
            .await?;
        conn.execute("insert into test values (12)", ()).await?;
        let e = conn
            .execute("insert into test values (12)", ())
            .await
            .unwrap_err();
        assert_debug_snapshot!(e);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn begin_commit() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;

        conn.execute("begin;", ()).await?;
        conn.execute("insert into test values (12);", ()).await?;

        // we can read the inserted row
        let mut rows = conn.query("select count(*) from test", ()).await?;
        assert_eq!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            Value::Integer(1)
        );

        conn.execute("commit;", ()).await?;

        // after rollback row is no longer there
        let mut rows = conn.query("select count(*) from test", ()).await?;
        assert_eq!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            Value::Integer(1)
        );

        Ok(())
    });

    sim.run().unwrap();
}
#[test]
fn begin_rollback() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;

        conn.execute("begin;", ()).await?;
        conn.execute("insert into test values (12);", ()).await?;

        // we can read the inserted row
        let mut rows = conn.query("select count(*) from test", ()).await?;
        assert_eq!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            Value::Integer(1)
        );

        conn.execute("rollback;", ()).await?;

        // after rollback row is no longer there
        let mut rows = conn.query("select count(*) from test", ()).await?;
        assert_eq!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            Value::Integer(0)
        );

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn is_autocommit() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        assert!(conn.is_autocommit());
        conn.execute("create table test (x)", ()).await?;

        conn.execute("begin;", ()).await?;
        assert!(!conn.is_autocommit());
        conn.execute("insert into test values (12);", ()).await?;
        conn.execute("commit;", ()).await?;
        assert!(conn.is_autocommit());

        // make an explicit transaction
        {
            let tx = conn.transaction().await?;
            assert!(!tx.is_autocommit());
            assert!(conn.is_autocommit()); // connection is still autocommit

            tx.execute("insert into test values (12);", ()).await?;
            // transaction rolls back
        }

        assert!(conn.is_autocommit());

        let mut rows = conn.query("select count(*) from test", ()).await?;
        assert_eq!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            Value::Integer(1)
        );

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn random_rowid() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

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
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

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

#[test]
fn row_count() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("CREATE TABLE test(a int, b int);", ()).await?;
        conn.execute("BEGIN;", ()).await?;
        insert_rows(&conn, 0, 10).await?;
        insert_rows_with_args(&conn, 10, 10).await?;
        assert_rows_count(&conn, 20).await?;

        Ok(())
    });

    sim.run().unwrap();
}

async fn insert_rows(conn: &Connection, start: u32, count: u32) -> libsql::Result<()> {
    for i in start..(start + count) {
        conn.execute(&format!("INSERT INTO test(a, b) VALUES({i},'{i}')"), ())
            .await?;
    }
    Ok(())
}

async fn insert_rows_with_args(conn: &Connection, start: u32, count: u32) -> libsql::Result<()> {
    for i in start..(start + count) {
        let mut stmt = conn.prepare("INSERT INTO test(a, b) VALUES(?,?)").await?;
        stmt.execute(params![i, i]).await?;
    }
    Ok(())
}

async fn assert_rows_count(conn: &Connection, expected: u32) -> libsql::Result<()> {
    let mut q = conn.query("SELECT COUNT(*) FROM test", ()).await?;
    let row = q.next().await?.unwrap();
    let count: u32 = row.get(0)?;
    assert_eq!(count, expected);
    Ok(())
}
