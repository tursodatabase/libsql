//! Tests for standalone primary configuration

use crate::common::net::SimServer;

use super::common;

use std::sync::Arc;

use libsql::{Database, Value};
use tempfile::tempdir;
use tokio::sync::Notify;

use sqld::config::UserApiConfig;

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
