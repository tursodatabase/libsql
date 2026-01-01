use std::convert::Infallible;
use std::time::Duration;

use hyper::{service::make_service_fn, Body, Response, StatusCode};
use insta::{assert_json_snapshot, assert_snapshot};
use libsql::{Database, Value};
use serde_json::json;
use tempfile::tempdir;
use tower::service_fn;
use turmoil::Builder;

use crate::common::http::Client;
use crate::common::net::{TurmoilAcceptor, TurmoilConnector};
use crate::namespaces::make_primary;

#[test]
fn load_namespace_from_dump_from_url() {
    const DUMP: &str = r#"
        PRAGMA foreign_keys=OFF;
    BEGIN TRANSACTION;
    CREATE TABLE test (x);
    INSERT INTO test VALUES(42);
    COMMIT;"#;

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.host("dump-store", || async {
        let incoming = TurmoilAcceptor::bind(([0, 0, 0, 0], 8080)).await?;
        let server =
            hyper::server::Server::builder(incoming).serve(make_service_fn(|_conn| async {
                Ok::<_, Infallible>(service_fn(|_req| async {
                    Ok::<_, Infallible>(Response::new(Body::from(DUMP)))
                }))
            }));

        server.await.unwrap();

        Ok(())
    });

    sim.client("client", async {
        let client = Client::new();
        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": "http://dump-store:8080/"}),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_snapshot!(resp.body_string().await.unwrap());

        let foo =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let foo_conn = foo.connect()?;
        let mut rows = foo_conn.query("select count(*) from test", ()).await?;
        assert!(matches!(
            rows.next().await.unwrap().unwrap().get_value(0)?,
            Value::Integer(1)
        ));

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn load_namespace_from_dump_from_file() {
    const DUMP: &str = r#"
        PRAGMA foreign_keys=OFF;
    BEGIN TRANSACTION;
    CREATE TABLE test (x);
    INSERT INTO test VALUES(42);
    COMMIT;"#;

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    std::fs::write(tmp_path.join("dump.sql"), DUMP).unwrap();

    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async move {
        let client = Client::new();

        // path is not absolute is an error
        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": "file:dump.sql"}),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // path doesn't exist is an error
        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": "file:/dump.sql"}),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": format!("file:{}", tmp_path.join("dump.sql").display())}),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "{}",
            resp.json::<serde_json::Value>().await.unwrap_or_default()
        );

        let foo =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let foo_conn = foo.connect()?;
        let mut rows = foo_conn.query("select count(*) from test", ()).await?;
        assert!(matches!(
            rows.next().await.unwrap().unwrap().get_value(0)?,
            Value::Integer(1)
        ));

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn load_namespace_from_no_commit() {
    const DUMP: &str = r#"
    PRAGMA foreign_keys=OFF;
    BEGIN TRANSACTION;
    CREATE TABLE test (x);
    INSERT INTO test VALUES(42);
    "#;

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    std::fs::write(tmp_path.join("dump.sql"), DUMP).unwrap();

    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async move {
        let client = Client::new();
        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": format!("file:{}", tmp_path.join("dump.sql").display())}),
            )
            .await
            .unwrap();
        // the dump is malformed
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "{}",
            resp.json::<serde_json::Value>().await.unwrap_or_default()
        );

        // namespace doesn't exist
        let foo =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let foo_conn = foo.connect()?;
        assert!(foo_conn
            .query("select count(*) from test", ())
            .await
            .is_err());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn load_namespace_from_no_txn() {
    const DUMP: &str = r#"
    PRAGMA foreign_keys=OFF;
    CREATE TABLE test (x);
    INSERT INTO test VALUES(42);
    COMMIT;
    "#;

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    std::fs::write(tmp_path.join("dump.sql"), DUMP).unwrap();

    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async move {
        let client = Client::new();
        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": format!("file:{}", tmp_path.join("dump.sql").display())}),
            )
            .await?;
        // the dump is malformed
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "{}",
            resp.json::<serde_json::Value>().await.unwrap_or_default()
        );
        assert_json_snapshot!(resp.json_value().await.unwrap());

        // namespace doesn't exist
        let foo =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let foo_conn = foo.connect()?;
        assert!(foo_conn
            .query("select count(*) from test", ())
            .await
            .is_err());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn export_dump() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();

    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async move {
        let client = Client::new();
        let resp = client
            .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
            .await?;
        assert_eq!(resp.status(), StatusCode::OK);

        let foo =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let foo_conn = foo.connect()?;
        foo_conn.execute("create table test (x)", ()).await?;
        foo_conn.execute("insert into test values (42)", ()).await?;
        foo_conn
            .execute("insert into test values ('foo')", ())
            .await?;
        foo_conn
            .execute("insert into test values ('bar')", ())
            .await?;

        let resp = client.get("http://foo.primary:8080/dump").await?;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_snapshot!(resp.body_string().await?);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn load_dump_with_attach_rejected() {
    const DUMP: &str = r#"
        PRAGMA foreign_keys=OFF;
    BEGIN TRANSACTION;
    CREATE TABLE test (x);
    INSERT INTO test VALUES(42);
    ATTACH foo/bar.sql
    COMMIT;"#;

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    std::fs::write(tmp_path.join("dump.sql"), DUMP).unwrap();

    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async move {
        let client = Client::new();

        // path is not absolute is an error
        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": "file:dump.sql"}),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // path doesn't exist is an error
        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": "file:/dump.sql"}),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": format!("file:{}", tmp_path.join("dump.sql").display())}),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "{}",
            resp.json::<serde_json::Value>().await.unwrap_or_default()
        );

        assert_snapshot!(resp.body_string().await?);

        let foo =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let foo_conn = foo.connect()?;

        let res = foo_conn.query("select count(*) from test", ()).await;
        // This should error since the dump should have failed!
        assert!(res.is_err());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn load_dump_with_invalid_sql() {
    const DUMP: &str = r#"
        PRAGMA foreign_keys=OFF;
    BEGIN TRANSACTION;
    CREATE TABLE test (x);
    INSERT INTO test VALUES(42);
    SELECT abs(-9223372036854775808) 
    COMMIT;"#;

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    std::fs::write(tmp_path.join("dump.sql"), DUMP).unwrap();

    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async move {
        let client = Client::new();

        // path is not absolute is an error
        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": "file:dump.sql"}),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // path doesn't exist is an error
        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": "file:/dump.sql"}),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "dump_url": format!("file:{}", tmp_path.join("dump.sql").display())}),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "{}",
            resp.json::<serde_json::Value>().await.unwrap_or_default()
        );

        assert_snapshot!(resp.body_string().await?);

        let foo =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let foo_conn = foo.connect()?;

        let res = foo_conn.query("select count(*) from test", ()).await;
        // This should error since the dump should have failed!
        assert!(res.is_err());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn load_dump_with_trigger() {
    const DUMP: &str = r#"
    BEGIN TRANSACTION;
    CREATE TABLE test (x);
    CREATE TRIGGER simple_trigger 
    AFTER INSERT ON test 
    BEGIN
        INSERT INTO test VALUES (999);
    END;
    INSERT INTO test VALUES (1);
    COMMIT;"#;

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    std::fs::write(tmp_path.join("dump.sql"), DUMP).unwrap();

    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async move {
        let client = Client::new();

        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/debug_test/create",
                json!({ "dump_url": format!("file:{}", tmp_path.join("dump.sql").display())}),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let db = Database::open_remote_with_connector(
            "http://debug_test.primary:8080",
            "",
            TurmoilConnector,
        )?;
        let conn = db.connect()?;

        // Original INSERT: 1, Trigger INSERT: 999 = 2 total rows
        let mut rows = conn.query("SELECT COUNT(*) FROM test", ()).await?;
        let row = rows.next().await?.unwrap();
        assert_eq!(row.get::<i64>(0)?, 2);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn load_dump_with_case_trigger() {
    const DUMP: &str = r#"
    BEGIN TRANSACTION;
    CREATE TABLE test (id INTEGER, rate REAL DEFAULT 0.0);
    CREATE TRIGGER case_trigger 
    AFTER INSERT ON test 
    BEGIN 
        UPDATE test 
        SET rate = 
            CASE 
                WHEN NEW.id = 1 
                    THEN 0.1 
                ELSE 0.0 
            END 
        WHERE id = NEW.id; 
    END;

    INSERT INTO test (id) VALUES (1);
    COMMIT;"#;

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    std::fs::write(tmp_path.join("dump.sql"), DUMP).unwrap();

    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async move {
        let client = Client::new();

        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/case_test/create",
                json!({ "dump_url": format!("file:{}", tmp_path.join("dump.sql").display())}),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let db = Database::open_remote_with_connector(
            "http://case_test.primary:8080",
            "",
            TurmoilConnector,
        )?;
        let conn = db.connect()?;

        let mut rows = conn.query("SELECT id, rate FROM test", ()).await?;
        let row = rows.next().await?.unwrap();
        assert_eq!(row.get::<i64>(0)?, 1);
        assert!((row.get::<f64>(1)? - 0.1).abs() < 0.001);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn load_dump_with_nested_case() {
    const DUMP: &str = r#"
    BEGIN TRANSACTION;
    CREATE TABLE orders (id INTEGER, amount REAL, status TEXT);
    CREATE TRIGGER nested_trigger 
    AFTER UPDATE ON orders 
    BEGIN 
        UPDATE orders 
        SET amount = 
            CASE 
                WHEN NEW.status = 'completed' 
                    THEN 
                        CASE
                            WHEN OLD.id = 1
                                THEN OLD.amount * 0.9
                            ELSE OLD.amount * 0.8
                        END
                ELSE OLD.amount 
            END 
        WHERE id = NEW.id; 
    END;
    
    INSERT INTO orders (id, amount, status) VALUES (1, 100.0, 'pending');
    COMMIT;"#;

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    std::fs::write(tmp_path.join("dump.sql"), DUMP).unwrap();

    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async move {
        let client = Client::new();

        let resp = client
            .post(
                "http://primary:9090/v1/namespaces/nested_test/create",
                json!({ "dump_url": format!("file:{}", tmp_path.join("dump.sql").display())}),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let db = Database::open_remote_with_connector(
            "http://nested_test.primary:8080",
            "",
            TurmoilConnector,
        )?;
        let conn = db.connect()?;

        conn.execute("UPDATE orders SET status = 'completed' WHERE id = 1", ())
            .await?;
        let mut rows = conn
            .query("SELECT amount FROM orders WHERE id = 1", ())
            .await?;
        let row = rows.next().await?.unwrap();
        assert!((row.get::<f64>(0)? - 90.0).abs() < 0.001);

        Ok(())
    });

    sim.run().unwrap();
}
