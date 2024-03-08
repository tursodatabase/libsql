use hyper::StatusCode;
use insta::{assert_debug_snapshot, assert_json_snapshot};
use libsql::Database;
use serde_json::json;
use tempfile::tempdir;
use tokio::time::Duration;
use turmoil::Builder;

use crate::common::{http::Client, net::TurmoilConnector};

use super::make_primary;

macro_rules! assert_all_eq {
    ($first:expr, $( $rest:expr ),+ $(,)?) => {
        $(
            assert_eq!($first, $rest);
        )+
    };
}

async fn get_schema_version(ns: &str) -> i64 {
    let db = Database::open_remote_with_connector(
        format!("http://{ns}.primary:8080"),
        String::new(),
        TurmoilConnector,
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let mut rows = conn.query("PRAGMA schema_version", ()).await.unwrap();
    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
}

async fn check_schema(ns: &str) -> Vec<String> {
    let db = Database::open_remote_with_connector(
        format!("http://{ns}.primary:8080"),
        String::new(),
        TurmoilConnector,
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let mut rows = conn.query("SELECT * from sqlite_schema", ()).await.unwrap();
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        out.push(format!("{row:?}"));
    }

    out
}

async fn http_get(url: &str) -> String {
    let client = Client::new();
    client.get(url).await.unwrap().body_string().await.unwrap()
}

#[test]
fn perform_schema_migration() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(100000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let client = Client::new();
        client
            .post(
                "http://primary:9090/v1/namespaces/schema/create",
                json!({"shared_schema": true }),
            )
            .await
            .unwrap();
        client
            .post(
                "http://primary:9090/v1/namespaces/ns1/create",
                json!({"shared_schema_name": "schema" }),) .await .unwrap();
        client
            .post(
                "http://primary:9090/v1/namespaces/ns2/create",
                json!({"shared_schema_name": "schema" }),
            )
            .await
            .unwrap();

        let schema_db = Database::open_remote_with_connector(
            "http://schema.primary:8080",
            String::new(),
            TurmoilConnector,
        )
        .unwrap();
        let schema_version_before = get_schema_version("schema").await;
        let schema_conn = schema_db.connect().unwrap();
        schema_conn
            .execute("create table test (c)", ())
            .await
            .unwrap();

        while get_schema_version("schema").await == schema_version_before {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // all schemas are the same
        assert_debug_snapshot!(check_schema("ns1").await);
        assert_debug_snapshot!(check_schema("ns2").await);
        assert_debug_snapshot!(check_schema("schema").await);

        // check all schema versions are same as primary schema db
        let expected_schema_version = 1;
        assert_all_eq!(expected_schema_version, get_schema_version("schema").await, get_schema_version("ns1").await, get_schema_version("ns2").await);

        let resp = http_get("http://primary:9090/v1/namespaces/schema/migrations").await;
        assert_eq!(
            resp,
            r#"{"schema_version":4,"migrations":[{"job_id":1,"status":"RunSuccess"}]}"#
        );

        let resp = http_get("http://primary:9090/v1/namespaces/schema/migrations/1").await;
        assert_eq!(resp, r#"{"job_id":1,"status":"RunSuccess","progress":[{"namespace":"ns1","status":"RunSuccess","error":null},{"namespace":"ns2","status":"RunSuccess","error":null}]}"#);

        // we add a new namespace and expect it to have the same schema version and schema
        client
            .post(
                "http://primary:9090/v1/namespaces/ns3/create",
                json!({"shared_schema_name": "schema" }),
            )
            .await
            .unwrap();
        assert_debug_snapshot!(check_schema("ns3").await);
        assert_eq!(expected_schema_version, get_schema_version("ns3").await);

        // we will perform a new migration and expect the new and old databases to have same schema
        schema_conn
            .execute("create table test2 (c)", ())
            .await
            .unwrap();

        while get_schema_version("schema").await == schema_version_before {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // all schemas are the same
        assert_debug_snapshot!(check_schema("ns1").await);
        assert_debug_snapshot!(check_schema("ns2").await);
        assert_debug_snapshot!(check_schema("ns3").await);
        assert_debug_snapshot!(check_schema("schema").await);

        // check all schema versions are same as primary schema db
        let expected_schema_version = 2;
        assert_all_eq!(expected_schema_version, get_schema_version("schema").await, get_schema_version("ns1").await, get_schema_version("ns2").await, get_schema_version("ns3").await);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn no_job_created_when_migration_job_is_invalid() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(100000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let client = Client::new();
        client
            .post(
                "http://primary:9090/v1/namespaces/schema/create",
                json!({"shared_schema": true }),
            )
            .await
            .unwrap();

        let schema_db = Database::open_remote_with_connector(
            "http://schema.primary:8080",
            String::new(),
            TurmoilConnector,
        )
        .unwrap();
        let schema_conn = schema_db.connect().unwrap();
        assert_debug_snapshot!(schema_conn
            .execute_batch("create table test (c); create table test (c)")
            .await
            .unwrap_err());

        let resp = client
            .get("http://primary:9090/v1/namespaces/schema/migrations/1")
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_debug_snapshot!(resp.json_value().await.unwrap());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn migration_contains_txn_statements() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(100000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let client = Client::new();
        client
            .post(
                "http://primary:9090/v1/namespaces/schema/create",
                json!({"shared_schema": true }),
            )
            .await
            .unwrap();

        let schema_db = Database::open_remote_with_connector(
            "http://schema.primary:8080",
            String::new(),
            TurmoilConnector,
        )
        .unwrap();
        let schema_conn = schema_db.connect().unwrap();
        assert_debug_snapshot!(schema_conn
            .execute_batch("begin; create table test (c)")
            .await
            .unwrap_err());

        let resp = client
            .get("http://primary:9090/v1/namespaces/schema/migrations/1")
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_debug_snapshot!(resp.json_value().await.unwrap());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn dry_run_failure() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(100000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let client = Client::new();
        client
            .post(
                "http://primary:9090/v1/namespaces/schema/create",
                json!({"shared_schema": true }),
            )
            .await
            .unwrap();

        client
            .post(
                "http://primary:9090/v1/namespaces/ns1/create",
                json!({"shared_schema_name": "schema" }),
            )
            .await
            .unwrap();
        client
            .post(
                "http://primary:9090/v1/namespaces/ns2/create",
                json!({"shared_schema_name": "schema" }),
            )
            .await
            .unwrap();

        let schema_db = Database::open_remote_with_connector(
            "http://schema.primary:8080",
            String::new(),
            TurmoilConnector,
        )
        .unwrap();
        let schema_conn = schema_db.connect().unwrap();
        let schema_version_before = get_schema_version("schema").await;
        schema_conn
            .execute_batch("create table test (c)")
            .await
            .unwrap();

        while get_schema_version("schema").await == schema_version_before {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let ns1_db = Database::open_remote_with_connector(
            "http://ns1.primary:8080",
            String::new(),
            TurmoilConnector,
        )
        .unwrap();
        let ns1_conn = ns1_db.connect().unwrap();
        ns1_conn
            .execute("insert into test values (NULL)", ())
            .await
            .unwrap();

        // we are creating a new table with a constraint on the column. when the dry run is
        // executed against the schema or ns2, it works, because test is empty there, but it should
        // fail on ns1, because it test contains a row with NULL.
        schema_conn
            .execute_batch("create table test2 (c NOT NULL); insert into test2 select * from test")
            .await
            .unwrap();

        loop {
            let resp = client
                .get("http://primary:9090/v1/namespaces/schema/migrations/2")
                .await
                .unwrap()
                .json_value()
                .await
                .unwrap();
            if resp["status"].as_str().unwrap() == "RunFailure" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let resp = client
            .get("http://primary:9090/v1/namespaces/schema/migrations/2")
            .await
            .unwrap()
            .json_value()
            .await
            .unwrap();
        assert_json_snapshot!(resp);

        // all schemas are the same (test2 doesn't exist)
        assert_debug_snapshot!(check_schema("ns1").await);
        assert_debug_snapshot!(check_schema("ns2").await);
        assert_debug_snapshot!(check_schema("schema").await);

        Ok(())
    });

    sim.run().unwrap();
}
