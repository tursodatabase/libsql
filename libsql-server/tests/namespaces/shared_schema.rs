use insta::assert_debug_snapshot;
use libsql::{Connection, Database};
use serde_json::json;
use tempfile::tempdir;
use tokio::time::Duration;
use turmoil::Builder;

use crate::common::{http::Client, net::TurmoilConnector};

use super::make_primary;

async fn get_schema_version(conn: &Connection) -> i64 {
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
        let schema_version_before = get_schema_version(&schema_conn).await;
        schema_conn
            .execute("create table test (c)", ())
            .await
            .unwrap();

        while get_schema_version(&schema_conn).await == schema_version_before {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // all schemas are the same
        assert_debug_snapshot!(check_schema("ns1").await);
        assert_debug_snapshot!(check_schema("ns2").await);
        assert_debug_snapshot!(check_schema("schema").await);

        let resp = http_get("http://primary:9090/v1/namespaces/schema/migrations").await;
        assert_eq!(
            resp,
            r#"{"schema_version":4,"migrations":[{"job_id":1,"status":"RunSuccess"}]}"#
        );

        let resp = http_get("http://primary:9090/v1/namespaces/schema/migrations/1").await;
        assert_eq!(resp, r#"{"job_id":1,"status":"RunSuccess","progress":[{"namespace":"ns1","status":"RunSuccess","error":null},{"namespace":"ns2","status":"RunSuccess","error":null}]}"#);

        Ok(())
    });

    sim.run().unwrap();
}
