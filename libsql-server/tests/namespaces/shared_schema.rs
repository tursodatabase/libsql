use hyper::StatusCode;
use insta::{assert_debug_snapshot, assert_json_snapshot};
use libsql::Database;
use serde_json::json;
use tempfile::tempdir;
use tokio::time::Duration;
use turmoil::Builder;

use crate::common::{
    auth::{encode, key_pair},
    http::Client,
    net::TurmoilConnector,
};

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

async fn check_data(ns: &str) -> Vec<String> {
    let db = Database::open_remote_with_connector(
        format!("http://{ns}.primary:8080"),
        String::new(),
        TurmoilConnector,
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let mut rows = conn.query("SELECT * from test", ()).await.unwrap();
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

        let resp = http_get("http://schema.primary:8080/v1/jobs").await;
        assert_eq!(
            resp,
            r#"{"schema_version":4,"migrations":[{"job_id":1,"status":"RunSuccess"}]}"#
        );

        let resp = http_get("http://schema.primary:8080/v1/jobs/1").await;
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
            .get("http://schema.primary:8080/v1/jobs/1")
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
            .get("http://schema.primary:8080/v1/jobs/1")
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
        assert_debug_snapshot!(schema_conn
            .execute_batch("create table test2 (c NOT NULL); insert into test2 select * from test")
            .await
            .unwrap_err());

        loop {
            let resp = client
                .get("http://schema.primary:8080/v1/jobs/2")
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
            .get("http://schema.primary:8080/v1/jobs/2")
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

#[test]
fn perform_data_migration() {
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
        let schema_version_before = get_schema_version("schema").await;
        let schema_conn = schema_db.connect().unwrap();
        schema_conn
            .execute("create table test (c)", ())
            .await
            .unwrap();
        schema_conn
            .execute("insert into test values (42)", ())
            .await
            .unwrap();

        while get_schema_version("schema").await == schema_version_before {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let get_row = |ns: String| async move {
            let db = Database::open_remote_with_connector(
                format!("http://{ns}.primary:8080"),
                String::new(),
                TurmoilConnector,
            )
            .unwrap();
            let conn = db.connect().unwrap();
            let mut rows = conn.query("SELECT * from test", ()).await.unwrap();
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
        };

        assert_all_eq!(
            42,
            get_row("ns1".to_string()).await,
            get_row("ns2".to_string()).await,
            get_row("schema".to_string()).await
        );

        // new db added to the linked schema also should have the data
        client
            .post(
                "http://primary:9090/v1/namespaces/ns3/create",
                json!({"shared_schema_name": "schema" }),
            )
            .await
            .unwrap();
        assert_eq!(42, get_row("ns3".to_string()).await);

        // new insertions in the schema db should be propagated to all
        schema_conn
            .execute("insert into test values (43)", ())
            .await
            .unwrap();
        assert_debug_snapshot!(check_data("ns1").await);
        assert_debug_snapshot!(check_data("ns2").await);
        assert_debug_snapshot!(check_data("ns3").await);
        assert_debug_snapshot!(check_data("schema").await);

        // updates and deletes should be propagated
        schema_conn
            .execute("update test set c = 50 where c = 42", ())
            .await
            .unwrap();
        schema_conn
            .execute("delete from test where c = 43", ())
            .await
            .unwrap();

        let get_row = |ns: String| async move {
            let db = Database::open_remote_with_connector(
                format!("http://{ns}.primary:8080"),
                String::new(),
                TurmoilConnector,
            )
            .unwrap();
            let conn = db.connect().unwrap();
            let mut rows = conn.query("SELECT * from test", ()).await.unwrap();
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
        };

        assert_all_eq!(
            50,
            get_row("ns1".to_string()).await,
            get_row("ns2".to_string()).await,
            get_row("ns3".to_string()).await,
            get_row("schema".to_string()).await
        );

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn conflicting_data_migration() {
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

        let schema_conn = Database::open_remote_with_connector(
            "http://schema.primary:8080",
            String::new(),
            TurmoilConnector,
        )
        .unwrap()
        .connect()
        .unwrap();
        let schema_version_before = get_schema_version("schema").await;
        schema_conn
            .execute("create table test (c)", ())
            .await
            .unwrap();
        schema_conn
            .execute("insert into test values (42)", ())
            .await
            .unwrap();

        while get_schema_version("schema").await == schema_version_before {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert_debug_snapshot!(check_data("ns1").await);
        assert_debug_snapshot!(check_data("schema").await);

        let ns1_conn = Database::open_remote_with_connector(
            "http://ns1.primary:8080",
            String::new(),
            TurmoilConnector,
        )
        .unwrap()
        .connect()
        .unwrap();

        // insert some data and try to create a conflicting index
        ns1_conn
            .execute("insert into test values (42)", ())
            .await
            .unwrap();

        // create unique index on the column
        assert_debug_snapshot!(schema_conn
            .execute("create unique index idx on test (c)", ())
            .await
            .unwrap_err());

        loop {
            let resp = client
                .get("http://schema.primary:8080/v1/jobs/3")
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
            .get("http://schema.primary:8080/v1/jobs/3")
            .await
            .unwrap()
            .json_value()
            .await
            .unwrap();
        assert_json_snapshot!(resp);

        // query if ns1 has zero index
        let mut rows = ns1_conn
            .query("select count(*) from sqlite_schema where type='index'", ())
            .await
            .unwrap();
        let c = rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap();
        assert_eq!(c, 0);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn disable_ddl() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(100000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let (encoding_key, validation_key) = key_pair();
        let client = Client::new();
        client
            .post(
                "http://primary:9090/v1/namespaces/schema/create",
                json!({"shared_schema": true, "jwt_key": validation_key.clone()}),
            )
            .await
            .unwrap();
        client
            .post(
                "http://primary:9090/v1/namespaces/ns1/create",
                json!({"shared_schema_name": "schema", "jwt_key": validation_key.clone() }),
            )
            .await
            .unwrap();

        {
            let claims = serde_json::json!( {
                "p": {
                    "rw": {
                        "ns": ["schema", "ns1"],
                    }
                }
            });
            let token = encode(&claims, &encoding_key);
            let conn = Database::open_remote_with_connector(
                "http://ns1.primary:8080",
                token.clone(),
                TurmoilConnector,
            )
            .unwrap()
            .connect()
            .unwrap();

            assert_debug_snapshot!(conn.execute("create table test (x)", ()).await.unwrap_err());
        }

        {
            let claims = serde_json::json!( {
                "p": {
                    "ddl": {
                        "ns": ["ns1"],
                    }
                }
            });
            let token = encode(&claims, &encoding_key);
            let conn = Database::open_remote_with_connector(
                "http://ns1.primary:8080",
                token.clone(),
                TurmoilConnector,
            )
            .unwrap()
            .connect()
            .unwrap();

            assert_debug_snapshot!(conn.execute("create table test (x)", ()).await.unwrap());
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn check_migration_perms() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(100000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let (encoding_key, validation_key) = key_pair();
        let client = Client::new();
        client
            .post(
                "http://primary:9090/v1/namespaces/schema/create",
                json!({"shared_schema": true, "jwt_key": validation_key.clone()}),
            )
            .await
            .unwrap();
        {
            let claims = serde_json::json!( {
                "p": {
                    "ro": {
                        "ns": ["schema"],
                    }
                }
            });
            let token = encode(&claims, &encoding_key);
            let conn = Database::open_remote_with_connector(
                "http://schema.primary:8080",
                token.clone(),
                TurmoilConnector,
            )
            .unwrap()
            .connect()
            .unwrap();

            assert_debug_snapshot!(conn.execute("create table test (x)", ()).await.unwrap_err());
        }

        {
            let claims = serde_json::json!( {
                "p": {
                    "rw": {
                        "ns": ["schema"],
                    }
                }
            });
            let token = encode(&claims, &encoding_key);
            let conn = Database::open_remote_with_connector(
                "http://schema.primary:8080",
                token.clone(),
                TurmoilConnector,
            )
            .unwrap()
            .connect()
            .unwrap();

            assert_debug_snapshot!(conn.execute("create table test (x)", ()).await.unwrap());
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn schema_deletion() {
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

        let resp = client
            .delete("http://primary:9090/v1/namespaces/schema", json!({}))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let resp = client
            .delete("http://primary:9090/v1/namespaces/ns1", json!({}))
            .await
            .unwrap();
        assert!(resp.status().is_success());

        let resp = client
            .delete("http://primary:9090/v1/namespaces/schema", json!({}))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let resp = client
            .delete("http://primary:9090/v1/namespaces/ns2", json!({}))
            .await
            .unwrap();
        assert!(resp.status().is_success());

        let resp = client
            .delete("http://primary:9090/v1/namespaces/schema", json!({}))
            .await
            .unwrap();
        assert!(resp.status().is_success());

        let resp = client
            .get("http://primary:9090/v1/namespaces/schema/config")
            .await
            .unwrap();
        assert_debug_snapshot!(resp.body_string().await.unwrap());
        let resp = client
            .get("http://primary:9090/v1/namespaces/ns1/config")
            .await
            .unwrap();
        assert_debug_snapshot!(resp.body_string().await.unwrap());
        let resp = client
            .get("http://primary:9090/v1/namespaces/ns2/config")
            .await
            .unwrap();
        assert_debug_snapshot!(resp.body_string().await.unwrap());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn attach_in_migration_is_forbidden() {
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
                "http://primary:9090/v1/namespaces/ns/create",
                json!({"allow_attach": true }),
            )
            .await
            .unwrap();

        let conn = Database::open_remote_with_connector(
            "http://schema.primary:8080",
            String::new(),
            TurmoilConnector,
        )
        .unwrap()
        .connect()
        .unwrap();

        assert_debug_snapshot!(conn
            .execute_batch("ATTACH ns as attached; create table test (c)")
            .await
            .unwrap_err());

        Ok(())
    });

    sim.run().unwrap();
}
