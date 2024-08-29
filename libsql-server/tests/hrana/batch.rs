use std::time::Duration;

use futures::StreamExt;
use insta::assert_json_snapshot;
use libsql::{params, Database};
use libsql_server::hrana_proto::{Batch, BatchStep, Stmt};

use crate::common::http::Client;
use crate::common::net::TurmoilConnector;

#[test]
fn sample_request() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let batch = Batch {
            steps: vec![BatchStep {
                condition: None,
                stmt: Stmt {
                    sql: Some("create table test (x)".to_string()),
                    ..Default::default()
                },
            }],
            replication_index: None,
        };
        let client = Client::new();

        let resp = client
            .post(
                "http://primary:8080/v1/batch",
                serde_json::json!({ "batch": batch }),
            )
            .await
            .unwrap();

        let mut json = resp.json_value().await.unwrap();

        for result in json["result"]["step_results"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
        {
            result
                .as_object_mut()
                .unwrap()
                .remove("query_duration_ms")
                .expect("expected query_duration_ms");
        }

        assert_json_snapshot!(json);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn execute_individual_statements() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table t(x text)", ()).await?;
        conn.execute("insert into t(x) values(?)", params!["hello"])
            .await?;
        let mut rows = conn
            .query("select * from t where x = ?", params!["hello"])
            .await?;

        assert_eq!(rows.column_count(), 1);
        assert_eq!(rows.column_name(0), Some("x"));
        assert_eq!(rows.next().await?.unwrap().get::<String>(0)?, "hello");
        assert!(rows.next().await?.is_none());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn execute_batch() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute_batch(
            r#"
        begin;
        create table t(x text);
        insert into t(x) values('hello; world');
        end;"#,
        )
        .await?;
        let mut rows = conn
            .query("select * from t where x = ?", params!["hello; world"])
            .await?;

        assert_eq!(rows.column_count(), 1);
        assert_eq!(rows.column_name(0), Some("x"));
        assert_eq!(
            rows.next().await?.unwrap().get::<String>(0)?,
            "hello; world"
        );
        assert!(rows.next().await?.is_none());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn execute_batch_returning() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        let mut batch_rows = conn
            .execute_transactional_batch(
                r#"
        create table t(x text);
        insert into t(x) values('hello; world') RETURNING *;
        "#,
            )
            .await?;

        batch_rows.next_stmt_row();

        let mut rows = batch_rows.next_stmt_row().unwrap().unwrap();

        assert!(batch_rows.next_stmt_row().is_none());

        assert_eq!(rows.column_count(), 1);
        assert_eq!(rows.column_name(0), Some("x"));
        assert_eq!(
            rows.next().await?.unwrap().get::<String>(0)?,
            "hello; world"
        );
        assert!(rows.next().await?.is_none());

        let mut batch_rows = conn
            .execute_batch(
                r#"
        create table t2(x text);
        insert into t2(x) values('hello; world') RETURNING *;
        "#,
            )
            .await?;

        batch_rows.next_stmt_row();

        let mut rows = batch_rows.next_stmt_row().unwrap().unwrap();

        assert!(batch_rows.next_stmt_row().is_none());

        assert_eq!(rows.column_count(), 1);
        assert_eq!(rows.column_name(0), Some("x"));
        assert_eq!(
            rows.next().await?.unwrap().get::<String>(0)?,
            "hello; world"
        );
        assert!(rows.next().await?.is_none());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn multistatement_query() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;
        let mut rows = conn
            .query("select 1 + ?; select 'abc';", params![1])
            .await?;

        assert_eq!(rows.column_count(), 1);
        assert_eq!(rows.next().await?.unwrap().get::<i32>(0)?, 2);
        assert!(rows.next().await?.is_none());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn affected_rows_and_last_rowid() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute(
            "create table t(id integer primary key autoincrement, x text);",
            (),
        )
        .await?;

        let r = conn.execute("insert into t(x) values('a');", ()).await?;
        assert_eq!(r, 1, "1st row inserted");
        assert_eq!(conn.last_insert_rowid(), 1, "1st row id");

        let r = conn
            .execute("insert into t(x) values('b'),('c');", ())
            .await?;
        assert_eq!(r, 2, "2nd and 3rd rows inserted");
        assert_eq!(conn.last_insert_rowid(), 3, "3rd row id");

        let r = conn.execute("update t set x = 'd';", ()).await?;
        assert_eq!(r, 3, "all three rows updated");
        assert_eq!(conn.last_insert_rowid(), 3, "last row id unchanged");

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn stats() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let req = serde_json::json!({
            "requests": [
                {"type": "execute", "stmt": { "sql": "CREATE TABLE foo (x INT)" }},
                {"type": "execute", "stmt": { "sql": "INSERT INTO foo VALUES (42)"}},
                {"type": "execute", "stmt": { "sql": "SELECT * FROM foo"}}
            ]
        });
        let client = Client::new();

        let resp = client
            .post("http://primary:8080/v2/pipeline", req)
            .await
            .unwrap();

        let mut json = resp.json_value().await.unwrap();
        json.as_object_mut().unwrap().remove("baton");

        for results in json["results"].as_array_mut().unwrap().iter_mut() {
            results["response"]["result"]
                .as_object_mut()
                .unwrap()
                .remove("query_duration_ms")
                .expect("expected query_duration_ms");
        }

        assert_json_snapshot!(json);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn stats_legacy() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let req = serde_json::json!({
            "statements": [
                "CREATE TABLE foo (x INT)",
                "INSERT INTO foo VALUES (42)",
                "SELECT * FROM foo"
            ]
        });
        let client = Client::new();

        let resp = client.post("http://primary:8080/", req).await.unwrap();

        let mut json = resp.json_value().await.unwrap();

        for result in json.as_array_mut().unwrap() {
            result["results"]
                .as_object_mut()
                .unwrap()
                .remove("query_duration_ms")
                .expect("expected query_duration_ms");
        }

        assert_json_snapshot!(json);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn stream() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table t(x text)", ()).await?;
        conn.execute("insert into t(x) values(?)", params!["hello"])
            .await?;

        conn.execute("insert into t(x) values(?)", params!["hello"])
            .await?;

        conn.execute("insert into t(x) values(?)", params!["hello"])
            .await?;

        conn.execute("insert into t(x) values(?)", params!["hello"])
            .await?;

        let rows = conn
            .query("select * from t where x = ?", params!["hello"])
            .await?
            .into_stream();

        let rows = rows.collect::<Vec<_>>().await;

        assert_eq!(rows.len(), 4);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn reindex_statement() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table t(x text)", ()).await?;
        conn.execute("create index t_idx on t(x)", ()).await?;
        conn.execute("insert into t(x) values(?)", params!["hello"])
            .await?;
        conn.execute("insert into t(x) values(?)", params!["hello"])
            .await?;
        conn.execute("reindex t_idx", ()).await?;

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn test_simulate_vector_index_load_from_dump() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    sim.host("primary", super::make_standalone_server);
    sim.client("client", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("CREATE TABLE t ( v FLOAT32(2) );", ()).await?;
        conn.execute("CREATE TABLE t_idx_shadow(index_key  INTEGER , data BLOB, PRIMARY KEY (index_key));", ()).await?;
        conn.execute("CREATE TABLE libsql_vector_meta_shadow ( name TEXT PRIMARY KEY, metadata BLOB ) WITHOUT ROWID", ()).await?;
        conn.execute("INSERT INTO libsql_vector_meta_shadow VALUES ('t_idx', x'');", ()).await?;
        conn.execute("INSERT INTO t VALUES (vector('[1,2]')), (vector('[2,3]'));", ()).await?;
        conn.execute("CREATE INDEX t_idx ON t (libsql_vector_idx(v));", ()).await?;

        Ok(())
    });

    sim.run().unwrap();
}
