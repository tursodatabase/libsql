use insta::assert_json_snapshot;
use libsql::{params, Database};
use sqld::hrana_proto::{Batch, BatchStep, Stmt};

use crate::common::http::Client;
use crate::common::net::TurmoilConnector;

#[test]
fn sample_request() {
    let mut sim = turmoil::Builder::new().build();
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
        assert_json_snapshot!(resp.json_value().await.unwrap());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn execute_individual_statements() {
    let mut sim = turmoil::Builder::new().build();
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
        assert_eq!(rows.next()?.unwrap().get::<String>(0)?, "hello");
        assert!(rows.next()?.is_none());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn execute_batch() {
    let mut sim = turmoil::Builder::new().build();
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
        assert_eq!(rows.next()?.unwrap().get::<String>(0)?, "hello; world");
        assert!(rows.next()?.is_none());

        Ok(())
    });

    sim.run().unwrap();
}
