use insta::assert_json_snapshot;
use sqld::hrana_proto::{Batch, BatchStep, Stmt};

use crate::common::http::Client;

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
