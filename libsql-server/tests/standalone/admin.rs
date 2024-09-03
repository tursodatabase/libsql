use std::time::Duration;

use hyper::StatusCode;
use libsql_server::config::{AdminApiConfig, UserApiConfig};
use s3s::header::AUTHORIZATION;
use serde_json::json;
use tempfile::tempdir;

use crate::common::{
    http::Client,
    net::{SimServer as _, TestServer, TurmoilAcceptor, TurmoilConnector},
};

#[test]
fn admin_auth() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", || async move {
        let tmp = tempdir().unwrap();
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
                auth_key: Some("secretkey".into()),
            }),
            disable_namespaces: false,
            ..Default::default()
        };
        server.start_sim(8080).await?;
        Ok(())
    });

    sim.client("test", async {
        let client = Client::new();

        assert_eq!(
            client
                .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert!(client
            .post_with_headers(
                "http://primary:9090/v1/namespaces/foo/create",
                &[(AUTHORIZATION, "basic  secretkey")],
                json!({})
            )
            .await
            .unwrap()
            .status()
            .is_success());

        Ok(())
    });

    sim.run().unwrap();
}
