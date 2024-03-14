//! Tests for sqld in cluster mode
#![allow(deprecated)]

use super::common;

use insta::assert_snapshot;
use libsql::{Database, Value};
use libsql_server::config::{AdminApiConfig, RpcClientConfig, RpcServerConfig, UserApiConfig};
use serde_json::json;
use tempfile::tempdir;
use tokio::{task::JoinSet, time::Duration};
use turmoil::{Builder, Sim};

use common::net::{init_tracing, TestServer, TurmoilAcceptor, TurmoilConnector};

use crate::common::{http::Client, net::SimServer, snapshot_metrics};

mod replica_restart;
mod replication;

pub fn make_cluster(sim: &mut Sim, num_replica: usize, disable_namespaces: bool) {
    init_tracing();
    let tmp = tempdir().unwrap();
    sim.host("primary", move || {
        let path = tmp.path().to_path_buf();
        async move {
            let server = TestServer {
                path: path.into(),
                user_api_config: UserApiConfig {
                    ..Default::default()
                },
                admin_api_config: Some(AdminApiConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                    connector: TurmoilConnector,
                    disable_metrics: true,
                }),
                rpc_server_config: Some(RpcServerConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await?,
                    tls_config: None,
                }),
                disable_namespaces,
                disable_default_namespace: !disable_namespaces,
                ..Default::default()
            };

            server.start_sim(8080).await?;

            Ok(())
        }
    });

    for i in 0..num_replica {
        let tmp = tempdir().unwrap();
        sim.host(format!("replica{i}"), move || {
            let path = tmp.path().to_path_buf();
            async move {
                let server = TestServer {
                    path: path.into(),
                    user_api_config: UserApiConfig {
                        ..Default::default()
                    },
                    admin_api_config: Some(AdminApiConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                        connector: TurmoilConnector,
                        disable_metrics: true,
                    }),
                    rpc_client_config: Some(RpcClientConfig {
                        remote_url: "http://primary:4567".into(),
                        connector: TurmoilConnector,
                        tls_config: None,
                    }),
                    disable_namespaces,
                    disable_default_namespace: !disable_namespaces,
                    ..Default::default()
                };

                server.start_sim(8080).await.unwrap();

                Ok(())
            }
        });
    }
}

#[test]
fn proxy_write() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    make_cluster(&mut sim, 1, true);

    sim.client("client", async {
        let db =
            Database::open_remote_with_connector("http://replica0:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;
        conn.execute("insert into test values (12)", ()).await?;

        // assert that the primary got the write
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;
        let mut rows = conn.query("select count(*) from test", ()).await?;

        assert!(matches!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            Value::Integer(1)
        ));

        snapshot_metrics().assert_gauge("libsql_server_current_frame_no", 2.0);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
#[ignore = "libsql client doesn't reuse the stream yet, so we can't do RYW"]
fn replica_read_write() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    make_cluster(&mut sim, 1, true);

    sim.client("client", async {
        let db =
            Database::open_remote_with_connector("http://replica0:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;
        conn.execute("insert into test values (12)", ()).await?;
        let mut rows = conn.query("select count(*) from test", ()).await?;

        assert!(matches!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            Value::Integer(1)
        ));

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn sync_many_replica() {
    const NUM_REPLICA: usize = 10;
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    make_cluster(&mut sim, NUM_REPLICA, true);
    sim.client("client", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        conn.execute("create table test (x)", ()).await?;
        conn.execute("insert into test values (42)", ()).await?;

        async fn get_frame_no(url: &str) -> Option<u64> {
            let client = Client::new();
            Some(
                client
                    .get(url)
                    .await
                    .unwrap()
                    .json::<serde_json::Value>()
                    .await
                    .unwrap()
                    .get("replication_index")?
                    .as_u64()
                    .unwrap(),
            )
        }

        let primary_fno = loop {
            if let Some(fno) = get_frame_no("http://primary:9090/v1/namespaces/default/stats").await
            {
                break fno;
            }
        };

        // wait for all replicas to sync
        let mut join_set = JoinSet::new();
        for i in 0..NUM_REPLICA {
            join_set.spawn(async move {
                let uri = format!("http://replica{i}:9090/v1/namespaces/default/stats");
                loop {
                    if let Some(replica_fno) = get_frame_no(&uri).await {
                        if replica_fno == primary_fno {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
        }

        while join_set.join_next().await.is_some() {}

        for i in 0..NUM_REPLICA {
            let db = Database::open_remote_with_connector(
                format!("http://replica{i}:8080"),
                "",
                TurmoilConnector,
            )?;
            let conn = db.connect()?;
            let mut rows = conn.query("select count(*) from test", ()).await?;
            assert!(matches!(
                rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
                Value::Integer(1)
            ));
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn create_namespace() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    make_cluster(&mut sim, 0, false);

    sim.client("client", async {
        let db =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        let Err(e) = conn.execute("create table test (x)", ()).await else {
            panic!()
        };
        assert_snapshot!(e.to_string());

        let client = Client::new();
        let resp = client
            .post(
                "http://foo.primary:9090/v1/namespaces/foo/create",
                json!({}),
            )
            .await?;

        assert_eq!(resp.status(), 200);

        conn.execute("create table test (x)", ()).await.unwrap();
        let mut rows = conn.query("select count(*) from test", ()).await.unwrap();
        assert!(matches!(
            rows.next().await.unwrap().unwrap().get_value(0).unwrap(),
            Value::Integer(0)
        ));

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn large_proxy_query() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(10000))
        .tcp_capacity(100000)
        .build();
    make_cluster(&mut sim, 1, true);

    sim.client("client", async {
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)
            .unwrap();
        let conn = db.connect().unwrap();

        conn.execute("create table test (x)", ()).await.unwrap();
        for _ in 0..5000 {
            conn.execute("insert into test values (randomblob(1000))", ())
                .await
                .unwrap();
        }

        let db = Database::open_remote_with_connector("http://replica0:8080", "", TurmoilConnector)
            .unwrap();
        let conn = db.connect().unwrap();

        conn.execute_batch("begin immediate; select * from test limit (4000)")
            .await
            .unwrap();

        Ok(())
    });

    sim.run().unwrap();
}
