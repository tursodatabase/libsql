use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use libsql::Database;
use libsql_server::config::{AdminApiConfig, RpcClientConfig, RpcServerConfig, UserApiConfig};
use tempfile::tempdir;
use tokio::sync::Notify;
use turmoil::Builder;

use crate::common::{
    http::Client,
    net::{init_tracing, SimServer, TestServer, TurmoilAcceptor, TurmoilConnector},
};

/// In this test, we create a primary and a replica, add some data and sync them. when then shut
/// down and bring back up the replica, and ensure the the replica continue normal mode of
/// operation.
#[test]
fn replica_restart() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
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
                ..Default::default()
            };

            server.start_sim(8080).await?;

            Ok(())
        }
    });

    let notify = Arc::new(Notify::new());
    let tmp = tempdir().unwrap();
    let notify_clone = notify.clone();
    sim.host("replica", move || {
        let path = tmp.path().to_path_buf();
        let notify = notify_clone.clone();
        async move {
            let make_server = || {
                let path = path.clone();
                async {
                    TestServer {
                        path: path.into(),
                        user_api_config: UserApiConfig {
                            ..Default::default()
                        },
                        admin_api_config: Some(AdminApiConfig {
                            acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await.unwrap(),
                            connector: TurmoilConnector,
                            disable_metrics: true,
                        }),
                        rpc_client_config: Some(RpcClientConfig {
                            remote_url: "http://primary:4567".into(),
                            connector: TurmoilConnector,
                            tls_config: None,
                        }),
                        ..Default::default()
                    }
                }
            };

            let server = make_server().await;

            tokio::select! {
                res = server.start_sim(8080) => {
                    res.unwrap()
                }
                _ = notify.notified() => (),
            }

            let server = make_server().await;
            server.start_sim(8080).await.unwrap();

            Ok(())
        }
    });

    sim.client("client", async move {
        let http = Client::new();
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        // insert a few valued into the primary
        conn.execute("create table test (x)", ()).await.unwrap();
        for _ in 0..50 {
            conn.execute("insert into test values (42)", ())
                .await
                .unwrap();
        }

        let primary_index = http
            .get("http://primary:9090/v1/namespaces/default/stats")
            .await
            .unwrap()
            .json_value()
            .await
            .unwrap()["replication_index"]
            .clone()
            .as_i64();

        loop {
            let replica_index = http
                .get("http://primary:9090/v1/namespaces/default/stats")
                .await
                .unwrap()
                .json_value()
                .await
                .unwrap()["replication_index"]
                .clone()
                .as_i64();
            if primary_index == replica_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        notify.notify_waiters();

        // make sure that replica is up to date
        loop {
            let replica_index = http
                .get("http://primary:9090/v1/namespaces/default/stats")
                .await
                .unwrap()
                .json_value()
                .await
                .unwrap()["replication_index"]
                .clone()
                .as_i64();
            if primary_index == replica_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// In this test, we start a primary and a replica. We add some entries to the primary, and wait
/// for the replica to be up to date. Then we stop the primary, remove it's wallog, and restart the
/// primary. This will force the primary to regenerate the log. The replica should catch that, and
/// self heal. During this process the replica is not shutdown.
#[test]
fn primary_regenerate_log_no_replica_restart() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();

    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();

    init_tracing();
    sim.host("primary", move || {
        let notify = notify_clone.clone();
        let path = tmp.path().to_path_buf();
        async move {
            let make_server = || async {
                TestServer {
                    path: path.clone().into(),
                    user_api_config: UserApiConfig {
                        ..Default::default()
                    },
                    admin_api_config: Some(AdminApiConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await.unwrap(),
                        connector: TurmoilConnector,
                        disable_metrics: true,
                    }),
                    rpc_server_config: Some(RpcServerConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await.unwrap(),
                        tls_config: None,
                    }),
                    ..Default::default()
                }
            };
            let server = make_server().await;
            let shutdown = server.shutdown.clone();

            let fut = async move { server.start_sim(8080).await };

            tokio::pin!(fut);

            loop {
                tokio::select! {
                    res =  &mut fut => {
                        res.unwrap();
                        break
                    }
                    _ = notify.notified() => {
                        shutdown.notify_waiters();
                    },
                }
            }
            // remove the wallog and start again
            tokio::fs::remove_file(path.join("dbs/default/wallog"))
                .await
                .unwrap();
            notify.notify_waiters();
            let server = make_server().await;
            server.start_sim(8080).await.unwrap();

            Ok(())
        }
    });

    let tmp = tempdir().unwrap();
    sim.host("replica", move || {
        let path = tmp.path().to_path_buf();
        async move {
            let make_server = || {
                let path = path.clone();
                async {
                    TestServer {
                        path: path.into(),
                        user_api_config: UserApiConfig {
                            ..Default::default()
                        },
                        admin_api_config: Some(AdminApiConfig {
                            acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await.unwrap(),
                            connector: TurmoilConnector,
                            disable_metrics: true,
                        }),
                        rpc_client_config: Some(RpcClientConfig {
                            remote_url: "http://primary:4567".into(),
                            connector: TurmoilConnector,
                            tls_config: None,
                        }),
                        ..Default::default()
                    }
                }
            };

            let server = make_server().await;
            server.start_sim(8080).await.unwrap();

            Ok(())
        }
    });

    sim.client("client", async move {
        let http = Client::new();
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        // insert a few valued into the primary
        conn.execute("create table test (x)", ()).await.unwrap();
        for _ in 0..50 {
            conn.execute("insert into test values (42)", ())
                .await
                .unwrap();
        }

        let primary_index = http
            .get("http://primary:9090/v1/namespaces/default/stats")
            .await
            .unwrap()
            .json_value()
            .await
            .unwrap()["replication_index"]
            .clone()
            .as_i64();

        loop {
            let replica_index = http
                .get("http://primary:9090/v1/namespaces/default/stats")
                .await
                .unwrap()
                .json_value()
                .await
                .unwrap()["replication_index"]
                .clone()
                .as_i64();
            if primary_index == replica_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        notify.notify_waiters();
        notify.notified().await;

        drop(http);
        let http = Client::new();
        // make sure that replica is up to date
        let new_primary_index = http
            .get("http://primary:9090/v1/namespaces/default/stats")
            .await
            .unwrap()
            .json_value()
            .await
            .unwrap()["replication_index"]
            .clone()
            .as_i64();
        assert_ne!(primary_index, new_primary_index);
        loop {
            let replica_index = http
                .get("http://primary:9090/v1/namespaces/default/stats")
                .await
                .unwrap()
                .json_value()
                .await
                .unwrap()["replication_index"]
                .clone()
                .as_i64();
            if new_primary_index == replica_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// This test is very similar to `primary_regenerate_log_no_replica_restart`. The only difference
/// is that the replica is being shutdown before the primary regenerates their log. When the
/// replica is brought back up, it will try to load the namespace from a primary with a new log,
/// and it should self heal.
#[test]
fn primary_regenerate_log_with_replica_restart() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();

    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();

    init_tracing();
    sim.host("primary", move || {
        let notify = notify_clone.clone();
        let path = tmp.path().to_path_buf();
        async move {
            let make_server = || async {
                TestServer {
                    path: path.clone().into(),
                    user_api_config: UserApiConfig {
                        ..Default::default()
                    },
                    admin_api_config: Some(AdminApiConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await.unwrap(),
                        connector: TurmoilConnector,
                        disable_metrics: true,
                    }),
                    rpc_server_config: Some(RpcServerConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await.unwrap(),
                        tls_config: None,
                    }),
                    ..Default::default()
                }
            };
            let server = make_server().await;
            let shutdown = server.shutdown.clone();

            let fut = async move { server.start_sim(8080).await };

            tokio::pin!(fut);

            loop {
                tokio::select! {
                    res =  &mut fut => {
                        res.unwrap();
                        break
                    }
                    _ = notify.notified() => {
                        shutdown.notify_waiters();
                    },
                }
            }
            // remove the wallog and start again
            tokio::fs::remove_file(path.join("dbs/default/wallog"))
                .await
                .unwrap();
            notify.notify_waiters();
            let server = make_server().await;
            server.start_sim(8080).await.unwrap();

            Ok(())
        }
    });

    let tmp = tempdir().unwrap();
    let notify_clone = notify.clone();
    sim.host("replica", move || {
        let path = tmp.path().to_path_buf();
        let notify = notify_clone.clone();
        async move {
            let make_server = || {
                let path = path.clone();
                async {
                    TestServer {
                        path: path.into(),
                        user_api_config: UserApiConfig {
                            ..Default::default()
                        },
                        admin_api_config: Some(AdminApiConfig {
                            acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await.unwrap(),
                            connector: TurmoilConnector,
                            disable_metrics: true,
                        }),
                        rpc_client_config: Some(RpcClientConfig {
                            remote_url: "http://primary:4567".into(),
                            connector: TurmoilConnector,
                            tls_config: None,
                        }),
                        ..Default::default()
                    }
                }
            };

            let server = make_server().await;
            let shutdown = server.shutdown.clone();
            let fut = async {
                server.start_sim(8080).await.unwrap();
            };

            tokio::pin!(fut);
            let notify_fut = async {
                notify.notified().await;
            }
            .fuse();
            tokio::pin!(notify_fut);
            loop {
                tokio::select! {
                    _ = &mut fut => break,
                    _ = &mut notify_fut => {
                        shutdown.notify_waiters();
                    }
                }
            }

            // we wait for the server to have restarted
            notify.notified().await;

            // and then restart the replica
            let server = make_server().await;
            server.start_sim(8080).await.unwrap();

            Ok(())
        }
    });

    sim.client("client", async move {
        let http = Client::new();
        let db = Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;

        // insert a few valued into the primary
        conn.execute("create table test (x)", ()).await.unwrap();
        for _ in 0..50 {
            conn.execute("insert into test values (42)", ())
                .await
                .unwrap();
        }

        let primary_index = http
            .get("http://primary:9090/v1/namespaces/default/stats")
            .await
            .unwrap()
            .json_value()
            .await
            .unwrap()["replication_index"]
            .clone()
            .as_i64();

        loop {
            let replica_index = http
                .get("http://primary:9090/v1/namespaces/default/stats")
                .await
                .unwrap()
                .json_value()
                .await
                .unwrap()["replication_index"]
                .clone()
                .as_i64();
            if primary_index == replica_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        notify.notify_waiters();
        notify.notified().await;

        drop(http);
        let http = Client::new();
        // make sure that replica is up to date
        let new_primary_index = http
            .get("http://primary:9090/v1/namespaces/default/stats")
            .await
            .unwrap()
            .json_value()
            .await
            .unwrap()["replication_index"]
            .clone()
            .as_i64();
        assert_ne!(primary_index, new_primary_index);
        loop {
            let replica_index = http
                .get("http://primary:9090/v1/namespaces/default/stats")
                .await
                .unwrap()
                .json_value()
                .await
                .unwrap()["replication_index"]
                .clone()
                .as_i64();
            if new_primary_index == replica_index {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(())
    });

    sim.run().unwrap();
}
