use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::common::http::Client;
use crate::common::net::{init_tracing, SimServer, TestServer, TurmoilAcceptor, TurmoilConnector};
use libsql::Database;
use libsql_server::config::{AdminApiConfig, RpcServerConfig, UserApiConfig};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tempfile::tempdir;
use tokio::sync::Notify;
use turmoil::Builder;

#[test]
fn replica_primary_reset2() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(u64::MAX))
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

    let mut rng = SmallRng::from_entropy();

    let done = Arc::new(AtomicBool::new(false));
    let done_clone = done.clone();

    sim.client("client", async move {
        let primary =
            Database::open_remote_with_connector("http://primary:8080", "", TurmoilConnector)?;
        let mut conn = primary.connect()?;

        conn.execute_batch(
            "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB(16), c BLOB(16), d BLOB(400));
             CREATE INDEX i1 ON t1(b);
             CREATE INDEX i2 ON t1(c);",
        )
        .await
        .unwrap();

        let max = 500u64;

        for i in 0..max {
            conn.execute(
                "insert into t1 values (?1, randomblob(16), randomblob(16), randomblob(400))",
                libsql::params![i + 1],
            )
            .await
            .unwrap();
        }

        let mut restarted = false;

        for i in 0..100 {
            if !restarted && (rng.gen::<usize>() % 100) == i {
                notify.notify_waiters();
                notify.notified().await;

                // Replace connection to force a reconnect
                let primary = Database::open_remote_with_connector(
                    "http://primary:8080",
                    "",
                    TurmoilConnector,
                )?;
                let conn2 = primary.connect()?;
                let _ = std::mem::replace(&mut conn, conn2);

                restarted = true;
            }

            let txn = conn.transaction().await.unwrap();

            for _ in 0..6 {
                txn.execute(
                    "REPLACE INTO t1 VALUES(?1, randomblob(16), randomblob(16), randomblob(400))",
                    [(rng.gen::<u64>() % max)],
                )
                .await
                .unwrap();
            }

            txn.commit().await.unwrap();
        }

        done.store(true, Ordering::Relaxed);

        Ok(())
    });

    sim.client("client2", async move {
        let tmp = tempdir().unwrap();
        let replica = Database::open_with_remote_sync_connector(
            tmp.path().join("data").display().to_string(),
            "http://primary:8080",
            "",
            TurmoilConnector,
            false,
            None,
        )
        .await
        .unwrap();

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            replica.sync().await.unwrap();

            if done_clone.load(Ordering::Relaxed) {
                replica.sync().await.unwrap();
                break;
            }
        }

        Ok(())
    });

    // let tmp = tempdir().unwrap();
    // let replica = Database::open_with_remote_sync_connector(
    //     tmp.path().join("data").display().to_string(),
    //     "http://primary:8080",
    //     "",
    //     TurmoilConnector,
    //     false,
    //     None,
    // )
    // .await
    // .unwrap();
    // let replica_index = replica.sync().await.unwrap().unwrap();
    // let primary_index = Client::new()
    //     .get("http://primary:9090/v1/namespaces/default/stats")
    //     .await
    //     .unwrap()
    //     .json_value()
    //     .await
    //     .unwrap()["replication_index"]
    //     .clone()
    //     .as_u64()
    //     .unwrap();

    // assert_eq!(replica_index, primary_index);

    // let replica_count = *replica
    //     .connect()
    //     .unwrap()
    //     .query("select count(*) from test", ())
    //     .await
    //     .unwrap()
    //     .next()
    //     .await
    //     .unwrap()
    //     .unwrap()
    //     .get_value(0)
    //     .unwrap()
    //     .as_integer()
    //     .unwrap();
    // let primary_count = *primary
    //     .connect()
    //     .unwrap()
    //     .query("select count(*) from test", ())
    //     .await
    //     .unwrap()
    //     .next()
    //     .await
    //     .unwrap()
    //     .unwrap()
    //     .get_value(0)
    //     .unwrap()
    //     .as_integer()
    //     .unwrap();
    // assert_eq!(primary_count, replica_count);

    // notify.notify_waiters();
    // notify.notified().await;

    // // drop the replica here, to make sure not to reuse an open connection.
    // drop(replica);
    // let replica = Database::open_with_remote_sync_connector(
    //     tmp.path().join("data").display().to_string(),
    //     "http://primary:8080",
    //     "",
    //     TurmoilConnector,
    //     false,
    //     None,
    // )
    // .await
    // .unwrap();
    // let replica_index = replica.sync().await.unwrap().unwrap();
    // let primary_index = Client::new()
    //     .get("http://primary:9090/v1/namespaces/default/stats")
    //     .await
    //     .unwrap()
    //     .json_value()
    //     .await
    //     .unwrap()["replication_index"]
    //     .clone()
    //     .as_u64()
    //     .unwrap();

    // assert_eq!(replica_index, primary_index);

    // let replica_count = *replica
    //     .connect()
    //     .unwrap()
    //     .query("select count(*) from test", ())
    //     .await
    //     .unwrap()
    //     .next()
    //     .await
    //     .unwrap()
    //     .unwrap()
    //     .get_value(0)
    //     .unwrap()
    //     .as_integer()
    //     .unwrap();
    // let primary_count = *primary
    //     .connect()
    //     .unwrap()
    //     .query("select count(*) from test", ())
    //     .await
    //     .unwrap()
    //     .next()
    //     .await
    //     .unwrap()
    //     .unwrap()
    //     .get_value(0)
    //     .unwrap()
    //     .as_integer()
    //     .unwrap();
    // assert_eq!(primary_count, replica_count);

    // Ok(())
    // });

    sim.run().unwrap();
}
