use std::time::Duration;

use libsql::Database;
use serde_json::json;
use turmoil::Builder;

use crate::common::http::Client;
use crate::common::net::TurmoilConnector;

use super::make_cluster;

#[test]
fn schema_migration_basics() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    make_cluster(&mut sim, 1, true);

    sim.client("client", async {
        let http = Client::new();

        assert!(http
            .post(
                "http://primary:9090/v1/namespaces/schema/create",
                json!({ "shared_schema": true })
            )
            .await
            .unwrap()
            .status()
            .is_success());
        assert!(http
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "shared_schema_name": "schema" })
            )
            .await
            .unwrap()
            .status()
            .is_success());

        {
            let db = Database::open_remote_with_connector(
                "http://schema.primary:8080",
                "",
                TurmoilConnector,
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("create table test (x)", ()).await.unwrap();
        }

        {
            let db = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("insert into test values (42)", ())
                .await
                .unwrap();

            assert_eq!(
                conn.query("select count(*) from test", ())
                    .await
                    .unwrap()
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .get::<u64>(0)
                    .unwrap(),
                1
            );
        }

        {
            let db = Database::open_remote_with_connector(
                "http://schema.replica0:8080",
                "",
                TurmoilConnector,
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("create table test2 (x)", ()).await.unwrap();
        }

        {
            let db = Database::open_remote_with_connector(
                "http://foo.replica0:8080",
                "",
                TurmoilConnector,
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("insert into test values (42)", ())
                .await
                .unwrap();

            // The replica applies the delegated write asynchronously and the
            // client does not give read-your-writes yet, so poll until it lands.
            let mut count = 0;
            for _ in 0..100 {
                count = conn
                    .query("select count(*) from test", ())
                    .await
                    .unwrap()
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .get::<u64>(0)
                    .unwrap();
                if count == 2 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            assert_eq!(count, 2);
            assert_eq!(
                conn.query("select count(*) from test2", ())
                    .await
                    .unwrap()
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .get::<u64>(0)
                    .unwrap(),
                0
            );
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn schema_migration_via_replica() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    make_cluster(&mut sim, 1, true);

    sim.client("client", async {
        let http = Client::new();

        assert!(http
            .post(
                "http://primary:9090/v1/namespaces/schema/create",
                json!({ "shared_schema": true })
            )
            .await
            .unwrap()
            .status()
            .is_success());
        assert!(http
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({ "shared_schema_name": "schema" })
            )
            .await
            .unwrap()
            .status()
            .is_success());

        {
            let db = Database::open_remote_with_connector(
                "http://schema.primary:8080",
                "",
                TurmoilConnector,
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("create table test (x)", ()).await.unwrap();
        }

        {
            let db = Database::open_remote_with_connector(
                "http://schema.replica0:8080",
                "",
                TurmoilConnector,
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("select * from sqlite_master;", ())
                .await
                .unwrap();

            conn.execute("create table foo (x)", ()).await.unwrap();
        }

        Ok(())
    });

    sim.run().unwrap();
}
