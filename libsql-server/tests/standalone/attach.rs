use std::time::Duration;

use insta::assert_debug_snapshot;
use libsql::Database;
use uuid::Uuid;

use crate::common::auth::{encode, key_pair};
use crate::common::http::Client;
use crate::common::net::TurmoilConnector;

use super::make_standalone_server;

#[test]
fn attach_no_auth() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let client = Client::new();

        client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                serde_json::json!({}),
            )
            .await
            .unwrap();
        client
            .post(
                "http://primary:9090/v1/namespaces/bar/create",
                serde_json::json!({ "allow_attach": true}),
            )
            .await
            .unwrap();

        let foo_db =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let foo_conn = foo_db.connect().unwrap();
        foo_conn
            .execute("CREATE TABLE foo_table (x)", ())
            .await
            .unwrap();
        foo_conn
            .execute("insert into foo_table values (42)", ())
            .await
            .unwrap();

        let bar_db =
            Database::open_remote_with_connector("http://bar.primary:8080", "", TurmoilConnector)?;
        let bar_conn = bar_db.connect().unwrap();
        bar_conn
            .execute("CREATE TABLE bar_table (x)", ())
            .await
            .unwrap();
        bar_conn
            .execute("insert into bar_table values (43)", ())
            .await
            .unwrap();

        // fails: foo doesn't allow attach
        assert_debug_snapshot!(bar_conn.execute("ATTACH foo as foo", ()).await.unwrap_err());

        let txn = foo_conn.transaction().await.unwrap();
        txn.execute("ATTACH DATABASE bar as bar", ()).await.unwrap();
        let mut rows = txn.query("SELECT * FROM bar.bar_table", ()).await.unwrap();
        // succeeds!
        assert_debug_snapshot!(rows.next().await);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn attach_auth() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let client = Client::new();

        let (enc, jwt_key) = key_pair();

        assert!(client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                serde_json::json!({ "jwt_key": jwt_key })
            )
            .await
            .unwrap()
            .status()
            .is_success());
        assert!(client
            .post(
                "http://primary:9090/v1/namespaces/bar/create",
                serde_json::json!({ "allow_attach": true, "jwt_key": jwt_key })
            )
            .await
            .unwrap()
            .status()
            .is_success());

        let claims = serde_json::json!({
            "p": {
                "rw": {
                    "ns": ["bar", "foo"]
                }
            }
        });
        let token = encode(&claims, &enc);

        let foo_db = Database::open_remote_with_connector(
            "http://foo.primary:8080",
            &token,
            TurmoilConnector,
        )?;
        let foo_conn = foo_db.connect().unwrap();
        foo_conn
            .execute("CREATE TABLE foo_table (x)", ())
            .await
            .unwrap();
        foo_conn
            .execute("insert into foo_table values (42)", ())
            .await
            .unwrap();

        let bar_db = Database::open_remote_with_connector(
            "http://bar.primary:8080",
            &token,
            TurmoilConnector,
        )?;
        let bar_conn = bar_db.connect().unwrap();
        bar_conn
            .execute("CREATE TABLE bar_table (x)", ())
            .await
            .unwrap();
        bar_conn
            .execute("insert into bar_table values (43)", ())
            .await
            .unwrap();

        // fails: no perm
        assert_debug_snapshot!(bar_conn.execute("ATTACH foo as foo", ()).await.unwrap_err());

        let txn = foo_conn.transaction().await.unwrap();
        // fails: no perm
        assert_debug_snapshot!(txn
            .execute("ATTACH DATABASE bar as bar", ())
            .await
            .unwrap_err());

        let claims = serde_json::json!({
            "p": {
                "roa": {
                    "ns": ["bar", "foo"]
                }
            }
        });
        let token = encode(&claims, &enc);

        let foo_db = Database::open_remote_with_connector(
            "http://foo.primary:8080",
            &token,
            TurmoilConnector,
        )?;
        let foo_conn = foo_db.connect().unwrap();
        let bar_db = Database::open_remote_with_connector(
            "http://bar.primary:8080",
            &token,
            TurmoilConnector,
        )?;
        let bar_conn = bar_db.connect().unwrap();

        // fails: namesapce doesn't allow attach
        assert_debug_snapshot!(bar_conn.execute("ATTACH foo as foo", ()).await.unwrap_err());

        let txn = foo_conn.transaction().await.unwrap();
        txn.execute("ATTACH DATABASE bar as bar", ()).await.unwrap();
        let mut rows = txn.query("SELECT * FROM bar.bar_table", ()).await.unwrap();
        // succeeds!
        assert_debug_snapshot!(rows.next().await);

        // mixed claims
        let claims = serde_json::json!({
            "p": {
                "rw": {
                    "ns": ["foo"]
                },
                "roa": {
                    "ns": ["bar"]
                }
            }
        });
        let token = encode(&claims, &enc);

        let foo_db = Database::open_remote_with_connector(
            "http://foo.primary:8080",
            &token,
            TurmoilConnector,
        )?;
        let foo_conn = foo_db.connect().unwrap();
        let txn = foo_conn.transaction().await.unwrap();
        txn.execute("ATTACH DATABASE bar as attached", ())
            .await
            .unwrap();
        let mut rows = txn
            .query("SELECT * FROM attached.bar_table", ())
            .await
            .unwrap();
        // succeeds!
        assert_debug_snapshot!(rows.next().await);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn attach_auth_with_uuids() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let client = Client::new();

        let (enc, jwt_key) = key_pair();

        let main_db_id = Uuid::new_v4();
        let attach_db_id = Uuid::new_v4();

        assert!(client
            .post(
                format!("http://primary:9090/v1/namespaces/{}/create", main_db_id).as_str(),
                serde_json::json!({ "jwt_key": jwt_key })
            )
            .await
            .unwrap()
            .status()
            .is_success());
        assert!(client
            .post(
                format!("http://primary:9090/v1/namespaces/{}/create", attach_db_id).as_str(),
                serde_json::json!({ "allow_attach": true, "jwt_key": jwt_key })
            )
            .await
            .unwrap()
            .status()
            .is_success());

        let claims = serde_json::json!({
            "p": {
                "rw": {
                    "ns": [main_db_id, attach_db_id]
                },
                "roa": {
                    "ns": [attach_db_id]
                }
            }
        });
        let token = encode(&claims, &enc);

        let attach_conn = Database::open_remote_with_connector(
            format!("http://{}.primary:8080", attach_db_id).as_str(),
            &token,
            TurmoilConnector,
        )?
        .connect()
        .unwrap();
        attach_conn
            .execute("CREATE TABLE bar_table (x)", ())
            .await
            .unwrap();
        attach_conn
            .execute("insert into bar_table values (43)", ())
            .await
            .unwrap();

        let main_conn = Database::open_remote_with_connector(
            format!("http://{}.primary:8080", main_db_id).as_str(),
            &token,
            TurmoilConnector,
        )?
        .connect()
        .unwrap();

        // fails: namespace is uuid, hence needs to be wrapped in quotes
        assert_debug_snapshot!(main_conn
            .execute(
                "ATTACH DATABASE ae308915-caca-480f-a6b4-9f9f9dc84b11 as bar",
                ()
            )
            .await
            .unwrap_err());

        let txn = main_conn.transaction().await.unwrap();
        txn.execute(
            format!("ATTACH DATABASE \"{}\" as bar", attach_db_id).as_str(),
            (),
        )
        .await
        .unwrap();
        let mut rows = txn.query("SELECT * FROM bar.bar_table", ()).await.unwrap();
        assert_debug_snapshot!(rows.next().await);
        Ok(())
    });

    sim.run().unwrap();
}
