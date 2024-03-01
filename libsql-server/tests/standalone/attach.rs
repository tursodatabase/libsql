use insta::assert_debug_snapshot;
use libsql::Database;

use crate::{
    common::{http::Client, net::TurmoilConnector},
    standalone::utils::{encode, key_pair},
};

use super::make_standalone_server;

#[test]
fn attach_no_auth() {
    let mut sim = turmoil::Builder::new().build();

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
            Database::open_remote_with_connector("http://foo.primary:8080", "dummy_token", TurmoilConnector)?;
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
            Database::open_remote_with_connector("http://bar.primary:8080", "dummy_token", TurmoilConnector)?;
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
    let mut sim = turmoil::Builder::new().build();

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
            "id": "foo",
            "p": {
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
