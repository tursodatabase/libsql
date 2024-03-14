use std::time::Duration;

use libsql::Database;

use crate::common::auth::{encode, key_pair};
use crate::common::http::Client;
use crate::common::net::TurmoilConnector;

use super::make_standalone_server;

#[test]
fn jwt_auth_namespace_access() {
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

        let claims = serde_json::json!({
            "id": "foo",
        });
        let token = encode(&claims, &enc);

        let foo_db = Database::open_remote_with_connector(
            "http://foo.primary:8080",
            &token,
            TurmoilConnector,
        )?;
        let foo_conn = foo_db.connect().unwrap();
        foo_conn.execute("SELECT 1", ()).await.unwrap();

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn jwt_auth_gid_scope() {
    std::env::set_var("LIBSQL_GID", "my_gid");
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

        {
            let claims = serde_json::json!({
                "p": {
                    "ro": {
                        "tags": ["my_gid"]
                    }
                },
            });
            let token = encode(&claims, &enc);
            let foo_db = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                &token,
                TurmoilConnector,
            )?;
            let foo_conn = foo_db.connect().unwrap();
            foo_conn.execute("SELECT 1", ()).await.unwrap();
            foo_conn
                .execute("create table test (c)", ())
                .await
                .unwrap_err();
        }

        {
            let claims = serde_json::json!({
                "p": {
                    "ro": {
                        "tags": ["other_gid"]
                    }
                },
            });
            let token = encode(&claims, &enc);
            let foo_db = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                &token,
                TurmoilConnector,
            )?;
            let foo_conn = foo_db.connect().unwrap();
            foo_conn.execute("SELECT 1", ()).await.unwrap_err();
            foo_conn
                .execute("create table test (c)", ())
                .await
                .unwrap_err();
        }

        Ok(())
    });

    sim.run().unwrap();
}
