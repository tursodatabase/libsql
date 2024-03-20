use std::time::Duration;

use libsql::Database;
use serde_json::json;
use tempfile::tempdir;
use turmoil::Builder;

use crate::common::{http::Client, net::TurmoilConnector};

use super::make_primary;

#[test]
fn replicated_config() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    crate::cluster::make_cluster(&mut sim, 1, false);

    sim.client("client", async {
        let client = Client::new();

        client
            .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
            .await
            .unwrap();

        // Update the config since we can't pass these specific items
        // to create.
        client
            .post(
                "http://primary:9090/v1/namespaces/foo/config",
                json!({
                    "block_reads": true,
                    "block_writes": false,
                }),
            )
            .await?;

        // Query primary
        {
            let foo = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;

            foo_conn.execute("select 1", ()).await.unwrap_err();
        }

        // Query replica
        {
            let foo = Database::open_remote_with_connector(
                "http://foo.replica1:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;

            foo_conn.execute("select 1", ()).await.unwrap_err();
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn meta_store() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let client = Client::new();

        // STEP 1: create namespace and check that it can be read from
        client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({
                    "max_db_size": "5mb"
                }),
            )
            .await?;

        {
            let foo = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;

            foo_conn.execute("select 1", ()).await.unwrap();
        }

        // STEP 2: update namespace config to block reads
        client
            .post(
                "http://primary:9090/v1/namespaces/foo/config",
                json!({
                    "block_reads": true,
                    "block_writes": false,
                }),
            )
            .await?;

        {
            let foo = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;

            foo_conn.execute("select 1", ()).await.unwrap_err();
        }

        // STEP 3: update config again to un-block reads
        client
            .post(
                "http://primary:9090/v1/namespaces/foo/config",
                json!({
                    "block_reads": false,
                    "block_writes": false,
                }),
            )
            .await?;

        {
            let foo = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;

            foo_conn.execute("select 1", ()).await.unwrap();
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn meta_attach() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let client = Client::new();

        // STEP 1: create namespace and check that it can be read from
        client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({
                    "max_db_size": "5mb"
                }),
            )
            .await?;

        {
            let foo = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;

            foo_conn.execute("select 1", ()).await.unwrap();
        }

        // STEP 2: try attaching a database
        {
            let foo = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;

            foo_conn.execute("attach foo as foo", ()).await.unwrap_err();
        }

        // STEP 3: update config to allow attaching databases
        client
            .post(
                "http://primary:9090/v1/namespaces/foo/config",
                json!({
                    "block_reads": false,
                    "block_writes": false,
                    "allow_attach": true,
                }),
            )
            .await?;

        {
            let foo = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;

            foo_conn
                .execute_batch("attach foo as foo; select * from foo.sqlite_master")
                .await
                .unwrap();
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn meta_attach_shared_schema() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let client = Client::new();

        // STEP 1: create two namespaces, one for the shared schema and one that borrows that
        // shared schema and check that it can be read from
        client
            .post(
                "http://primary:9090/v1/namespaces/schema/create",
                json!({
                    "shared_schema": true
                }),
            )
            .await?;

        client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({
                    "shared_schema_name": "schema"
                }),
            )
            .await?;

        // STEP 3: update config to allow attaching databases
        client
            .post(
                "http://primary:9090/v1/namespaces/foo/config",
                json!({
                    "block_reads": false,
                    "block_writes": false,
                    "allow_attach": true,
                }),
            )
            .await?;

        {
            let foo = Database::open_remote_with_connector(
                "http://foo.primary:8080",
                "",
                TurmoilConnector,
            )?;
            let foo_conn = foo.connect()?;

            foo_conn
                .execute_batch("attach foo as foo; select * from foo.sqlite_master")
                .await
                .unwrap();
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn meta_config_update() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();
    let tmp = tempdir().unwrap();
    make_primary(&mut sim, tmp.path().to_path_buf());

    sim.client("client", async {
        let client = Client::new();

        // STEP 1: create two namespaces, one for the shared schema and one that borrows that
        // shared schema and check that it can be read from
        client
            .post(
                "http://primary:9090/v1/namespaces/foo/create",
                json!({
                    "max_db_size": "5mb"
                }),
            )
            .await?;

        // STEP 3: update config to allow attaching databases
        client
            .post(
                "http://primary:9090/v1/namespaces/foo/config",
                json!({
                    "block_reads": false,
                    "block_writes": false,
                }),
            )
            .await?;

        // STEP 4: update config again
        client
            .post(
                "http://primary:9090/v1/namespaces/foo/config",
                json!({
                    "block_reads": false,
                    "block_writes": true,
                }),
            )
            .await?;

        Ok(())
    });

    sim.run().unwrap();
}
