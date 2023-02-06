mod basic_cluster;

use clap::Parser;
use octopod::{AppConfig, Octopod, ServiceConfig};

fn create_simple_cluster_app() -> AppConfig {
    let mut app = AppConfig::new("simple-cluster");
    app.add_service(ServiceConfig {
        name: "primary".into(),
        image: "sqld".into(),
        env: vec![
            ("SQLD_NODE".into(), "primary".into()),
            ("RUST_LOG".into(), "sqld:debug".into()),
        ],
    });
    app.add_service(ServiceConfig {
        name: "replica".into(),
        image: "sqld".into(),
        env: vec![
            ("SQLD_NODE".into(), "replica".into()),
            ("RUST_LOG".into(), "sqld:debug".into()),
            ("SQLD_PRIMARY_URL".into(), "http://primary:5001".into()),
            ("SQLD_HTTP_LISTEN_ADDR".into(), "0.0.0.0:8080".into()),
        ],
    });
    app
}

#[derive(clap::Parser)]
struct Opts {
    #[clap(long, env = "SQLD_TEST_PODMAN_ADDR")]
    podman_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    Octopod::init(&opts.podman_addr, vec![create_simple_cluster_app()])?
        .run()
        .await?;

    Ok(())
}
