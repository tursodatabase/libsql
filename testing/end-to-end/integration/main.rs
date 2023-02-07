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
    #[clap(long, env = "SQLD_TEST_PODMAN_ADDR", requires("run"))]
    podman_addr: Option<String>,
    /// Whether the end-to-end tests should be run
    #[clap(long)]
    run: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    if opts.run {
        Octopod::init(
            opts.podman_addr.as_ref().unwrap(),
            vec![create_simple_cluster_app()],
        )?
        .run()
        .await?;
    }

    Ok(())
}
