mod basic_cluster;

use anyhow::bail;
use clap::Parser;
use octopod::{AppConfig, Octopod, ServiceConfig};

fn create_simple_cluster_app() -> AppConfig {
    let mut app = AppConfig::new("simple-cluster");
    app.add_service(
        ServiceConfig::new("primary", "sqld")
            .env([("SQLD_NODE", "primary"), ("RUST_LOG", "sqld=debug")])
            .health("/health", 8080),
    );
    app.add_service(
        ServiceConfig::new("replica", "sqld")
            .env([
                ("SQLD_NODE", "replica"),
                ("RUST_LOG", "sqld=debug"),
                ("SQLD_PRIMARY_URL", "http://primary:5001"),
                ("SQLD_HTTP_LISTEN_ADDR", "0.0.0.0:8080"),
            ])
            .health("/health", 8080),
    );
    app
}

#[derive(clap::Parser)]
struct Opts {
    #[clap(long, env = "SQLD_TEST_PODMAN_ADDR", requires("run"))]
    podman_addr: Option<String>,
    /// Whether the end-to-end tests should be run
    #[clap(long, env = "SQLD_TEST_RUN")]
    run: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    if opts.run {
        let success = Octopod::init(
            opts.podman_addr.as_ref().unwrap(),
            vec![create_simple_cluster_app()],
        )?
        .run()
        .await?;

        if !success {
            bail!("tests failed")
        }
    }

    Ok(())
}
