use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::Result;
use clap::Parser;
use sqld::Config;
use tracing_subscriber::filter::LevelFilter;

/// SQL daemon
#[derive(Debug, Parser)]
#[command(name = "sqld")]
#[command(about = "SQL daemon", long_about = None)]
struct Cli {
    #[clap(long, short, default_value = "data.sqld", env = "SQLD_DB_PATH")]
    db_path: PathBuf,
    /// The address and port the PostgreSQL server listens to.
    #[clap(long, short, env = "SQLD_PG_LISTEN_ADDR")]
    pg_listen_addr: Option<SocketAddr>,
    /// The address and port the PostgreSQL over WebSocket server listens to.
    #[clap(long, short, env = "SQLD_WS_LISTEN_ADDR")]
    ws_listen_addr: Option<SocketAddr>,
    /// The address and port the Hrana server listens to.
    #[clap(long, short = 'l', env = "SQLD_HRANA_LISTEN_ADDR")]
    hrana_listen_addr: Option<SocketAddr>,

    /// The address and port the inter-node RPC protocol listens to. Example: `0.0.0.0:5001`.
    #[clap(
        long,
        conflicts_with = "primary_grpc_url",
        env = "SQLD_GRPC_LISTEN_ADDR"
    )]
    grpc_listen_addr: Option<SocketAddr>,
    #[clap(
        long,
        requires = "grpc_cert_file",
        requires = "grpc_key_file",
        requires = "grpc_ca_cert_file"
    )]
    grpc_tls: bool,
    #[clap(long)]
    grpc_cert_file: Option<PathBuf>,
    #[clap(long)]
    grpc_key_file: Option<PathBuf>,
    #[clap(long)]
    grpc_ca_cert_file: Option<PathBuf>,

    /// The gRPC URL of the primary node to connect to for writes. Example: `http://localhost:5001`.
    #[clap(long, env = "SQLD_PRIMARY_GRPC_URL")]
    primary_grpc_url: Option<String>,
    #[clap(
        long,
        requires = "primary_grpc_cert_file",
        requires = "primary_grpc_key_file",
        requires = "primary_grpc_ca_cert_file"
    )]
    primary_grpc_tls: bool,
    #[clap(long)]
    primary_grpc_cert_file: Option<PathBuf>,
    #[clap(long)]
    primary_grpc_key_file: Option<PathBuf>,
    #[clap(long)]
    primary_grpc_ca_cert_file: Option<PathBuf>,

    #[clap(
        long,
        short,
        value_enum,
        default_value = "libsql",
        env = "SQLD_BACKEND"
    )]
    backend: sqld::Backend,
    // The url to connect with mWAL backend, based on mvSQLite
    #[cfg(feature = "mwal_backend")]
    #[clap(long, short, env = "SQLD_MWAL_ADDR")]
    mwal_addr: Option<String>,

    #[clap(long, default_value = "127.0.0.1:8080", env = "SQLD_HTTP_LISTEN_ADDR")]
    http_listen_addr: SocketAddr,
    #[clap(long, env = "SQLD_HTTP_AUTH")]
    http_auth: Option<String>,
    #[clap(long)]
    enable_http_console: bool,
    /// Don't display welcome message
    #[clap(long)]
    no_welcome: bool,
    #[clap(long, env = "SQLD_ENABLE_BOTTOMLESS_REPLICATION")]
    enable_bottomless_replication: bool,
    /// Create a tunnel for the HTTP interface, available publicly via the https://localtunnel.me interface. The tunnel URL will be printed to stdin
    #[clap(long, env = "SQLD_CREATE_LOCAL_HTTP_TUNNEL")]
    create_local_http_tunnel: bool,
    /// The duration, in second, after which to shutdown the server if no request have been
    /// received.
    /// By default, the server doesn't shutdown when idle.
    #[clap(long, env = "SQLD_IDLE_SHUTDOWN_TIMEOUT_S")]
    idle_shutdown_timeout_s: Option<u64>,
}

impl Cli {
    #[rustfmt::skip]
    fn print_welcome_message(&self) {
        // no welcome :'(
        if self.no_welcome { return }

        eprintln!(r#"_____/\\\\\\\\\\\__________/\\\________/\\\______________/\\\\\\\\\\\\____        "#);
        eprintln!(r#" ___/\\\/////////\\\_____/\\\\/\\\\____\/\\\_____________\/\\\////////\\\__       "#);
        eprintln!(r#"  __\//\\\______\///____/\\\//\////\\\__\/\\\_____________\/\\\______\//\\\_      "#);
        eprintln!(r#"   ___\////\\\__________/\\\______\//\\\_\/\\\_____________\/\\\_______\/\\\_     "#);
        eprintln!(r#"    ______\////\\\______\//\\\______/\\\__\/\\\_____________\/\\\_______\/\\\_    "#);
        eprintln!(r#"     _________\////\\\____\///\\\\/\\\\/___\/\\\_____________\/\\\_______\/\\\_   "#);
        eprintln!(r#"      __/\\\______\//\\\_____\////\\\//_____\/\\\_____________\/\\\_______/\\\__  "#);
        eprintln!(r#"       _\///\\\\\\\\\\\/_________\///\\\\\\__\/\\\\\\\\\\\\\\\_\/\\\\\\\\\\\\/___ "#);
        eprintln!(r#"        ___\///////////_____________\//////___\///////////////__\////////////_____"#);

        eprintln!();
        eprintln!("Welcome to sqld!");
        eprintln!();
        eprintln!("version: {}", env!("VERGEN_BUILD_SEMVER"));
        eprintln!("commit SHA: {}", env!("VERGEN_GIT_SHA"));
        eprintln!("build date: {}", env!("VERGEN_BUILD_DATE"));
        eprintln!();
        eprintln!("This software is in BETA version.");
        eprintln!("If you encounter any bug, please open an issue at https://github.com/libsql/sqld/issues");
        eprintln!();

        eprintln!("config:");

        eprint!("\t- mode: ");
        match (&self.grpc_listen_addr, &self.primary_grpc_url) {
            (None, None) => eprintln!("standalone"),
            (Some(addr), None) => eprintln!("primary ({addr})"),
            (None, Some(url)) => eprintln!("replica (primary at {url})"),
            _ => unreachable!("invalid configuration!"),
        };
        eprintln!("\t- database path: {}", self.db_path.display());
        eprintln!("\t- listening for HTTP requests on: {}", self.http_listen_addr);
        if let Some(ref addr) = self.pg_listen_addr {
            eprintln!("\t- listening for PostgreSQL wire on: {addr}");
        }
        eprintln!("\t- gprc_tls: {}", if self.grpc_tls { "yes" } else { "no" });
    }
}

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        Self {
            db_path: cli.db_path,
            tcp_addr: cli.pg_listen_addr,
            ws_addr: cli.ws_listen_addr,
            http_addr: Some(cli.http_listen_addr),
            http_auth: cli.http_auth,
            enable_http_console: cli.enable_http_console,
            hrana_addr: cli.hrana_listen_addr,
            backend: cli.backend,
            writer_rpc_addr: cli.primary_grpc_url,
            writer_rpc_tls: cli.primary_grpc_tls,
            writer_rpc_cert: cli.primary_grpc_cert_file,
            writer_rpc_key: cli.primary_grpc_key_file,
            writer_rpc_ca_cert: cli.primary_grpc_ca_cert_file,
            rpc_server_addr: cli.grpc_listen_addr,
            rpc_server_tls: cli.grpc_tls,
            rpc_server_cert: cli.grpc_cert_file,
            rpc_server_key: cli.grpc_key_file,
            rpc_server_ca_cert: cli.grpc_ca_cert_file,
            #[cfg(feature = "mwal_backend")]
            mwal_addr: cli.mwal_addr,
            enable_bottomless_replication: cli.enable_bottomless_replication,
            create_local_http_tunnel: cli.create_local_http_tunnel,
            idle_shutdown_timeout: cli.idle_shutdown_timeout_s.map(Duration::from_secs),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
    let args = Cli::parse();
    args.print_welcome_message();

    #[cfg(feature = "mwal_backend")]
    match (&args.backend, args.mwal_addr.is_some()) {
        (sqld::Backend::Mwal, false) => {
            anyhow::bail!("--mwal-addr parameter must be present with mwal backend")
        }
        (backend, true) if backend != &sqld::Backend::Mwal => {
            anyhow::bail!(
                "--mwal-addr parameter conflicts with backend {:?}",
                args.backend
            )
        }
        _ => (),
    }

    sqld::run_server(args.into()).await?;

    Ok(())
}
