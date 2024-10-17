use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use libsql_server::config::{AdminApiConfig, RpcClientConfig, RpcServerConfig, UserApiConfig};
use libsql_server::Server;
use rand::{seq::SliceRandom, Rng as _, RngCore};
use tempfile::{tempdir, TempDir};
use tracing::{Instrument, Level};
use uuid::Uuid;

use crate::auth::make_auth;
use crate::common::net::TurmoilAcceptor;
use crate::libsql_wal::config::SimConfig;
use crate::libsql_wal::dns::Dns;
use crate::libsql_wal::net::TurmoilConnector;
use crate::libsql_wal::{S3_KEY_ID, S3_KEY_SECRET};

use super::SimService;

#[derive(Debug, Clone, Copy)]
enum ServiceMode {
    Primary,
    Replica,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceState {
    SoftCrashed,
    HardCrashed,
    Healthy,
}

pub struct SqldService {
    mode: ServiceMode,
    hostname: Uuid,
    state: ServiceState,
    dir: TempDir,
}

impl SimService for SqldService {
    #[tracing::instrument(skip_all, fields(mode = ?self.mode, host = ?self.hostname))]
    fn tick(&mut self, sim: &mut turmoil::Sim, config: &SimConfig, rng: &mut dyn RngCore) -> bool {
        let next_state = match self.state {
            ServiceState::Healthy => {
                let next_state = [
                    (ServiceState::Healthy, 1.0 - config.p_soft_crash),
                    (ServiceState::SoftCrashed, config.p_soft_crash),
                ]
                .choose_weighted(rng, |i| i.1)
                .unwrap().0;

                match next_state {
                    ServiceState::SoftCrashed => {
                        sim.crash(self.hostname.to_string());
                    },
                    ServiceState::Healthy => (),
                    _ => unreachable!(),
                }

                next_state
            }
            ServiceState::SoftCrashed => {
                let next_state = [
                    (ServiceState::SoftCrashed, 1.0 - (config.p_hard_crash + config.p_repair)),
                    (ServiceState::HardCrashed, config.p_hard_crash),
                    (ServiceState::Healthy, config.p_repair),
                ]
                .choose_weighted(rng, |i| i.1)
                .unwrap().0;

                match next_state {
                    ServiceState::SoftCrashed => (),
                    ServiceState::HardCrashed => {
                        std::fs::remove_dir_all(self.dir.path().join("iku.db")).unwrap();
                    },
                    ServiceState::Healthy => {
                        sim.bounce(self.hostname.to_string());
                    },
                }

                next_state
            }
            ServiceState::HardCrashed => {
                let next_state = [
                    (ServiceState::HardCrashed, 1.0 - config.p_hard_crash),
                    (ServiceState::Healthy, config.p_repair),
                ]
                .choose_weighted(rng, |i| i.1)
                .unwrap().0;

                match next_state {
                    ServiceState::HardCrashed => (),
                    ServiceState::Healthy => {
                        sim.bounce(self.hostname.to_string());
                    },
                    ServiceState::SoftCrashed => unreachable!(),
                }

                next_state
            },
        };

        if next_state != self.state {
            tracing::info!(old_state = ?self.state, new_state = ?next_state, "sqld state transition");
        }

        self.state = next_state;

        true
    }
}

impl SqldService {
    pub fn configure_primary(
        sim: &mut turmoil::Sim,
        auth_key: String,
        dns: Dns,
        rng: &mut impl RngCore,
    ) -> Self {
        let hostname = Uuid::from_u128(rng.gen());
        let tmp = tempdir().unwrap();
        let path: Arc<Path> = tmp.path().join("iku.db").to_path_buf().into();
        sim.host(hostname.to_string(), move || {
            let auth = make_auth(&auth_key);
            let dns = dns.clone();
            let path = path.clone();
            let span = tracing::span!(Level::INFO, "sqld", mode = "primary", host = %hostname);
            async move {
                tokio::fs::create_dir_all(&path).await.unwrap();
                let server: Server<TurmoilConnector, _, _> = Server {
                    path,
                    connector: Some(TurmoilConnector::new(dns.clone())),
                    user_api_config: UserApiConfig {
                        auth_strategy: auth,
                        http_acceptor: Some(
                            TurmoilAcceptor::bind(([0, 0, 0, 0], 8080)).await.unwrap(),
                        ),
                        ..Default::default()
                    },
                    db_config: libsql_server::config::DbConfig {
                        bottomless_replication: Some(bottomless::replicator::Options {
                            create_bucket_if_not_exists: false,
                            verify_crc: false,
                            use_compression: bottomless::replicator::CompressionKind::None,
                            encryption_config: None,
                            aws_endpoint: Some("http://s3:9000".to_string()),
                            access_key_id: Some(S3_KEY_ID.to_string()),
                            secret_access_key: Some(S3_KEY_SECRET.to_string()),
                            session_token: None,
                            region: Some("us-east2".to_string()),
                            db_id: Some("test-db".to_string()),
                            bucket_name: "s3-bucket".to_string(),
                            max_frames_per_batch: 0,
                            max_batch_interval: Duration::from_secs(0),
                            s3_max_parallelism: 0,
                            s3_max_retries: 0,
                            skip_snapshot: true,
                        }),
                        ..Default::default()
                    },
                    admin_api_config: Some(AdminApiConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                        connector: TurmoilConnector::new(dns),
                        disable_metrics: true,
                        auth_key: None,
                    }),
                    rpc_server_config: Some(RpcServerConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await?,
                        tls_config: None,
                    }),
                    should_sync_from_storage: true,
                    sync_conccurency: 4,
                    use_custom_wal: Some(libsql_server::CustomWAL::LibsqlWal),
                    ..Default::default()
                };

                server.start().await.unwrap();

                Ok(())
            }.instrument(span)
        });

        Self {
            mode: ServiceMode::Primary,
            hostname,
            state: ServiceState::Healthy,
            dir: tmp,
        }
    }

    pub fn configure_replica(sim: &mut turmoil::Sim, auth_key: String, dns: Dns, rng: &mut impl RngCore) -> Self {
        let hostname = Uuid::from_u128(rng.gen());
        let tmp = tempdir().unwrap();
        let path: Arc<Path> = tmp.path().join("iku.db").to_path_buf().into();
        sim.host(hostname.to_string(), move || {
            let path = path.clone();
            let dns = dns.clone();
            let auth = make_auth(&auth_key);
            let span = tracing::span!(Level::INFO, "sqld", mode = "replica", host = %hostname);
            async move {
                tokio::fs::create_dir_all(&path).await.unwrap();
                let server = Server {
                    path,
                    user_api_config: UserApiConfig {
                        auth_strategy: auth,
                        http_acceptor: Some(
                            TurmoilAcceptor::bind(([0, 0, 0, 0], 8080)).await.unwrap(),
                        ),
                        ..Default::default()
                    },
                    admin_api_config: Some(AdminApiConfig {
                        acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                        connector: TurmoilConnector::new(dns.clone()),
                        disable_metrics: true,
                        auth_key: None,
                    }),
                    rpc_client_config: Some(RpcClientConfig {
                        remote_url: "http://primary:4567".into(),
                        connector: TurmoilConnector::new(dns.clone()),
                        tls_config: None,
                    }),
                    use_custom_wal: Some(libsql_server::CustomWAL::LibsqlWal),
                    ..Default::default()
                };

                server.start().await.unwrap();

                Ok(())
            }.instrument(span)
        });

        Self {
            mode: ServiceMode::Replica,
            hostname,
            dir: tmp,
            state: ServiceState::Healthy,
        }
    }

    pub fn hostname(&self) -> Uuid {
        self.hostname
    }
}
