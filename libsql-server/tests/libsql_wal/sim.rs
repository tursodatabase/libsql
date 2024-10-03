use rand::{RngCore, SeedableRng as _};

use super::services::clients::ClientsService;
use super::services::sqld::SqldService;
use super::services::s3::S3Service;
use super::services::SimService;
use super::dns::Dns;
use super::config::SimConfig;


type Services = Vec<Box<dyn SimService>>;

pub struct Sim {
    simulator: turmoil::Sim<'static>,
    services: Services,
    config: SimConfig,
    rng: Box<dyn RngCore>,
}

impl Sim {
    pub fn configure(config: SimConfig) -> Self {
        let rng = Box::new(rand_chacha::ChaCha8Rng::seed_from_u64(config.seed));
        let mut builder = turmoil::Builder::new();
        builder
            .repair_rate(config.p_net_repair)
            .fail_rate(config.net_failrate)
            .enable_random_order();

        let mut sim =builder
            .build_with_rng(rng);
        let (encoding, decoding) = crate::common::auth::key_pair();
        let dns = Dns::new();

        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(config.seed);
        let mut services: Services = Vec::new();

        let s3 = S3Service::configure(&mut sim, &mut rng);
        dns.register("s3".to_string(), s3.hostname());
        services.push(Box::new(s3));

        let primary = SqldService::configure_primary(&mut sim, decoding.clone(), dns.clone(), &mut rng);
        dns.register("primary".to_string(), primary.hostname());
        services.push(Box::new(primary));

        let clients = ClientsService::configure(dns.clone(), encoding);
        services.push(Box::new(clients));

        for _ in 0..config.n_replicas {
            let replica = SqldService::configure_replica(&mut sim, decoding.clone(), dns.clone(), &mut rng);
            dns.register("replica".to_string(), replica.hostname());
            services.push(Box::new(replica));
        }

        Self {
            simulator: sim,
            services,
            config,
            rng: Box::new(rng),
        }
    }

    pub fn run(&mut self) {
        tracing_subscriber::fmt::try_init();

        loop {
            self.simulator.step().unwrap();
            self.services.retain_mut(|s| s.tick(&mut self.simulator, &self.config, &mut self.rng));
        }
    }
}

