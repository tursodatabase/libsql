use std::time::Duration;

use config::SimConfig;
use rand::{thread_rng, Rng as _};
use sim::Sim;

mod services;
mod dns;
mod config;
mod net;
mod sim;

const S3_KEY_ID: &str = "some_key_id";
const S3_KEY_SECRET: &str = "some_key_secret";

#[test]
fn simulation() {
    let seed = thread_rng().gen();
    println!("running sim with seed: {seed}");
    let config = SimConfig {
        seed,
        n_replicas: 1,
        n_clients: 1,
        p_soft_crash: 0.000001,
        p_hard_crash: 0.0000001,
        p_repair: 0.01,
        net_failrate: 0.000001,
        p_net_repair: 0.00001,
        latency_curve: 0.04,
        sim_duration: Duration::from_secs(3600),
        n_namespaces: 1,
    };

    let mut simulator = Sim::configure(config);
    simulator.run();
}
