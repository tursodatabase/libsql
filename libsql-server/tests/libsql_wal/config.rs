use std::time::Duration;

#[derive(Debug, serde::Deserialize)]
pub struct SimConfig {
    /// random seed to use for the simulation
    pub seed: u64,

    /// how many replica to start the sim with
    pub n_replicas: usize,

    /// p that a service soft-crashes
    pub p_soft_crash: f64,
    /// when in soft crash, prob that primary transition to hard crash (volume loss)
    pub p_hard_crash: f64,
    /// P that a servive in a broken state is repaired
    pub p_repair: f64,

    /// general sim settings
    pub net_failrate: f64,
    pub p_net_repair: f64,
    pub latency_curve: f64,
    pub sim_duration: Duration,

    pub n_clients: usize,
    pub n_namespaces: usize,

    /* workload config */
    /// probability to perform a batch query
    pub p_batch: f64,
    /// probability to perform an interactive
    pub p_interactive_txn: f64,
    /// probability to perform a write query
    pub p_write: f64,
    /// probability to perform a read query
    pub p_read: f64,
    /// maximum number of steps in a client workload
    pub max_steps: usize,
}
