use rand::RngCore;

use super::config::SimConfig;

pub mod sqld;
pub mod s3;
pub mod clients;

pub trait SimService {
    fn tick(
        &mut self,
        sim: &mut turmoil::Sim,
        config: &SimConfig,
        rng: &mut dyn RngCore,
    ) -> bool;
}

