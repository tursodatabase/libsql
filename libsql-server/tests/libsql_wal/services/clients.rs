use hashbrown::HashMap;
use uuid::Uuid;

use super::SimService;

struct Namespace { }

pub struct ClientsService {
    namespaces: HashMap<Uuid, Namespace>
}

impl SimService for ClientsService {
    fn tick(
        &mut self,
        _sim: &mut turmoil::Sim,
        _config: &crate::libsql_wal::config::SimConfig,
        _rng: &mut dyn rand::RngCore,
    ) -> bool {
        todo!()
    }
}
