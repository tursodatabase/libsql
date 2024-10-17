use hyper::server::conn::Http;
use rand::RngCore;
use rand::Rng;
use s3s::auth::SimpleAuth;
use tempfile::tempdir;
use uuid::Uuid;

use crate::libsql_wal::config::SimConfig;
use crate::libsql_wal::S3_KEY_ID;
use crate::libsql_wal::S3_KEY_SECRET;

use super::SimService;

pub struct S3Service {
    hostname: Uuid,
}

impl SimService for S3Service {
    fn tick(
        &mut self,
        _sim: &mut turmoil::Sim,
        _config: &SimConfig,
        _rng: &mut dyn RngCore,
    ) -> bool {
        true
    }
}

impl S3Service {
    pub fn configure(sim: &mut turmoil::Sim, rng: &mut impl RngCore) -> Self {
        let hostname = Uuid::from_u128(rng.gen());
        sim.host(hostname.to_string(), move || async {
            let tmp = tempdir().unwrap();
            let fs_s3 = s3s_fs::FileSystem::new(tmp.path()).unwrap();
            let auth = SimpleAuth::from_single(S3_KEY_ID, S3_KEY_SECRET);
            let mut builder = s3s::service::S3ServiceBuilder::new(fs_s3);
            builder.set_auth(auth);
            let s3_service = builder.build().into_shared().into_make_service();

            let incoming = crate::common::net::TurmoilAcceptor::bind(([0, 0, 0, 0], 9000))
                .await
                .unwrap();
            hyper::server::Builder::new(incoming, Http::new())
                .serve(s3_service)
                .await
                .unwrap();

            Ok(())
        });

        Self {
            hostname,
        }
    }

    pub fn hostname(&self) -> Uuid {
        self.hostname
    }
}
