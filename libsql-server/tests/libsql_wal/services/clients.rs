use std::time::Duration;

use hashbrown::HashMap;
use itertools::Itertools;
use jsonwebtoken::EncodingKey;
use libsql::Database;
use rand::seq::SliceRandom;
use rand::{Rng, RngCore};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::libsql_wal::config::SimConfig;
use crate::{
    auth::make_default_token,
    libsql_wal::{dns::Dns, net::TurmoilConnector},
};

use super::SimService;

#[derive(Debug)]
struct Namespace {
    db: Database,
}

enum State {
    Init,
    /// waiting for primary healthcheck to pass
    WaitingHealth(oneshot::Receiver<()>),
    /// waiting for namespace creation
    WaitingForNamespace(oneshot::Receiver<HashMap<Uuid, Namespace>>),
    Running {
        namespaces: HashMap<Uuid, Namespace>,
    },
}

type HttpClient = crate::common::http::Client<TurmoilConnector>;

pub struct ClientsService {
    state: State,
    dns: Dns,
    encoding_key: EncodingKey,
}

impl ClientsService {
    pub fn configure(dns: Dns, encoding_key: EncodingKey) -> Self {
        Self {
            state: State::Init,
            dns,
            encoding_key,
        }
    }

    fn http_client(&self) -> HttpClient {
        crate::common::http::Client::from(
            hyper::client::Client::builder().build(TurmoilConnector::new(self.dns.clone())),
        )
    }
}

impl SimService for ClientsService {
    fn tick(
        &mut self,
        sim: &mut turmoil::Sim,
        config: &crate::libsql_wal::config::SimConfig,
        rng: &mut dyn rand::RngCore,
    ) -> bool {
        match self.state {
            State::Init => {
                let (snd, rcv) = oneshot::channel();
                let id = Uuid::from_u128(rng.gen());
                let client = self.http_client();
                sim.client(id.to_string(), async move {
                    loop {
                        if client.get("http://primary:8080/health").await.is_ok() {
                            let _ = snd.send(());
                            break;
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }

                    Ok(())
                });

                self.state = State::WaitingHealth(rcv);
            }
            State::WaitingHealth(ref mut rcv) => {
                if rcv.try_recv().is_ok() {
                    let (snd, rcv) = oneshot::channel();
                    let id = Uuid::from_u128(rng.gen());

                    let mut namespaces = HashMap::new();
                    let token = make_default_token(&self.encoding_key);
                    for _ in 0..config.n_namespaces {
                        let ns_id = Uuid::from_u128(rng.gen());
                        let mut b = vec![0; 4096];
                        rng.fill_bytes(&mut b);
                        let mut u = Unstructured::new(&b);
                        let schema = Schema::arbitrary(&mut u).unwrap();
                        let ns = Namespace {
                            schema,
                            #[allow(deprecated)] // the builder uses async for no reason
                            db: Database::open_remote_with_connector(
                                format!("http://{ns_id}.primary:8080"),
                                &token,
                                TurmoilConnector::new(self.dns.clone()),
                            ).unwrap(),
                        };

                        namespaces.insert(ns_id, ns);
                    }

                    let client = self.http_client();
                    sim.client(id.to_string(), async move {
                        for (id, ns) in namespaces.iter() {
                            let resp = client
                                .post(
                                    &format!("http://primary:9090/v1/namespaces/{id}/create"),
                                    serde_json::json!({}),
                                )
                                .await
                                .unwrap();
                            assert!(resp.status().is_success());
                            let schema_sql = ns.schema.to_sql();
                            let conn = ns.db.connect().unwrap();
                            let tx = conn.transaction().await.unwrap();
                            tx.execute("CREATE TABLE IF NOT EXISTS t1(a INTEGER PRIMARY KEY, b BLOB(16), c BLOB(16), d BLOB(400))", ()).await?;
                            tx.execute("CREATE INDEX IF NOT EXISTS i1 ON t1(b)", ()).await?;
                            tx.execute("CREATE INDEX IF NOT EXISTS i2 ON t1(c)", ()).await?;
                            tx.commit().await.unwrap();
                        }

                        snd.send(namespaces).unwrap();

                        Ok(())
                    });

                    self.state = State::WaitingForNamespace(rcv);
                }
            }
            State::WaitingForNamespace(ref mut s) => {
                if let Ok(namespaces) = s.try_recv() {
                    self.state = State::Running { namespaces };
                }
            }
            State::Running { .. } => {
                todo!()
            }
        }

        true
    }
}

enum Workload {
    Read,
    Write,
    Batch(Vec<Self>),
    InteractiveTxn(Vec<Self>),
}

enum Op {
    Read,
    Write,
    Batch,
    InteractiveTxn,
}

impl Workload {
    fn generate(config: &SimConfig, rng: &mut impl Rng) -> Self {
        let mut steps = rng.gen_range(1..config.max_steps);
        while steps != 0 {
            let opts = [(Op::Read), (Op::Write), (Op::Batch), (Op::InteractiveTxn)].choose_weighted(rng, weight)
        }
    }
}
