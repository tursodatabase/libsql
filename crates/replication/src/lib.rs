use tokio::runtime::Runtime;

pub mod rpc {
    #![allow(clippy::all)]
    tonic::include_proto!("wal_log");
}

pub struct Replicator {
    url: String,
}

impl Replicator {
    pub fn new(url: String) -> Replicator {
        Replicator { url }
    }

    pub fn sync(&self) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut client =
                rpc::replication_log_client::ReplicationLogClient::connect(self.url.to_owned())
                    .await
                    .unwrap();
            let response = client.hello(rpc::HelloRequest {}).await;
            println!("RESPONSE={:?}", response);
        });
    }
}
