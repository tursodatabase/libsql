pub mod proxy {
    #![allow(clippy::all)]
    tonic::include_proto!("proxy");
}

pub mod replication {
    #![allow(clippy::all)]
    tonic::include_proto!("wal_log");
}
