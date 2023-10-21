pub mod proxy {
    #![allow(clippy::all)]
    tonic::include_proto!("proxy");
}

pub mod replication {
    #![allow(clippy::all)]
    tonic::include_proto!("wal_log");

    pub const NO_HELLO_ERROR_MSG: &str = "NO_HELLO";
    pub const NEED_SNAPSHOT_ERROR_MSG: &str = "NEED_SNAPSHOT";
}
