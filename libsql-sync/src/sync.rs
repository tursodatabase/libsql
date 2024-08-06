pub mod rpc {
    #![allow(clippy::all)]
    include!("generated/walsync.rs");
}

pub struct SyncContext {
    durable_frame_num: u32,
}

impl SyncContext {
    pub fn new() -> Self {
        Self {
            durable_frame_num: 0,
        }
    }

    pub fn durable_frame_num(&self) -> u32 {
        0
    }
}
