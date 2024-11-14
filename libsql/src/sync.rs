const DEFAULT_MAX_RETRIES: usize = 5;
pub struct SyncContext {
    pub sync_url: String,
    pub auth_token: Option<String>,
    pub max_retries: usize,
    pub durable_frame_num: u32,
}

impl SyncContext {
    pub fn new(sync_url: String, auth_token: Option<String>) -> Self {
        Self {
            sync_url,
            auth_token,
            durable_frame_num: 0,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}
