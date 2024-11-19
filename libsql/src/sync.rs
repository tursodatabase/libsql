use crate::Result;

const DEFAULT_MAX_RETRIES: usize = 5;
pub struct SyncContext {
    sync_url: String,
    auth_token: Option<String>,
    max_retries: usize,
    durable_frame_num: u32,
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

    pub(crate) async fn push_one_frame(
        &self,
        frame: Vec<u8>,
        generation: u32,
        frame_no: u32,
    ) -> Result<u32> {
        let uri = format!(
            "{}/sync/{}/{}/{}",
            self.sync_url,
            generation,
            frame_no,
            frame_no + 1
        );
        let max_frame_no = self
            .push_with_retry(uri, frame.to_vec(), self.max_retries)
            .await?;

        Ok(max_frame_no)
    }

    async fn push_with_retry(
        &self,
        uri: String,
        frame: Vec<u8>,
        max_retries: usize,
    ) -> Result<u32> {
        let mut nr_retries = 0;
        loop {
            let client = reqwest::Client::new();
            let mut builder = client.post(uri.to_owned());
            match &self.auth_token {
                Some(ref auth_token) => {
                    builder = builder.header("Authorization", format!("Bearer {}", auth_token));
                }
                None => {}
            }
            let res = builder.body(frame.to_vec()).send().await.unwrap();
            if res.status().is_success() {
                let resp = res.json::<serde_json::Value>().await.unwrap();
                let max_frame_no = resp.get("max_frame_no").unwrap().as_u64().unwrap();
                return Ok(max_frame_no as u32);
            }
            if nr_retries > max_retries {
                return Err(crate::errors::Error::ConnectionFailed(format!(
                    "Failed to push frame: {}",
                    res.status()
                )));
            }
            let delay = std::time::Duration::from_millis(100 * (1 << nr_retries));
            tokio::time::sleep(delay).await;
            nr_retries += 1;
        }
    }

    pub(crate) fn durable_frame_num(&self) -> u32 {
        self.durable_frame_num
    }
}
