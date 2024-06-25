use crate::errors::Error;
use crate::store::FrameStore;
use async_trait::async_trait;
use bytes::Bytes;
use libsql_storage::rpc::Frame;
use redis::{Client, Commands, RedisResult};
use tracing::error;

pub struct RedisFrameStore {
    client: Client,
}

impl RedisFrameStore {
    pub fn new(redis_addr: String) -> Self {
        let client = Client::open(redis_addr).unwrap();
        Self { client }
    }
}

#[async_trait]
impl FrameStore for RedisFrameStore {
    async fn insert_frames(
        &self,
        namespace: &str,
        _max_frame_no: u64,
        frames: Vec<Frame>,
    ) -> Result<u64, Error> {
        let mut max_frame_no = 0;
        let max_frame_key = format!("{}/max_frame_no", namespace);
        let mut con = self.client.get_connection().unwrap();
        // max_frame_key might change if another client inserts a frame, so do
        // all this in a transaction!
        let (max_frame_no,): (u64,) =
            redis::transaction(&mut con, &[&max_frame_key], |con, pipe| {
                let result: RedisResult<u64> = con.get(max_frame_key.clone());
                if result.is_err() && !is_nil_response(result.as_ref().err().unwrap()) {
                    return Err(result.err().unwrap());
                }
                max_frame_no = result.unwrap_or(0);
                for frame in &frames {
                    let max_frame_no = max_frame_no + 1;
                    let frame_key = format!("f/{}/{}", namespace, max_frame_no);
                    let page_key = format!("p/{}/{}", namespace, frame.page_no);

                    pipe.hset::<String, &str, Vec<u8>>(frame_key.clone(), "f", frame.data.to_vec())
                        .ignore()
                        .hset::<String, &str, u32>(frame_key.clone(), "p", frame.page_no)
                        .ignore()
                        .set::<String, u64>(page_key, max_frame_no)
                        .ignore()
                        .set::<String, u64>(max_frame_key.clone(), max_frame_no)
                        .ignore();
                }
                pipe.get(max_frame_key.clone()).query(con)
            })
            .unwrap();
        Ok(max_frame_no)
    }

    async fn read_frame(&self, namespace: &str, frame_no: u64) -> Option<Bytes> {
        let frame_key = format!("f/{}/{}", namespace, frame_no);
        let mut con = self.client.get_connection().unwrap();
        let result = con.hget::<String, &str, Vec<u8>>(frame_key.clone(), "f");
        match result {
            Ok(frame) => Some(Bytes::from(frame)),
            Err(e) => {
                if !is_nil_response(&e) {
                    error!(
                        "read_frame() failed for frame_no={} with err={}",
                        frame_no, e
                    );
                }
                None
            }
        }
    }

    async fn find_frame(&self, namespace: &str, page_no: u32, _max_frame_no: u64) -> Option<u64> {
        let page_key = format!("p/{}/{}", namespace, page_no);
        let mut con = self.client.get_connection().unwrap();
        let frame_no = con.get::<String, u64>(page_key.clone());
        match frame_no {
            Ok(frame_no) => Some(frame_no),
            Err(e) => {
                if !is_nil_response(&e) {
                    error!("find_frame() failed for page_no={} with err={}", page_no, e);
                }
                None
            }
        }
    }

    async fn frame_page_no(&self, namespace: &str, frame_no: u64) -> Option<u32> {
        let frame_key = format!("f/{}/{}", namespace, frame_no);
        let mut con = self.client.get_connection().unwrap();
        let result = con.hget::<String, &str, u32>(frame_key.clone(), "p");
        match result {
            Ok(page_no) => Some(page_no),
            Err(e) => {
                if !is_nil_response(&e) {
                    error!(
                        "frame_page_no() failed for frame_no={} with err={}",
                        frame_no, e
                    );
                }
                None
            }
        }
    }

    async fn frames_in_wal(&self, namespace: &str) -> u64 {
        let max_frame_key = format!("{}/max_frame_no", namespace);
        let mut con = self.client.get_connection().unwrap();
        let result = con.get::<String, u64>(max_frame_key.clone());
        result.unwrap_or_else(|e| {
            if !is_nil_response(&e) {
                error!("frames_in_wal() failed with err={}", e);
            }
            0
        })
    }

    async fn destroy(&self, _namespace: &str) {
        // remove all the keys in redis
        let mut con = self.client.get_connection().unwrap();
        // send a FLUSHALL request
        let _: () = redis::cmd("FLUSHALL").query(&mut con).unwrap();
    }
}

fn is_nil_response(e: &redis::RedisError) -> bool {
    e.to_string().contains("response was nil")
}
