use crate::store::FrameData;
use crate::store::FrameStore;
use async_trait::async_trait;
use foundationdb::api::NetworkAutoStop;
use foundationdb::tuple::pack;
use foundationdb::tuple::unpack;
use foundationdb::Transaction;
use tracing::error;

pub struct FDBFrameStore {
    _network: NetworkAutoStop,
}

impl FDBFrameStore {
    pub fn new() -> Self {
        let _network = unsafe { foundationdb::boot() };
        Self { _network }
    }

    async fn get_max_frame_no(&self, txn: &Transaction, namespace: &str) -> u64 {
        let max_frame_key = format!("{}/max_frame_no", namespace);
        let result = txn.get(&max_frame_key.as_bytes(), false).await;
        if let Err(e) = result {
            error!("get failed: {:?}", e);
            return 0;
        }
        if let Ok(None) = result {
            error!("page not found");
            return 0;
        }
        let frame_no: u64 = unpack(&result.unwrap().unwrap()).expect("failed to decode u64");
        tracing::info!("max_frame_no ({}) = {}", max_frame_key, frame_no);
        frame_no
    }

    async fn insert_with_tx(
        &self,
        namespace: &str,
        txn: &Transaction,
        frame_no: u64,
        frame: FrameData,
    ) {
        let frame_data_key = format!("{}/f/{}/f", namespace, frame_no);
        let frame_page_key = format!("{}/f/{}/p", namespace, frame_no);
        let page_key = format!("{}/p/{}", namespace, frame.page_no);

        txn.set(&frame_data_key.as_bytes(), &frame.data);
        txn.set(&frame_page_key.as_bytes(), &pack(&frame.page_no));
        txn.set(&page_key.as_bytes(), &pack(&frame_no));
    }
}

#[async_trait]
impl FrameStore for FDBFrameStore {
    async fn insert_frame(&self, namespace: &str, page_no: u32, frame: bytes::Bytes) -> u64 {
        let max_frame_key = format!("{}/max_frame_no", namespace);
        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let frame_no = self.get_max_frame_no(&txn, namespace).await + 1;
        self.insert_with_tx(
            namespace,
            &txn,
            frame_no,
            FrameData {
                page_no,
                data: frame,
            },
        )
        .await;
        txn.set(&max_frame_key.as_bytes(), &pack(&(frame_no)));
        txn.commit().await.expect("commit failed");
        frame_no
    }

    async fn insert_frames(&self, namespace: &str, frames: Vec<FrameData>) -> u64 {
        let max_frame_key = format!("{}/max_frame_no", namespace);
        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let mut frame_no = self.get_max_frame_no(&txn, namespace).await;

        for f in frames {
            frame_no += 1;
            self.insert_with_tx(
                namespace,
                &txn,
                frame_no,
                FrameData {
                    page_no: f.page_no,
                    data: f.data,
                },
            )
            .await;
        }
        txn.set(&max_frame_key.as_bytes(), &pack(&(frame_no)));
        txn.commit().await.expect("commit failed");
        frame_no
    }

    async fn read_frame(&self, namespace: &str, frame_no: u64) -> Option<bytes::Bytes> {
        let frame_key = format!("{}/f/{}/f", namespace, frame_no);

        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let frame = txn.get(frame_key.as_bytes(), false).await;
        if let Ok(Some(data)) = frame {
            return Some(data.to_vec().into());
        }
        None
    }

    async fn find_frame(&self, namespace: &str, page_no: u32) -> Option<u64> {
        let page_key = format!("{}/p/{}", namespace, page_no);

        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");

        let result = txn.get(&page_key.as_bytes(), false).await;
        if let Err(e) = result {
            error!("get failed: {:?}", e);
            return None;
        }
        if let Ok(None) = result {
            error!("page not found");
            return None;
        }
        let frame_no: u64 = unpack(&result.unwrap().unwrap()).expect("failed to decode u64");
        Some(frame_no)
    }

    async fn frame_page_no(&self, namespace: &str, frame_no: u64) -> Option<u32> {
        let frame_key = format!("{}/f/{}/p", namespace, frame_no);

        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let page_no: u32 = unpack(
            &txn.get(&frame_key.as_bytes(), true)
                .await
                .expect("get failed")
                .expect("frame not found"),
        )
        .expect("failed to decode u64");

        Some(page_no)
    }

    async fn frames_in_wal(&self, namespace: &str) -> u64 {
        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        self.get_max_frame_no(&txn, namespace).await
    }

    async fn destroy(&self, _namespace: &str) {}
}
