use crate::errors::Error;
use crate::errors::Error::WriteConflict;
use crate::store::FrameStore;
use async_trait::async_trait;
use foundationdb::api::NetworkAutoStop;
use foundationdb::tuple::pack;
use foundationdb::tuple::unpack;
use foundationdb::{KeySelector, Transaction};
use libsql_storage::rpc::Frame;
use tracing::error;

pub struct FDBFrameStore {
    _network: NetworkAutoStop,
}

// Some information about how we map keys on Foundation DB.
//
// key: (<ns>, "f", <frame_no>, "f")                value: bytes (i.e. frame data)
// key: (<ns>, "f", <frame_no>, "p")                value: u32 (stores page number)
// key: (<ns>, "p", <page_no>)                      value: u64 (stores latest frame no of a page)
// key: (<ns>, "pf", <page_no>, <frame_no>)         value: "" (empty string, this is used to keep track of page versions)
// key: (<ns>, "max_frame_no")                      value: u64 (current max frame no of this ns)

#[inline]
fn frame_key(namespace: &str, frame_no: u64) -> Vec<u8> {
    pack(&(namespace, "f", frame_no, "f"))
}

#[inline]
fn frame_page_key(namespace: &str, frame_no: u64) -> Vec<u8> {
    pack(&(namespace, "f", frame_no, "p"))
}

#[inline]
fn page_key(namespace: &str, page_no: u32) -> Vec<u8> {
    pack(&(namespace, "p", page_no))
}

#[inline]
fn page_index_key(namespace: &str, page_no: u32, frame_no: u64) -> Vec<u8> {
    pack(&(namespace, "pf", page_no, frame_no))
}

#[inline]
fn max_frame_key(namespace: &str) -> Vec<u8> {
    pack(&(namespace, "max_frame_no"))
}

impl FDBFrameStore {
    pub fn new() -> Self {
        let _network = unsafe { foundationdb::boot() };
        Self { _network }
    }

    async fn get_max_frame_no(&self, txn: &Transaction, namespace: &str) -> u64 {
        let key = max_frame_key(namespace);
        let result = txn.get(&key, false).await;
        if let Err(e) = result {
            error!("get failed: {:?}", e);
            return 0;
        }
        if let Ok(None) = result {
            error!("max_frame_key not found");
            return 0;
        }
        let frame_no: u64 = unpack(&result.unwrap().unwrap()).expect("failed to decode u64");
        tracing::info!("max_frame_no ({}) = {}", namespace, frame_no);
        frame_no
    }

    async fn insert_with_tx(
        &self,
        namespace: &str,
        txn: &Transaction,
        frame_no: u64,
        frame: Frame,
    ) {
        let frame_data_key = frame_key(namespace, frame_no);
        let frame_page_key = frame_page_key(namespace, frame_no);
        let page_key = page_key(namespace, frame.page_no);
        let page_frame_idx = page_index_key(namespace, frame.page_no, frame_no);

        txn.set(&frame_data_key, &frame.data);
        txn.set(&frame_page_key, &pack(&frame.page_no));
        txn.set(&page_key, &pack(&frame_no));
        txn.set(&page_frame_idx, &pack(&""));
    }
}

#[async_trait]
impl FrameStore for FDBFrameStore {
    async fn insert_frames(
        &self,
        namespace: &str,
        max_frame_no: u64,
        frames: Vec<Frame>,
    ) -> Result<u64, Error> {
        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let mut frame_no = self.get_max_frame_no(&txn, namespace).await;
        if frame_no != max_frame_no {
            return Err(WriteConflict);
        }
        for f in frames {
            frame_no += 1;
            self.insert_with_tx(namespace, &txn, frame_no, f).await;
        }
        let key = max_frame_key(namespace);
        txn.set(&key, &pack(&(frame_no)));
        txn.commit().await.expect("commit failed");
        Ok(frame_no)
    }

    async fn read_frame(&self, namespace: &str, frame_no: u64) -> Option<bytes::Bytes> {
        let key = frame_key(namespace, frame_no);
        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let frame = txn.get(&key, false).await;
        if let Ok(Some(data)) = frame {
            return Some(data.to_vec().into());
        }
        None
    }

    #[tracing::instrument(skip(self))]
    async fn find_frame(&self, namespace: &str, page_no: u32, max_frame_no: u64) -> Option<u64> {
        if max_frame_no == 0 {
            return None;
        }

        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let page_key = page_index_key(namespace, page_no, max_frame_no);
        let result = txn
            .get_key(&KeySelector::last_less_or_equal(&page_key), false)
            .await;
        let unpacked: (String, String, u32, u64) =
            unpack(&result.unwrap().to_vec()).expect("failed to decode");
        // It is important to verify that the data we got from Foundation DB matches with what we
        // want, since we are doing a range query.
        //
        // for example, say we searched for ('db_name42', 10, 20). If this page does not exist, then
        // it could match with ('db_name41', 10, 20) or with ('db_name42', 9, 20) since it does
        // lexicographic search.
        if (namespace, "pf", page_no) == (&unpacked.0, &unpacked.1, unpacked.2) {
            tracing::info!("got the frame_no = {:?}", unpacked);
            Some(unpacked.3)
        } else {
            None
        }
    }

    async fn frame_page_no(&self, namespace: &str, frame_no: u64) -> Option<u32> {
        let key = frame_page_key(namespace, frame_no);
        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let page_no: u32 = unpack(
            &txn.get(&key, true)
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
