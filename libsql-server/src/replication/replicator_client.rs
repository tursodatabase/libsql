use std::path::Path;
use std::pin::Pin;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use libsql_replication::meta::WalIndexMeta;
use libsql_replication::replicator::{Error, ReplicatorClient};
use libsql_replication::rpc::replication::log_offset::WalFlavor;
use libsql_replication::rpc::replication::replication_log_client::ReplicationLogClient;
use libsql_replication::rpc::replication::{
    verify_session_token, Frame as RpcFrame, HelloRequest, LogOffset, NAMESPACE_METADATA_KEY,
    SESSION_TOKEN_KEY,
};
use tokio::sync::watch;
use tokio_stream::Stream;

use tonic::metadata::{AsciiMetadataValue, BinaryMetadataValue};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

use crate::connection::config::DatabaseConfig;
use crate::metrics::{
    REPLICATION_LATENCY, REPLICATION_LATENCY_CACHE_MISS, REPLICATION_LATENCY_OUT_OF_SYNC,
};
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::{NamespaceName, NamespaceStore};
use crate::replication::FrameNo;

pub struct Client {
    client: ReplicationLogClient<Channel>,
    meta: WalIndexMeta,
    pub current_frame_no_notifier: watch::Sender<Option<FrameNo>>,
    namespace: NamespaceName,
    session_token: Option<Bytes>,
    meta_store_handle: MetaStoreHandle,
    // the primary current replication index, as reported by the last handshake
    pub primary_replication_index: Option<FrameNo>,
    store: NamespaceStore,
    wal_flavor: WalFlavor,
}

impl Client {
    pub async fn new(
        namespace: NamespaceName,
        client: ReplicationLogClient<Channel>,
        path: &Path,
        meta_store_handle: MetaStoreHandle,
        store: NamespaceStore,
        wal_flavor: WalFlavor,
    ) -> crate::Result<Self> {
        let (current_frame_no_notifier, _) = watch::channel(None);
        let meta = WalIndexMeta::open(path).await?;

        Ok(Self {
            namespace,
            client,
            current_frame_no_notifier,
            meta,
            session_token: None,
            meta_store_handle,
            primary_replication_index: None,
            store,
            wal_flavor,
        })
    }

    fn make_request<T>(&self, msg: T) -> Request<T> {
        let mut req = Request::new(msg);
        req.metadata_mut().insert_bin(
            NAMESPACE_METADATA_KEY,
            BinaryMetadataValue::from_bytes(self.namespace.as_slice()),
        );

        if let Some(token) = self.session_token.clone() {
            // SAFETY: we always check the session token
            req.metadata_mut().insert(SESSION_TOKEN_KEY, unsafe {
                AsciiMetadataValue::from_shared_unchecked(token)
            });
        }

        req
    }

    fn next_frame_no(&self) -> FrameNo {
        match *self.current_frame_no_notifier.borrow() {
            Some(fno) => fno + 1,
            None => 0,
        }
    }

    pub(crate) fn reset_token(&mut self) {
        self.session_token = None;
    }
}

#[async_trait::async_trait]
impl ReplicatorClient for Client {
    type FrameStream = Pin<Box<dyn Stream<Item = Result<RpcFrame, Error>> + Send + 'static>>;

    #[tracing::instrument(skip(self))]
    async fn handshake(&mut self) -> Result<(), Error> {
        tracing::debug!("Attempting to perform handshake with primary.");
        let req = self.make_request(HelloRequest::new());
        let resp = self.client.hello(req).await?;
        let hello = resp.into_inner();
        verify_session_token(&hello.session_token).map_err(Error::Client)?;
        self.primary_replication_index = hello.current_replication_index;
        self.session_token.replace(hello.session_token.clone());

        if let Some(config) = &hello.config {
            // HACK: if we load a shared schema db before the main schema is replicated,
            // inserting the new database in the meta store will cause a foreign constraint Error
            // because we have a constraint check that ensure shared schema dbs point to a valid
            // main schema. To prevent that, we load the main schema first.
            if let Some(ref name) = config.shared_schema_name {
                let name = NamespaceName::from_string(name.clone())
                    .map_err(|_| Status::new(Code::InvalidArgument, "invalid namespace name"))?;
                self.store
                    .with(name, |_| ())
                    .await
                    .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
            }

            self.meta_store_handle
                .store(DatabaseConfig::from(config))
                .await
                .map_err(|e| Error::Internal(e.into()))?;

            tracing::debug!("replica config has been updated");
        } else {
            tracing::debug!("no config passed in handshake");
        }

        self.meta.init_from_hello(hello)?;
        self.current_frame_no_notifier
            .send_replace(self.meta.current_frame_no());

        tracing::trace!("handshake completed");

        Ok(())
    }

    async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
        let offset = LogOffset {
            next_offset: self.next_frame_no(),
            wal_flavor: Some(self.wal_flavor.into()),
        };
        let req = self.make_request(offset);
        let stream = self
            .client
            .log_entries(req)
            .await?
            .into_inner()
            .inspect_ok(|f| {
                match f.timestamp {
                    Some(ts_millis) => {
                        if let Some(commited_at) = DateTime::from_timestamp_millis(ts_millis) {
                            let lat = Utc::now() - commited_at;
                            match lat.to_std() {
                                Ok(lat) => {
                                    // we can record negative values if the clocks are out-of-sync. There is not
                                    // point in recording those values.
                                    REPLICATION_LATENCY.record(lat);
                                }
                                Err(_) => {
                                    REPLICATION_LATENCY_OUT_OF_SYNC.increment(1);
                                }
                            }
                        }
                    }
                    None => REPLICATION_LATENCY_CACHE_MISS.increment(1),
                }
            })
            .map_err(Into::into);

        Ok(Box::pin(stream))
    }

    async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
        let offset = LogOffset {
            next_offset: self.next_frame_no(),
            wal_flavor: Some(self.wal_flavor.into()),
        };
        let req = self.make_request(offset);
        match self.client.snapshot(req).await {
            Ok(resp) => {
                let stream = resp.into_inner().map_err(Into::into);
                Ok(Box::pin(stream))
            }
            Err(e) if e.code() == Code::Unavailable => Err(Error::SnapshotPending),
            Err(e) => return Err(e.into()),
        }
    }

    async fn commit_frame_no(
        &mut self,
        frame_no: libsql_replication::frame::FrameNo,
    ) -> Result<(), Error> {
        self.current_frame_no_notifier.send_replace(Some(frame_no));
        self.meta.set_commit_frame_no(frame_no).await?;

        Ok(())
    }

    fn committed_frame_no(&self) -> Option<FrameNo> {
        self.meta.current_frame_no()
    }

    fn rollback(&mut self) {}
}
