use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::Stream;
use libsql_sys::wal::either::Either;
use libsql_wal::io::StdIO;
use libsql_wal::registry::WalRegistry;
use libsql_wal::replication::injector::Injector;
use libsql_wal::replication::storage::StorageReplicator;
use libsql_wal::replication::{replicator::Replicator, storage::ReplicateFromStorage as _};
use libsql_wal::segment::Frame;
use libsql_wal::shared_wal::SharedWal;
use libsql_wal::storage::backend::Backend as _;
use tempfile::tempdir;
use tokio_stream::StreamExt as _;

use crate::namespace::configurator::fork::ForkError;
use crate::namespace::RestoreOption;
use crate::{
    namespace::{meta_store::MetaStoreHandle, Namespace, NamespaceName, NamespaceStore},
    SqldStorage,
};

pub(crate) async fn libsql_wal_fork(
    registry: Arc<WalRegistry<StdIO, SqldStorage>>,
    base_path: &Path,
    from_ns: &Namespace,
    to_ns: NamespaceName,
    to_config: MetaStoreHandle,
    timestamp: Option<DateTime<Utc>>,
    store: NamespaceStore,
) -> crate::Result<Namespace> {
    let mut seen = Default::default();
    let storage = registry.storage();
    match &*storage {
        Either::A(s) => {
            let from_ns_name: libsql_sys::name::NamespaceName = from_ns.name().clone().into();
            let to_ns_name: libsql_sys::name::NamespaceName = to_ns.clone().into();

            let mut stream = match timestamp {
                Some(ts) => {
                    let key = s
                        .backend()
                        .find_segment(
                            &s.backend().default_config(),
                            &from_ns_name,
                            libsql_wal::storage::backend::FindSegmentReq::Timestamp(ts),
                        )
                        .await
                        .unwrap();
                    let restore_until = key.end_frame_no;
                    let replicator = StorageReplicator::new(storage.clone(), from_ns_name.clone());
                    replicator.stream(&mut seen, restore_until, 1)
                }
                // find the most recent frame_no
                None => {
                    let from_shared = tokio::task::spawn_blocking({
                        let registry = registry.clone();
                        let from_ns_name = from_ns_name.clone();
                        let path = from_ns.path.join("data");
                        move || registry.open(&path, &from_ns_name)
                    })
                    .await
                    .unwrap()?;

                    let replicator = Replicator::new(from_shared, 1, false);
                    Box::pin(replicator.into_frame_stream())
                }
            };

            let tmp = tempdir()?;
            let to_shared = tokio::task::spawn_blocking({
                let registry = registry.clone();
                let path = tmp.path().join("data");
                let to_ns_name = to_ns_name.clone();
                move || registry.open(&path, &to_ns_name)
            })
            .await
            .unwrap()?;

            // make sure that nobody can use that namespace
            registry.tombstone(&to_ns_name).await;
            let ret = try_inject(to_shared, &mut stream).await;
            registry.remove(&to_ns_name).await;
            ret?;

            tokio::fs::rename(tmp.path(), base_path.join("dbs").join(to_ns.as_str())).await?;

            Ok(store
                .make_namespace(&to_ns, to_config, RestoreOption::Latest)
                .await?)
        }
        Either::B(_) => Err(crate::Error::Fork(super::fork::ForkError::ForkNoStorage)),
    }
}

async fn try_inject(
    to_shared: Arc<SharedWal<StdIO, SqldStorage>>,
    stream: &mut Pin<
        Box<dyn Stream<Item = Result<Box<Frame>, libsql_wal::replication::Error>> + Send + '_>,
    >,
) -> crate::Result<()> {
    let stream = stream.peekable();
    tokio::pin!(stream);
    let mut injector = Injector::new(to_shared.clone(), 16)?;
    let mut count = 0;
    while let Some(f) = stream.next().await {
        let mut frame = f.map_err(|e| ForkError::Internal(anyhow::anyhow!(e)))?;
        count += 1;
        if stream.peek().await.is_none() {
            frame.header_mut().set_size_after(count);
        }

        injector.insert_frame(frame).await?;
    }

    tokio::task::spawn_blocking({
        let shared = to_shared.clone();
        move || shared.seal_current()
    })
    .await
    .unwrap()?;

    Ok(())
}
