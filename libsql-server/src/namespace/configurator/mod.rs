use std::pin::Pin;

use futures::Future;

use super::broadcasters::BroadcasterHandle;
use super::meta_store::MetaStoreHandle;
use super::{NamespaceConfig, NamespaceName, NamespaceStore, ResetCb, ResolveNamespacePathFn, RestoreOption};

mod replica;
mod primary;

type DynConfigurator = Box<dyn ConfigureNamespace + Send + Sync + 'static>;

#[derive(Default)]
struct NamespaceConfigurators {
    replica_configurator: Option<DynConfigurator>,
    primary_configurator: Option<DynConfigurator>,
    schema_configurator: Option<DynConfigurator>,
}

impl NamespaceConfigurators {
    pub fn with_primary(
        &mut self,
        c: impl ConfigureNamespace + Send + Sync + 'static,
    ) -> &mut Self {
        self.primary_configurator = Some(Box::new(c));
        self
    }

    pub fn with_replica(
        &mut self,
        c: impl ConfigureNamespace + Send + Sync + 'static,
    ) -> &mut Self {
        self.replica_configurator = Some(Box::new(c));
        self
    }

    pub fn with_schema(&mut self, c: impl ConfigureNamespace + Send + Sync + 'static) -> &mut Self {
        self.schema_configurator = Some(Box::new(c));
        self
    }
}

pub trait ConfigureNamespace {
    fn setup<'a>(
        &'a self,
        ns_config: &'a NamespaceConfig,
        db_config: MetaStoreHandle,
        restore_option: RestoreOption,
        name: &'a NamespaceName,
        reset: ResetCb,
        resolve_attach_path: ResolveNamespacePathFn,
        store: NamespaceStore,
        broadcaster: BroadcasterHandle,
    ) -> Pin<Box<dyn Future<Output = crate::Result<super::Namespace>> + Send + 'a>>;
}
