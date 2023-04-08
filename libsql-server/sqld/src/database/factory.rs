use std::sync::Arc;

use futures::Future;

use super::Database;
use crate::error::Error;

#[async_trait::async_trait]
pub trait DbFactory: Send + Sync {
    async fn create(&self) -> Result<Arc<dyn Database>, Error>;
}

#[async_trait::async_trait]
impl<F, DB, Fut> DbFactory for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<DB, Error>> + Send,
    DB: Database + Sync + Send + 'static,
{
    async fn create(&self) -> Result<Arc<dyn Database>, Error> {
        let db = (self)().await?;
        Ok(Arc::new(db))
    }
}
