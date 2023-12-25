use crate::hrana::HranaRows;
use crate::Row;
use bytes::Bytes;
use futures::Stream;

pub struct Rows {
    pub(super) inner: Box<dyn RowsInner>,
}

impl Rows {
    pub async fn next(&mut self) -> crate::Result<Option<Row>> {
        self.inner.next().await
    }

    pub fn column_count(&self) -> i32 {
        self.inner.column_count()
    }

    pub fn column_name(&self, idx: i32) -> Option<&str> {
        self.inner.column_name(idx)
    }
}

#[async_trait::async_trait(?Send)]
pub(super) trait RowsInner {
    async fn next(&mut self) -> crate::Result<Option<Row>>;

    fn column_count(&self) -> i32;

    fn column_name(&self, idx: i32) -> Option<&str>;
}

#[async_trait::async_trait(?Send)]
impl<S> RowsInner for HranaRows<S>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin,
{
    async fn next(&mut self) -> crate::Result<Option<Row>> {
        self.next().await
    }

    fn column_count(&self) -> i32 {
        self.column_count()
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.column_name(idx)
    }
}
