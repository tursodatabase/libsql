use crate::{Result, Value};

pub(super) trait RowsInner {
    fn next(&mut self) -> Result<Option<Row>>;

    fn column_count(&self) -> i32;

    fn column_name(&self, idx: i32) -> Option<&str>;
}

pub struct Rows {
    pub(super) inner: Box<dyn RowsInner + Send + Sync>,
}

impl Rows {
    pub fn next(&mut self) -> Result<Option<Row>> {
        self.inner.next()
    }

    pub fn column_count(&self) -> i32 {
        self.inner.column_count()
    }

    pub fn column_name(&self, idx: i32) -> Option<&str> {
        self.inner.column_name(idx)
    }
}

pub(super) struct LibsqlRows(pub(super) crate::Rows);

impl RowsInner for LibsqlRows {
    fn next(&mut self) -> Result<Option<Row>> {
        let row = self.0.next()?.map(|r| Row {
            inner: Box::new(LibsqlRow(r)),
        });

        Ok(row)
    }

    fn column_count(&self) -> i32 {
        self.0.column_count()
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.0.column_name(idx)
    }
}

pub struct Row {
    pub(super) inner: Box<dyn RowInner + Send + Sync>,
}

impl Row {
    pub fn get_value(&self, idx: i32) -> Result<Value> {
        self.inner.column_value(idx)
    }

    pub fn get_str(&self, idx: i32) -> Result<&str> {
        self.inner.column_str(idx)
    }

    pub fn column_name(&self, idx: i32) -> Option<&str> {
        self.inner.column_name(idx)
    }
}

pub(super) trait RowInner {
    fn column_value(&self, idx: i32) -> Result<Value>;
    fn column_str(&self, idx: i32) -> Result<&str>;
    fn column_name(&self, idx: i32) -> Option<&str>;
}

struct LibsqlRow(crate::Row);

impl RowInner for LibsqlRow {
    fn column_value(&self, idx: i32) -> Result<Value> {
        self.0.get_value(idx)
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.0.column_name(idx)
    }

    fn column_str(&self, idx: i32) -> Result<&str> {
        self.0.get::<&str>(idx)
    }
}
