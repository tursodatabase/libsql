use crate::{Error, Result, Value};

use libsql_sys::ValueType;

pub(crate) trait RowsInner {
    fn next(&mut self) -> Result<Option<Row>>;

    fn column_count(&self) -> i32;

    fn column_name(&self, idx: i32) -> Option<&str>;

    fn column_type(&self, idx: i32) -> Result<ValueType>;
}

pub struct Rows {
    pub(crate) inner: Box<dyn RowsInner + Send + Sync>,
}

impl Rows {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Row>> {
        self.inner.next()
    }

    pub fn column_count(&self) -> i32 {
        self.inner.column_count()
    }

    pub fn column_name(&self, idx: i32) -> Option<&str> {
        self.inner.column_name(idx)
    }

    pub fn column_type(&self, idx: i32) -> Result<ValueType> {
        self.inner.column_type(idx)
    }
}

pub(crate) struct LibsqlRows(pub(crate) crate::v1::Rows);

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

    fn column_type(&self, idx: i32) -> Result<ValueType> {
        self.0.column_type(idx)
    }
}

pub struct Row {
    pub(super) inner: Box<dyn RowInner + Send + Sync>,
}

impl Row {
    pub fn get<T>(&self, idx: i32) -> Result<T>
    where
        T: FromValue,
    {
        let val = self.inner.column_value(idx)?;
        T::from_sql(val)
    }

    pub fn get_value(&self, idx: i32) -> Result<Value> {
        self.inner.column_value(idx)
    }

    pub fn get_str(&self, idx: i32) -> Result<&str> {
        self.inner.column_str(idx)
    }

    pub fn column_name(&self, idx: i32) -> Option<&str> {
        self.inner.column_name(idx)
    }

    pub fn column_type(&self, idx: i32) -> Result<ValueType> {
        self.inner.column_type(idx)
    }
}

pub trait FromValue {
    fn from_sql(val: Value) -> Result<Self>
    where
        Self: Sized;
}

impl FromValue for crate::Value {
    fn from_sql(val: Value) -> Result<Self> {
        Ok(val)
    }
}

impl FromValue for i32 {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(crate::Error::NullValue),
            Value::Integer(i) => Ok(i as i32),
            _ => unreachable!("invalid value type"),
        }
    }
}

impl FromValue for u32 {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(crate::Error::NullValue),
            Value::Integer(i) => Ok(i as u32),
            _ => unreachable!("invalid value type"),
        }
    }
}

impl FromValue for i64 {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(crate::Error::NullValue),
            Value::Integer(i) => Ok(i),
            _ => unreachable!("invalid value type"),
        }
    }
}

impl FromValue for u64 {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(crate::Error::NullValue),
            Value::Integer(i) => Ok(i as u64),
            _ => unreachable!("invalid value type"),
        }
    }
}

impl FromValue for f64 {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(crate::Error::NullValue),
            Value::Real(f) => Ok(f),
            _ => unreachable!("invalid value type"),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(crate::Error::NullValue),
            Value::Blob(blob) => Ok(blob),
            _ => unreachable!("invalid value type"),
        }
    }
}

impl FromValue for String {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(crate::Error::NullValue),
            Value::Text(s) => Ok(s),
            _ => unreachable!("invalid value type"),
        }
    }
}

pub(super) trait RowInner {
    fn column_value(&self, idx: i32) -> Result<Value>;
    fn column_str(&self, idx: i32) -> Result<&str>;
    fn column_name(&self, idx: i32) -> Option<&str>;
    fn column_type(&self, idx: i32) -> Result<ValueType>;
}

struct LibsqlRow(crate::v1::Row);

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

    fn column_type(&self, idx: i32) -> Result<ValueType> {
        self.0.column_type(idx)
    }
}

pub(crate) struct LibsqlRemoteRows(
    pub(crate) libsql_replication::pb::ResultRows,
    pub(crate) usize,
);

impl RowsInner for LibsqlRemoteRows {
    fn next(&mut self) -> Result<Option<Row>> {
        // TODO(lucio): Switch to a vecdeque and reduce allocations
        let cursor = self.1;
        self.1 += 1;
        let row = self.0.rows.get(cursor);

        if row.is_none() {
            return Ok(None);
        }

        let row = row.unwrap();

        let values = row
            .values
            .iter()
            .map(|v| bincode::deserialize(&v.data[..]).map_err(Error::from))
            .collect::<Result<Vec<_>>>()?;

        let row = LibsqlRemoteRow(values, self.0.column_descriptions.clone());
        Ok(Some(row).map(Box::new).map(|inner| Row { inner }))
    }

    fn column_count(&self) -> i32 {
        self.0.column_descriptions.len() as i32
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.0
            .column_descriptions
            .get(idx as usize)
            .map(|s| s.name.as_str())
    }

    fn column_type(&self, idx: i32) -> Result<ValueType> {
        let col = self.0.column_descriptions.get(idx as usize).unwrap();
        col.decltype
            .as_ref()
            .map(|s| s.as_str())
            .and_then(ValueType::from_str)
            .ok_or(Error::InvalidColumnType)
    }
}

struct LibsqlRemoteRow(Vec<Value>, Vec<libsql_replication::pb::Column>);

impl RowInner for LibsqlRemoteRow {
    fn column_value(&self, idx: i32) -> Result<Value> {
        self.0
            .get(idx as usize)
            .cloned()
            .ok_or(Error::InvalidColumnIndex)
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.1.get(idx as usize).map(|s| s.name.as_str())
    }

    fn column_str(&self, idx: i32) -> Result<&str> {
        let value = self.0.get(idx as usize).ok_or(Error::InvalidColumnIndex)?;

        match &value {
            Value::Text(s) => Ok(s.as_str()),
            _ => Err(Error::InvalidColumnType),
        }
    }

    fn column_type(&self, idx: i32) -> Result<ValueType> {
        let col = self.1.get(idx as usize).unwrap();
        col.decltype
            .as_ref()
            .map(|s| s.as_str())
            .and_then(ValueType::from_str)
            .ok_or(Error::InvalidColumnType)
    }
}
