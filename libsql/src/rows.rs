use crate::{Result, Value, ValueType};
use std::fmt;

// NOTICE: Column is blatantly copy-pasted from rusqlite
pub struct Column<'stmt> {
    pub name: &'stmt str,
    pub origin_name: Option<&'stmt str>,
    pub table_name: Option<&'stmt str>,
    pub database_name: Option<&'stmt str>,
    pub decl_type: Option<&'stmt str>,
}

impl Column<'_> {
    /// Returns the name assigned to the column in the result set.
    pub fn name(&self) -> &str {
        self.name
    }

    /// Returns the name of the column in the origin table.
    pub fn origin_name(&self) -> Option<&str> {
        self.origin_name
    }

    /// Returns the name of the origin table.
    pub fn table_name(&self) -> Option<&str> {
        self.table_name
    }

    /// Returns the name of the origin database.
    pub fn database_name(&self) -> Option<&str> {
        self.database_name
    }

    /// Returns the type of the column (`None` for expression).
    pub fn decl_type(&self) -> Option<&str> {
        self.decl_type
    }
}

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

pub struct Row {
    pub(crate) inner: Box<dyn RowInner + Send + Sync>,
}

pub trait ColumnIndex: fmt::Debug {
    fn index(&self, row: &Row) -> Result<i32>;
}

impl ColumnIndex for &str {
    fn index(&self, row: &Row) -> Result<i32> {
        row.column_index(*self)
            .ok_or_else(|| crate::Error::InvalidColumnName(self.to_string()))
    }
}

impl ColumnIndex for i32 {
    fn index(&self, _: &Row) -> Result<i32> {
        Ok(*self)
    }
}

impl Row {
    pub fn get<T, I>(&self, idx: I) -> Result<T>
    where
        T: FromValue,
        I: ColumnIndex,
    {
        let val = self.inner.column_value(idx.index(self)?)?;
        T::from_sql(val)
    }

    pub fn get_value<I>(&self, idx: I) -> Result<Value>
    where
        I: ColumnIndex,
    {
        self.inner.column_value(idx.index(self)?)
    }

    pub fn get_str<I>(&self, idx: I) -> Result<&str>
    where
        I: ColumnIndex,
    {
        self.inner.column_str(idx.index(self)?)
    }

    pub fn column_name(&self, idx: i32) -> Option<&str> {
        self.inner.column_name(idx)
    }

    pub fn column_index(&self, name: &str) -> Option<i32> {
        self.inner.column_index(name)
    }

    pub fn column_type<I>(&self, idx: I) -> Result<ValueType>
    where
        I: ColumnIndex,
    {
        self.inner.column_type(idx.index(self)?)
    }
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        self.inner.fmt(f)
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

impl FromValue for bool {
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Err(crate::Error::NullValue),
            Value::Integer(i) => match i {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(crate::Error::InvalidColumnType),
            },
            _ => unreachable!("invalid value type"),
        }
    }
}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    fn from_sql(val: Value) -> Result<Self> {
        match val {
            Value::Null => Ok(None),
            _ => T::from_sql(val).map(Some),
        }
    }
}

pub(crate) trait RowInner: fmt::Debug {
    fn column_value(&self, idx: i32) -> Result<Value>;
    fn column_str(&self, idx: i32) -> Result<&str>;
    fn column_name(&self, idx: i32) -> Option<&str>;
    fn column_index(&self, name: &str) -> Option<i32>;
    fn column_type(&self, idx: i32) -> Result<ValueType>;
}
