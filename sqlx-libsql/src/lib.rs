#![allow(unused)]

use std::{
    iter::{Extend, IntoIterator},
    marker::PhantomData,
    ops::Deref,
    str::FromStr,
};

use futures_core::future::BoxFuture;
use sqlx_core::{
    arguments::Arguments,
    column::Column,
    connection::{ConnectOptions, Connection},
    database::Database,
    encode::Encode,
    executor::Executor,
    row::Row,
    statement::Statement,
    transaction::TransactionManager,
    type_info::TypeInfo,
    types::Type,
    value::{Value, ValueRef},
    Either,
};

#[derive(Debug)]
pub struct Libsql {}

impl Database for Libsql {
    type Connection = LibsqlConnection;
    type TransactionManager = LibsqlTransactionManager;
    type Row = LibsqlRow;
    type QueryResult = LibsqlQueryResult;
    type Column = LibsqlColumn;
    type TypeInfo = LibsqlTypeInfo;
    type Value = LibsqlValue;
    type ValueRef<'r> = LibsqlValueRef<'r>;
    type Arguments<'q> = LibsqlArguments<'q>;
    type ArgumentBuffer<'q> = ();
    type Statement<'q> = LibsqlStatement<'q>;

    const NAME: &'static str = "Libsql";
    const URL_SCHEMES: &'static [&'static str] = &["libsql"];
}

impl<'a> Executor<'a> for &'a mut LibsqlConnection {
    type Database = Libsql;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures_core::stream::BoxStream<
        'e,
        Result<
            sqlx_core::Either<
                <Self::Database as Database>::QueryResult,
                <Self::Database as Database>::Row,
            >,
            sqlx_core::Error,
        >,
    >
    where
        'a: 'e,
        E: 'q + sqlx_core::executor::Execute<'q, Self::Database>,
    {
        let sql = query.sql();

        Box::pin(async_stream::stream! {
            let mut rows = self.conn.query(sql, ()).await.unwrap();

            while let Some(row) = rows.next().await.unwrap() {
                yield Ok(Either::Right(LibsqlRow { row }));
            }
        })
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Self::Database as Database>::Row>, sqlx_core::Error>>
    where
        'a: 'e,
        E: 'q + sqlx_core::executor::Execute<'q, Self::Database>,
    {
        todo!()
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement<'q>, sqlx_core::Error>>
    where
        'a: 'e,
    {
        todo!()
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<sqlx_core::describe::Describe<Self::Database>, sqlx_core::Error>>
    where
        'a: 'e,
    {
        todo!()
    }
}

pub type LibsqlPool = sqlx_core::pool::Pool<Libsql>;

sqlx_core::impl_into_arguments_for_arguments!(LibsqlArguments<'q>);
sqlx_core::impl_column_index_for_row!(LibsqlRow);
sqlx_core::impl_column_index_for_statement!(LibsqlStatement);
sqlx_core::impl_acquire!(Libsql, LibsqlConnection);

// required because some databases have a different handling of NULL
// borrowed from sqlx_sqlite
sqlx_core::impl_encode_for_option!(Libsql);

#[derive(Debug)]
pub struct LibsqlConnection {
    conn: libsql::Connection,
}

impl Connection for LibsqlConnection {
    type Database = Libsql;

    type Options = LibsqlConnectionOptions;

    fn close(self) -> BoxFuture<'static, Result<(), sqlx_core::Error>> {
        todo!()
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), sqlx_core::Error>> {
        todo!()
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        Box::pin(async { Ok(()) })
    }

    fn begin(
        &mut self,
    ) -> BoxFuture<
        '_,
        Result<sqlx_core::transaction::Transaction<'_, Self::Database>, sqlx_core::Error>,
    >
    where
        Self: Sized,
    {
        todo!()
    }

    fn shrink_buffers(&mut self) {
        todo!()
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        todo!()
    }

    fn should_flush(&self) -> bool {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct LibsqlConnectionOptions {
    url: String,
}

impl ConnectOptions for LibsqlConnectionOptions {
    type Connection = LibsqlConnection;

    fn from_url(url: &sqlx_core::Url) -> Result<Self, sqlx_core::Error> {
        todo!()
    }

    fn connect(&self) -> BoxFuture<'_, Result<Self::Connection, sqlx_core::Error>>
    where
        Self::Connection: Sized,
    {
        let url = self.url.clone();

        Box::pin(async {
            let db = libsql::Builder::new_remote(url, "".to_string())
                .build()
                .await
                .unwrap();
            let conn = db.connect().unwrap();
            Ok(LibsqlConnection { conn })
        })
    }

    fn log_statements(self, level: log::LevelFilter) -> Self {
        todo!()
    }

    fn log_slow_statements(self, level: log::LevelFilter, duration: std::time::Duration) -> Self {
        todo!()
    }
}

impl FromStr for LibsqlConnectionOptions {
    type Err = sqlx_core::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(LibsqlConnectionOptions { url: s.to_string() })
    }
}

pub struct LibsqlTransactionManager {}

impl TransactionManager for LibsqlTransactionManager {
    type Database = Libsql;

    fn begin(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        todo!()
    }

    fn commit(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        todo!()
    }

    fn rollback(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> BoxFuture<'_, Result<(), sqlx_core::Error>> {
        todo!()
    }

    fn start_rollback(conn: &mut <Self::Database as Database>::Connection) {
        todo!()
    }
}

pub struct LibsqlRow {
    row: libsql::Row,
}

impl Deref for LibsqlRow {
    type Target = libsql::Row;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

impl Row for LibsqlRow {
    type Database = Libsql;

    fn columns(&self) -> &[<Self::Database as Database>::Column] {
        todo!()
    }

    fn try_get_raw<I>(
        &self,
        index: I,
    ) -> Result<<Self::Database as Database>::ValueRef<'_>, sqlx_core::Error>
    where
        I: sqlx_core::column::ColumnIndex<Self>,
    {
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct LibsqlQueryResult {
    changes: u64,
    last_insert_rowid: i64,
}

impl LibsqlQueryResult {
    pub fn rows_affected(&self) -> u64 {
        self.changes
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid
    }
}

impl Extend<LibsqlQueryResult> for LibsqlQueryResult {
    fn extend<T: IntoIterator<Item = LibsqlQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.changes += elem.changes;
            self.last_insert_rowid = elem.last_insert_rowid;
        }
    }
}

#[derive(Debug)]
pub struct LibsqlColumn {}

impl Column for LibsqlColumn {
    type Database = Libsql;

    fn ordinal(&self) -> usize {
        todo!()
    }

    fn name(&self) -> &str {
        todo!()
    }

    fn type_info(&self) -> &<Self::Database as Database>::TypeInfo {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LibsqlTypeInfo {}

impl TypeInfo for LibsqlTypeInfo {
    fn is_null(&self) -> bool {
        todo!()
    }

    fn name(&self) -> &str {
        todo!()
    }
}

impl std::fmt::Display for LibsqlTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LibsqlTypeInfo").finish()
    }
}

pub struct LibsqlValue {}

impl Value for LibsqlValue {
    type Database = Libsql;

    fn as_ref(&self) -> <Self::Database as Database>::ValueRef<'_> {
        todo!()
    }

    fn type_info(&self) -> std::borrow::Cow<'_, <Self::Database as Database>::TypeInfo> {
        todo!()
    }

    fn is_null(&self) -> bool {
        todo!()
    }
}

pub struct LibsqlValueRef<'a> {
    _f: &'a (),
}

impl<'a> ValueRef<'a> for LibsqlValueRef<'a> {
    type Database = Libsql;

    fn to_owned(&self) -> <Self::Database as Database>::Value {
        todo!()
    }

    fn type_info(&self) -> std::borrow::Cow<'_, <Self::Database as Database>::TypeInfo> {
        todo!()
    }

    fn is_null(&self) -> bool {
        todo!()
    }
}

#[derive(Default)]
pub struct LibsqlArguments<'a> {
    _a: PhantomData<&'a ()>,
}

impl<'a> Arguments<'a> for LibsqlArguments<'a> {
    type Database = Libsql;

    fn reserve(&mut self, additional: usize, size: usize) {
        todo!()
    }

    fn add<T>(&mut self, value: T) -> Result<(), sqlx_core::error::BoxDynError>
    where
        T: 'a
            + sqlx_core::encode::Encode<'a, Self::Database>
            + sqlx_core::types::Type<Self::Database>,
    {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }
}

impl<'a> Encode<'a, Libsql> for String {
    fn encode_by_ref(
        &self,
        buf: &mut (),
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        todo!()
    }
}

impl Type<Libsql> for String {
    fn type_info() -> <Libsql as Database>::TypeInfo {
        todo!()
    }
}

impl<'a> Encode<'a, Libsql> for i64 {
    fn encode_by_ref(
        &self,
        buf: &mut (),
    ) -> Result<sqlx_core::encode::IsNull, sqlx_core::error::BoxDynError> {
        todo!()
    }
}

impl Type<Libsql> for i64 {
    fn type_info() -> <Libsql as Database>::TypeInfo {
        todo!()
    }
}

pub struct LibsqlStatement<'a> {
    _a: PhantomData<&'a ()>,
}

impl<'a> Statement<'a> for LibsqlStatement<'a> {
    type Database = Libsql;

    fn to_owned(&self) -> <Self::Database as Database>::Statement<'static> {
        todo!()
    }

    fn sql(&self) -> &str {
        todo!()
    }

    fn parameters(
        &self,
    ) -> Option<sqlx_core::Either<&[<Self::Database as Database>::TypeInfo], usize>> {
        todo!()
    }

    fn columns(&self) -> &[<Self::Database as Database>::Column] {
        todo!()
    }

    fn query(
        &self,
    ) -> sqlx_core::query::Query<'_, Self::Database, <Self::Database as Database>::Arguments<'_>>
    {
        todo!()
    }

    fn query_with<'s, A>(&'s self, arguments: A) -> sqlx_core::query::Query<'s, Self::Database, A>
    where
        A: sqlx_core::arguments::IntoArguments<'s, Self::Database>,
    {
        todo!()
    }

    fn query_as<O>(
        &self,
    ) -> sqlx_core::query_as::QueryAs<
        '_,
        Self::Database,
        O,
        <Self::Database as Database>::Arguments<'_>,
    >
    where
        O: for<'r> sqlx_core::from_row::FromRow<'r, <Self::Database as Database>::Row>,
    {
        todo!()
    }

    fn query_as_with<'s, O, A>(
        &'s self,
        arguments: A,
    ) -> sqlx_core::query_as::QueryAs<'s, Self::Database, O, A>
    where
        O: for<'r> sqlx_core::from_row::FromRow<'r, <Self::Database as Database>::Row>,
        A: sqlx_core::arguments::IntoArguments<'s, Self::Database>,
    {
        todo!()
    }

    fn query_scalar<O>(
        &self,
    ) -> sqlx_core::query_scalar::QueryScalar<
        '_,
        Self::Database,
        O,
        <Self::Database as Database>::Arguments<'_>,
    >
    where
        (O,): for<'r> sqlx_core::from_row::FromRow<'r, <Self::Database as Database>::Row>,
    {
        todo!()
    }

    fn query_scalar_with<'s, O, A>(
        &'s self,
        arguments: A,
    ) -> sqlx_core::query_scalar::QueryScalar<'s, Self::Database, O, A>
    where
        (O,): for<'r> sqlx_core::from_row::FromRow<'r, <Self::Database as Database>::Row>,
        A: sqlx_core::arguments::IntoArguments<'s, Self::Database>,
    {
        todo!()
    }
}
