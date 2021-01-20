//! [`ToSql`] and [`FromSql`] implementation for [`url::Url`].
use crate::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use crate::Result;
use url::Url;

/// Serialize `Url` to text.
impl ToSql for Url {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_str()))
    }
}

/// Deserialize text to `Url`.
impl FromSql for Url {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => {
                let s = std::str::from_utf8(s).map_err(|e| FromSqlError::Other(Box::new(e)))?;
                Url::parse(s).map_err(|e| FromSqlError::Other(Box::new(e)))
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{params, Connection, Error, Result};
    use url::{ParseError, Url};

    fn checked_memory_handle() -> Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE urls (i INTEGER, v TEXT)")?;
        Ok(db)
    }

    fn get_url(db: &Connection, id: i64) -> Result<Url> {
        db.query_row("SELECT v FROM urls WHERE i = ?", [id], |r| r.get(0))
    }

    #[test]
    fn test_sql_url() -> Result<()> {
        let db = &checked_memory_handle()?;

        let url0 = Url::parse("http://www.example1.com").unwrap();
        let url1 = Url::parse("http://www.example1.com/👌").unwrap();
        let url2 = "http://www.example2.com/👌";

        db.execute(
            "INSERT INTO urls (i, v) VALUES (0, ?), (1, ?), (2, ?), (3, ?)",
            // also insert a non-hex encoded url (which might be present if it was
            // inserted separately)
            params![url0, url1, url2, "illegal"],
        )?;

        assert_eq!(get_url(db, 0)?, url0);

        assert_eq!(get_url(db, 1)?, url1);

        // Should successfully read it, even though it wasn't inserted as an
        // escaped url.
        let out_url2: Url = get_url(db, 2)?;
        assert_eq!(out_url2, Url::parse(url2).unwrap());

        // Make sure the conversion error comes through correctly.
        let err = get_url(db, 3).unwrap_err();
        match err {
            Error::FromSqlConversionFailure(_, _, e) => {
                assert_eq!(
                    *e.downcast::<ParseError>().unwrap(),
                    ParseError::RelativeUrlWithoutBase,
                );
            }
            e => {
                panic!("Expected conversion failure, got {}", e);
            }
        }
        Ok(())
    }
}
