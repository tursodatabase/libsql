//! Ensure Virtual tables can be declared outside `rusqlite` crate.

#[cfg(feature = "vtab")]
#[test]
fn test_dummy_module() -> rusqlite::Result<()> {
    use rusqlite::vtab::{
        eponymous_only_module, sqlite3_vtab, sqlite3_vtab_cursor, Context, IndexInfo, VTab,
        VTabConnection, VTabCursor, Values,
    };
    use rusqlite::{version_number, Connection, Result};
    use std::marker::PhantomData;
    use std::os::raw::c_int;

    let module = eponymous_only_module::<DummyTab>();

    #[repr(C)]
    struct DummyTab {
        /// Base class. Must be first
        base: sqlite3_vtab,
    }

    unsafe impl<'vtab> VTab<'vtab> for DummyTab {
        type Aux = ();
        type Cursor = DummyTabCursor<'vtab>;

        fn connect(
            _: &mut VTabConnection,
            _aux: Option<&()>,
            _args: &[&[u8]],
        ) -> Result<(String, DummyTab)> {
            let vtab = DummyTab {
                base: sqlite3_vtab::default(),
            };
            Ok(("CREATE TABLE x(value)".to_owned(), vtab))
        }

        fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
            info.set_estimated_cost(1.);
            Ok(())
        }

        fn open(&'vtab mut self) -> Result<DummyTabCursor<'vtab>> {
            Ok(DummyTabCursor::default())
        }
    }

    #[derive(Default)]
    #[repr(C)]
    struct DummyTabCursor<'vtab> {
        /// Base class. Must be first
        base: sqlite3_vtab_cursor,
        /// The rowid
        row_id: i64,
        phantom: PhantomData<&'vtab DummyTab>,
    }

    unsafe impl VTabCursor for DummyTabCursor<'_> {
        fn filter(
            &mut self,
            _idx_num: c_int,
            _idx_str: Option<&str>,
            _args: &Values<'_>,
        ) -> Result<()> {
            self.row_id = 1;
            Ok(())
        }

        fn next(&mut self) -> Result<()> {
            self.row_id += 1;
            Ok(())
        }

        fn eof(&self) -> bool {
            self.row_id > 1
        }

        fn column(&self, ctx: &mut Context, _: c_int) -> Result<()> {
            ctx.set_result(&self.row_id)
        }

        fn rowid(&self) -> Result<i64> {
            Ok(self.row_id)
        }
    }

    let db = Connection::open_in_memory()?;

    db.create_module::<DummyTab>("dummy", module, None)?;

    let version = version_number();
    if version < 3_009_000 {
        return Ok(());
    }

    let mut s = db.prepare("SELECT * FROM dummy()")?;

    let dummy = s.query_row([], |row| row.get::<_, i32>(0))?;
    assert_eq!(1, dummy);
    Ok(())
}
