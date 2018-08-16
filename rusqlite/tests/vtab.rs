//! Ensure Virtual tables can be declared outside `rusqlite` crate.

#[cfg(feature = "vtab")]
extern crate rusqlite;

#[cfg(feature = "vtab")]
#[test]
fn test_dummy_module() {
    use rusqlite::vtab::{
        eponymous_only_module, sqlite3_vtab, sqlite3_vtab_cursor, Context, IndexInfo, VTab,
        VTabConnection, VTabCursor, Values,
    };
    use rusqlite::{version_number, Connection, Result};
    use std::os::raw::c_int;

    let module = eponymous_only_module::<DummyTab>(1);

    #[repr(C)]
    struct DummyTab {
        /// Base class. Must be first
        base: sqlite3_vtab,
    }

    impl VTab for DummyTab {
        type Aux = ();
        type Cursor = DummyTabCursor;

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

        fn open(&self) -> Result<DummyTabCursor> {
            Ok(DummyTabCursor::default())
        }
    }

    #[derive(Default)]
    #[repr(C)]
    struct DummyTabCursor {
        /// Base class. Must be first
        base: sqlite3_vtab_cursor,
        /// The rowid
        row_id: i64,
    }

    impl VTabCursor for DummyTabCursor {
        fn filter(
            &mut self,
            _idx_num: c_int,
            _idx_str: Option<&str>,
            _args: &Values,
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

    let db = Connection::open_in_memory().unwrap();

    db.create_module::<DummyTab>("dummy", &module, None)
        .unwrap();

    let version = version_number();
    if version < 3008012 {
        return;
    }

    let mut s = db.prepare("SELECT * FROM dummy()").unwrap();

    let dummy = s.query_row(&[], |row| row.get::<_, i32>(0)).unwrap();
    assert_eq!(1, dummy);
}
