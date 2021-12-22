//! Generate series virtual table.
//!
//! Port of C [generate series
//! "function"](http://www.sqlite.org/cgi/src/finfo?name=ext/misc/series.c):
//! `https://www.sqlite.org/series.html`
use std::default::Default;
use std::marker::PhantomData;
use std::os::raw::c_int;

use crate::ffi;
use crate::types::Type;
use crate::vtab::{
    eponymous_only_module, Context, IndexConstraintOp, IndexInfo, VTab, VTabConnection, VTabCursor,
    Values,
};
use crate::{Connection, Error, Result};

/// Register the "generate_series" module.
pub fn load_module(conn: &Connection) -> Result<()> {
    let aux: Option<()> = None;
    conn.create_module("generate_series", eponymous_only_module::<SeriesTab>(), aux)
}

// Column numbers
// const SERIES_COLUMN_VALUE : c_int = 0;
const SERIES_COLUMN_START: c_int = 1;
const SERIES_COLUMN_STOP: c_int = 2;
const SERIES_COLUMN_STEP: c_int = 3;

bitflags::bitflags! {
    #[repr(C)]
    struct QueryPlanFlags: ::std::os::raw::c_int {
        // start = $value  -- constraint exists
        const START = 1;
        // stop = $value   -- constraint exists
        const STOP  = 2;
        // step = $value   -- constraint exists
        const STEP  = 4;
        // output in descending order
        const DESC  = 8;
        // output in ascending order
        const ASC  = 16;
        // Both start and stop
        const BOTH  = QueryPlanFlags::START.bits | QueryPlanFlags::STOP.bits;
    }
}

/// An instance of the Series virtual table
#[repr(C)]
struct SeriesTab {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab,
}

unsafe impl<'vtab> VTab<'vtab> for SeriesTab {
    type Aux = ();
    type Cursor = SeriesTabCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        _aux: Option<&()>,
        _args: &[&[u8]],
    ) -> Result<(String, SeriesTab)> {
        let vtab = SeriesTab {
            base: ffi::sqlite3_vtab::default(),
        };
        Ok((
            "CREATE TABLE x(value,start hidden,stop hidden,step hidden)".to_owned(),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // The query plan bitmask
        let mut idx_num: QueryPlanFlags = QueryPlanFlags::empty();
        // Mask of unusable constraints
        let mut unusable_mask: QueryPlanFlags = QueryPlanFlags::empty();
        // Constraints on start, stop, and step
        let mut a_idx: [Option<usize>; 3] = [None, None, None];
        for (i, constraint) in info.constraints().enumerate() {
            if constraint.column() < SERIES_COLUMN_START {
                continue;
            }
            let (i_col, i_mask) = match constraint.column() {
                SERIES_COLUMN_START => (0, QueryPlanFlags::START),
                SERIES_COLUMN_STOP => (1, QueryPlanFlags::STOP),
                SERIES_COLUMN_STEP => (2, QueryPlanFlags::STEP),
                _ => {
                    unreachable!()
                }
            };
            if !constraint.is_usable() {
                unusable_mask |= i_mask;
            } else if constraint.operator() == IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ {
                idx_num |= i_mask;
                a_idx[i_col] = Some(i);
            }
        }
        // Number of arguments that SeriesTabCursor::filter expects
        let mut n_arg = 0;
        for j in a_idx.iter().flatten() {
            n_arg += 1;
            let mut constraint_usage = info.constraint_usage(*j);
            constraint_usage.set_argv_index(n_arg);
            constraint_usage.set_omit(true);
        }
        if !(unusable_mask & !idx_num).is_empty() {
            return Err(Error::SqliteFailure(
                ffi::Error::new(ffi::SQLITE_CONSTRAINT),
                None,
            ));
        }
        if idx_num.contains(QueryPlanFlags::BOTH) {
            // Both start= and stop= boundaries are available.
            info.set_estimated_cost(f64::from(
                2 - if idx_num.contains(QueryPlanFlags::STEP) {
                    1
                } else {
                    0
                },
            ));
            info.set_estimated_rows(1000);
            let order_by_consumed = {
                let mut order_bys = info.order_bys();
                if let Some(order_by) = order_bys.next() {
                    if order_by.column() == 0 {
                        if order_by.is_order_by_desc() {
                            idx_num |= QueryPlanFlags::DESC;
                        } else {
                            idx_num |= QueryPlanFlags::ASC;
                        }
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };
            if order_by_consumed {
                info.set_order_by_consumed(true);
            }
        } else {
            // If either boundary is missing, we have to generate a huge span
            // of numbers.  Make this case very expensive so that the query
            // planner will work hard to avoid it.
            info.set_estimated_rows(2_147_483_647);
        }
        info.set_idx_num(idx_num.bits());
        Ok(())
    }

    fn open(&self) -> Result<SeriesTabCursor<'_>> {
        Ok(SeriesTabCursor::new())
    }
}

/// A cursor for the Series virtual table
#[repr(C)]
struct SeriesTabCursor<'vtab> {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab_cursor,
    /// True to count down rather than up
    is_desc: bool,
    /// The rowid
    row_id: i64,
    /// Current value ("value")
    value: i64,
    /// Minimum value ("start")
    min_value: i64,
    /// Maximum value ("stop")
    max_value: i64,
    /// Increment ("step")
    step: i64,
    phantom: PhantomData<&'vtab SeriesTab>,
}

impl SeriesTabCursor<'_> {
    fn new<'vtab>() -> SeriesTabCursor<'vtab> {
        SeriesTabCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            is_desc: false,
            row_id: 0,
            value: 0,
            min_value: 0,
            max_value: 0,
            step: 0,
            phantom: PhantomData,
        }
    }
}
#[allow(clippy::comparison_chain)]
unsafe impl VTabCursor for SeriesTabCursor<'_> {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Values<'_>) -> Result<()> {
        let mut idx_num = QueryPlanFlags::from_bits_truncate(idx_num);
        let mut i = 0;
        if idx_num.contains(QueryPlanFlags::START) {
            self.min_value = args.get(i)?;
            i += 1;
        } else {
            self.min_value = 0;
        }
        if idx_num.contains(QueryPlanFlags::STOP) {
            self.max_value = args.get(i)?;
            i += 1;
        } else {
            self.max_value = 0xffff_ffff;
        }
        if idx_num.contains(QueryPlanFlags::STEP) {
            self.step = args.get(i)?;
            if self.step == 0 {
                self.step = 1;
            } else if self.step < 0 {
                self.step = -self.step;
                if !idx_num.contains(QueryPlanFlags::ASC) {
                    idx_num |= QueryPlanFlags::DESC;
                }
            }
        } else {
            self.step = 1;
        };
        for arg in args.iter() {
            if arg.data_type() == Type::Null {
                // If any of the constraints have a NULL value, then return no rows.
                self.min_value = 1;
                self.max_value = 0;
                break;
            }
        }
        self.is_desc = idx_num.contains(QueryPlanFlags::DESC);
        if self.is_desc {
            self.value = self.max_value;
            if self.step > 0 {
                self.value -= (self.max_value - self.min_value) % self.step;
            }
        } else {
            self.value = self.min_value;
        }
        self.row_id = 1;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        if self.is_desc {
            self.value -= self.step;
        } else {
            self.value += self.step;
        }
        self.row_id += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        if self.is_desc {
            self.value < self.min_value
        } else {
            self.value > self.max_value
        }
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        let x = match i {
            SERIES_COLUMN_START => self.min_value,
            SERIES_COLUMN_STOP => self.max_value,
            SERIES_COLUMN_STEP => self.step,
            _ => self.value,
        };
        ctx.set_result(&x)
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.row_id)
    }
}

#[cfg(test)]
mod test {
    use crate::ffi;
    use crate::vtab::series;
    use crate::{Connection, Result};
    use fallible_iterator::FallibleIterator;

    #[test]
    fn test_series_module() -> Result<()> {
        let version = unsafe { ffi::sqlite3_libversion_number() };
        if version < 3_008_012 {
            return Ok(());
        }

        let db = Connection::open_in_memory()?;
        series::load_module(&db)?;

        let mut s = db.prepare("SELECT * FROM generate_series(0,20,5)")?;

        let series = s.query_map([], |row| row.get::<_, i32>(0))?;

        let mut expected = 0;
        for value in series {
            assert_eq!(expected, value?);
            expected += 5;
        }

        let mut s =
            db.prepare("SELECT * FROM generate_series WHERE start=1 AND stop=9 AND step=2")?;
        let series: Vec<i32> = s.query([])?.map(|r| r.get(0)).collect()?;
        assert_eq!(vec![1, 3, 5, 7, 9], series);
        let mut s = db.prepare("SELECT * FROM generate_series LIMIT 5")?;
        let series: Vec<i32> = s.query([])?.map(|r| r.get(0)).collect()?;
        assert_eq!(vec![0, 1, 2, 3, 4], series);
        let mut s = db.prepare("SELECT * FROM generate_series(0,32,5) ORDER BY value DESC")?;
        let series: Vec<i32> = s.query([])?.map(|r| r.get(0)).collect()?;
        assert_eq!(vec![30, 25, 20, 15, 10, 5, 0], series);

        Ok(())
    }
}
