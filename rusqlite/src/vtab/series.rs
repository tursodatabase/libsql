//! `feature = "series"` Generate series virtual table.
//!
//! Port of C [generate series
//! "function"](http://www.sqlite.org/cgi/src/finfo?name=ext/misc/series.c):
//! https://www.sqlite.org/series.html
use std::default::Default;
use std::os::raw::c_int;

use crate::ffi;
use crate::types::Type;
use crate::vtab::{
    eponymous_only_module, Context, IndexConstraintOp, IndexInfo, Module, VTab, VTabConnection,
    VTabCursor, Values,
};
use crate::{Connection, Result};

/// `feature = "series"` Register the "generate_series" module.
pub fn load_module(conn: &Connection) -> Result<()> {
    let aux: Option<()> = None;
    conn.create_module("generate_series", &SERIES_MODULE, aux)
}

lazy_static::lazy_static! {
    static ref SERIES_MODULE: Module<SeriesTab> = eponymous_only_module::<SeriesTab>(1);
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

impl VTab for SeriesTab {
    type Aux = ();
    type Cursor = SeriesTabCursor;

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
        // Index of the start= constraint
        let mut start_idx = None;
        // Index of the stop= constraint
        let mut stop_idx = None;
        // Index of the step= constraint
        let mut step_idx = None;
        for (i, constraint) in info.constraints().enumerate() {
            if !constraint.is_usable() {
                continue;
            }
            if constraint.operator() != IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ {
                continue;
            }
            match constraint.column() {
                SERIES_COLUMN_START => {
                    start_idx = Some(i);
                    idx_num |= QueryPlanFlags::START;
                }
                SERIES_COLUMN_STOP => {
                    stop_idx = Some(i);
                    idx_num |= QueryPlanFlags::STOP;
                }
                SERIES_COLUMN_STEP => {
                    step_idx = Some(i);
                    idx_num |= QueryPlanFlags::STEP;
                }
                _ => {}
            };
        }

        let mut num_of_arg = 0;
        if let Some(start_idx) = start_idx {
            num_of_arg += 1;
            let mut constraint_usage = info.constraint_usage(start_idx);
            constraint_usage.set_argv_index(num_of_arg);
            constraint_usage.set_omit(true);
        }
        if let Some(stop_idx) = stop_idx {
            num_of_arg += 1;
            let mut constraint_usage = info.constraint_usage(stop_idx);
            constraint_usage.set_argv_index(num_of_arg);
            constraint_usage.set_omit(true);
        }
        if let Some(step_idx) = step_idx {
            num_of_arg += 1;
            let mut constraint_usage = info.constraint_usage(step_idx);
            constraint_usage.set_argv_index(num_of_arg);
            constraint_usage.set_omit(true);
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
                    if order_by.is_order_by_desc() {
                        idx_num |= QueryPlanFlags::DESC;
                    }
                    true
                } else {
                    false
                }
            };
            if order_by_consumed {
                info.set_order_by_consumed(true);
            }
        } else {
            info.set_estimated_cost(2_147_483_647f64);
            info.set_estimated_rows(2_147_483_647);
        }
        info.set_idx_num(idx_num.bits());
        Ok(())
    }

    fn open(&self) -> Result<SeriesTabCursor> {
        Ok(SeriesTabCursor::new())
    }
}

/// A cursor for the Series virtual table
#[derive(Default)]
#[repr(C)]
struct SeriesTabCursor {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab_cursor,
    /// True to count down rather than up
    is_desc: bool,
    /// The rowid
    row_id: i64,
    /// Current value ("value")
    value: i64,
    /// Mimimum value ("start")
    min_value: i64,
    /// Maximum value ("stop")
    max_value: i64,
    /// Increment ("step")
    step: i64,
}

impl SeriesTabCursor {
    fn new() -> SeriesTabCursor {
        SeriesTabCursor::default()
    }
}
impl VTabCursor for SeriesTabCursor {
    fn filter(&mut self, idx_num: c_int, _idx_str: Option<&str>, args: &Values<'_>) -> Result<()> {
        let idx_num = QueryPlanFlags::from_bits_truncate(idx_num);
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
            if self.step < 1 {
                self.step = 1;
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
    use crate::{Connection, NO_PARAMS};

    #[test]
    fn test_series_module() {
        let version = unsafe { ffi::sqlite3_libversion_number() };
        if version < 3_008_012 {
            return;
        }

        let db = Connection::open_in_memory().unwrap();
        series::load_module(&db).unwrap();

        let mut s = db.prepare("SELECT * FROM generate_series(0,20,5)").unwrap();

        let series = s.query_map(NO_PARAMS, |row| row.get::<_, i32>(0)).unwrap();

        let mut expected = 0;
        for value in series {
            assert_eq!(expected, value.unwrap());
            expected += 5;
        }
    }
}
