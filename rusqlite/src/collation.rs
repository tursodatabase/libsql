//! `feature = "collation"` Add, remove, or modify a collation
use std::cmp::Ordering;
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, UnwindSafe};
use std::ptr;
use std::slice;

use crate::ffi;
use crate::{str_to_cstring, Connection, InnerConnection, Result};

// FIXME copy/paste from function.rs
unsafe extern "C" fn free_boxed_value<T>(p: *mut c_void) {
    drop(Box::from_raw(p as *mut T));
}

impl Connection {
    /// `feature = "collation"` Add or modify a collation.
    pub fn create_collation<C>(&self, collation_name: &str, x_compare: C) -> Result<()>
    where
        C: Fn(&str, &str) -> Ordering + Send + UnwindSafe + 'static,
    {
        self.db
            .borrow_mut()
            .create_collation(collation_name, x_compare)
    }

    /// `feature = "collation"` Collation needed callback
    pub fn collation_needed(
        &self,
        x_coll_needed: fn(&Connection, &str) -> Result<()>,
    ) -> Result<()> {
        self.db.borrow_mut().collation_needed(x_coll_needed)
    }

    /// `feature = "collation"` Remove collation.
    pub fn remove_collation(&self, collation_name: &str) -> Result<()> {
        self.db.borrow_mut().remove_collation(collation_name)
    }
}

impl InnerConnection {
    fn create_collation<C>(&mut self, collation_name: &str, x_compare: C) -> Result<()>
    where
        C: Fn(&str, &str) -> Ordering + Send + UnwindSafe + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<C>(
            arg1: *mut c_void,
            arg2: c_int,
            arg3: *const c_void,
            arg4: c_int,
            arg5: *const c_void,
        ) -> c_int
        where
            C: Fn(&str, &str) -> Ordering,
        {
            use std::str;

            let r = catch_unwind(|| {
                let boxed_f: *mut C = arg1 as *mut C;
                assert!(!boxed_f.is_null(), "Internal error - null function pointer");
                let s1 = {
                    let c_slice = slice::from_raw_parts(arg3 as *const u8, arg2 as usize);
                    str::from_utf8_unchecked(c_slice)
                };
                let s2 = {
                    let c_slice = slice::from_raw_parts(arg5 as *const u8, arg4 as usize);
                    str::from_utf8_unchecked(c_slice)
                };
                (*boxed_f)(s1, s2)
            });
            let t = match r {
                Err(_) => {
                    return -1; // FIXME How ?
                }
                Ok(r) => r,
            };

            match t {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }
        }

        let boxed_f: *mut C = Box::into_raw(Box::new(x_compare));
        let c_name = str_to_cstring(collation_name)?;
        let flags = ffi::SQLITE_UTF8;
        let r = unsafe {
            ffi::sqlite3_create_collation_v2(
                self.db(),
                c_name.as_ptr(),
                flags,
                boxed_f as *mut c_void,
                Some(call_boxed_closure::<C>),
                Some(free_boxed_value::<C>),
            )
        };
        self.decode_result(r)
    }

    fn collation_needed(
        &mut self,
        x_coll_needed: fn(&Connection, &str) -> Result<()>,
    ) -> Result<()> {
        use std::mem;
        unsafe extern "C" fn collation_needed_callback(
            arg1: *mut c_void,
            arg2: *mut ffi::sqlite3,
            e_text_rep: c_int,
            arg3: *const c_char,
        ) {
            use std::ffi::CStr;
            use std::str;

            if e_text_rep != ffi::SQLITE_UTF8 {
                // TODO: validate
                return;
            }

            let callback: fn(&Connection, &str) -> Result<()> = mem::transmute(arg1);
            let res = catch_unwind(|| {
                let conn = Connection::from_handle(arg2).unwrap();
                let collation_name = {
                    let c_slice = CStr::from_ptr(arg3).to_bytes();
                    str::from_utf8_unchecked(c_slice)
                };
                callback(&conn, collation_name)
            });
            if res.is_err() {
                return; // FIXME How ?
            }
        }

        let r = unsafe {
            ffi::sqlite3_collation_needed(
                self.db(),
                mem::transmute(x_coll_needed),
                Some(collation_needed_callback),
            )
        };
        self.decode_result(r)
    }

    fn remove_collation(&mut self, collation_name: &str) -> Result<()> {
        let c_name = str_to_cstring(collation_name)?;
        let r = unsafe {
            ffi::sqlite3_create_collation_v2(
                self.db(),
                c_name.as_ptr(),
                ffi::SQLITE_UTF8,
                ptr::null_mut(),
                None,
                None,
            )
        };
        self.decode_result(r)
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, Result, NO_PARAMS};
    use fallible_streaming_iterator::FallibleStreamingIterator;
    use std::cmp::Ordering;
    use unicase::UniCase;

    fn unicase_compare(s1: &str, s2: &str) -> Ordering {
        UniCase::new(s1).cmp(&UniCase::new(s2))
    }

    #[test]
    fn test_unicase() {
        let db = Connection::open_in_memory().unwrap();

        db.create_collation("unicase", unicase_compare).unwrap();

        collate(db);
    }

    fn collate(db: Connection) {
        db.execute_batch(
            "CREATE TABLE foo (bar);
             INSERT INTO foo (bar) VALUES ('Maße');
             INSERT INTO foo (bar) VALUES ('MASSE');",
        )
        .unwrap();
        let mut stmt = db
            .prepare("SELECT DISTINCT bar COLLATE unicase FROM foo ORDER BY 1")
            .unwrap();
        let rows = stmt.query(NO_PARAMS).unwrap();
        assert_eq!(rows.count().unwrap(), 1);
    }

    fn collation_needed(db: &Connection, collation_name: &str) -> Result<()> {
        if "unicase" == collation_name {
            db.create_collation(collation_name, unicase_compare)
        } else {
            Ok(())
        }
    }

    #[test]
    fn test_collation_needed() {
        let db = Connection::open_in_memory().unwrap();
        db.collation_needed(collation_needed).unwrap();
        collate(db);
    }
}
