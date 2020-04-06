//! `feature = "session"` [Session Extension](https://sqlite.org/sessionintro.html)
#![allow(non_camel_case_types)]

use std::ffi::CStr;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::os::raw::{c_char, c_int, c_uchar, c_void};
use std::panic::{catch_unwind, RefUnwindSafe};
use std::ptr;
use std::slice::{from_raw_parts, from_raw_parts_mut};

use fallible_streaming_iterator::FallibleStreamingIterator;

use crate::error::error_from_sqlite_code;
use crate::ffi;
use crate::hooks::Action;
use crate::types::ValueRef;
use crate::{errmsg_to_string, str_to_cstring, Connection, DatabaseName, Result};

// https://sqlite.org/session.html

/// `feature = "session"` An instance of this object is a session that can be used to record changes
/// to a database.
pub struct Session<'conn> {
    phantom: PhantomData<&'conn ()>,
    s: *mut ffi::sqlite3_session,
    filter: Option<Box<dyn Fn(&str) -> bool>>,
}

impl Session<'_> {
    /// Create a new session object
    pub fn new<'conn>(db: &'conn Connection) -> Result<Session<'conn>> {
        Session::new_with_name(db, DatabaseName::Main)
    }

    /// Create a new session object
    pub fn new_with_name<'conn>(
        db: &'conn Connection,
        name: DatabaseName<'_>,
    ) -> Result<Session<'conn>> {
        let name = name.to_cstring()?;

        let db = db.db.borrow_mut().db;

        let mut s = MaybeUninit::uninit();
        check!(unsafe { ffi::sqlite3session_create(db, name.as_ptr(), s.as_mut_ptr()) });
        let s: *mut ffi::sqlite3_session = unsafe { s.assume_init() };

        Ok(Session {
            phantom: PhantomData,
            s,
            filter: None,
        })
    }

    /// Set a table filter
    pub fn table_filter<F>(&mut self, filter: Option<F>)
    where
        F: Fn(&str) -> bool + Send + RefUnwindSafe + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<F>(
            p_arg: *mut c_void,
            tbl_str: *const c_char,
        ) -> c_int
        where
            F: Fn(&str) -> bool + RefUnwindSafe,
        {
            use std::str;

            let boxed_filter: *mut F = p_arg as *mut F;
            let tbl_name = {
                let c_slice = CStr::from_ptr(tbl_str).to_bytes();
                str::from_utf8_unchecked(c_slice)
            };
            if let Ok(true) = catch_unwind(|| (*boxed_filter)(tbl_name)) {
                1
            } else {
                0
            }
        }

        match filter {
            Some(filter) => {
                let boxed_filter = Box::new(filter);
                unsafe {
                    ffi::sqlite3session_table_filter(
                        self.s,
                        Some(call_boxed_closure::<F>),
                        &*boxed_filter as *const F as *mut _,
                    );
                }
                self.filter = Some(boxed_filter);
            }
            _ => {
                unsafe { ffi::sqlite3session_table_filter(self.s, None, ptr::null_mut()) }
                self.filter = None;
            }
        };
    }

    /// Attach a table. `None` means all tables.
    pub fn attach(&mut self, table: Option<&str>) -> Result<()> {
        let table = if let Some(table) = table {
            str_to_cstring(table)?.as_ptr()
        } else {
            ptr::null()
        };
        unsafe { check!(ffi::sqlite3session_attach(self.s, table)) };
        Ok(())
    }

    /// Generate a Changeset
    pub fn changeset(&mut self) -> Result<Changeset> {
        let mut n = 0;
        let mut cs = MaybeUninit::uninit();
        check!(unsafe { ffi::sqlite3session_changeset(self.s, &mut n, cs.as_mut_ptr()) });
        let cs: *mut c_void = unsafe { cs.assume_init() };
        Ok(Changeset { cs, n })
    }

    /// Write the set of changes represented by this session to `output`.
    pub fn changeset_strm(&mut self, output: &mut dyn Write) -> Result<()> {
        let output_ref = &output;
        check!(unsafe {
            ffi::sqlite3session_changeset_strm(
                self.s,
                Some(x_output),
                output_ref as *const &mut dyn Write as *mut c_void,
            )
        });
        Ok(())
    }

    /// Generate a Patchset
    pub fn patchset(&mut self) -> Result<Changeset> {
        let mut n = 0;
        let mut ps = MaybeUninit::uninit();
        check!(unsafe { ffi::sqlite3session_patchset(self.s, &mut n, ps.as_mut_ptr()) });
        let ps: *mut c_void = unsafe { ps.assume_init() };
        // TODO Validate: same struct
        Ok(Changeset { cs: ps, n })
    }

    /// Write the set of patches represented by this session to `output`.
    pub fn patchset_strm(&mut self, output: &mut dyn Write) -> Result<()> {
        let output_ref = &output;
        check!(unsafe {
            ffi::sqlite3session_patchset_strm(
                self.s,
                Some(x_output),
                output_ref as *const &mut dyn Write as *mut c_void,
            )
        });
        Ok(())
    }

    /// Load the difference between tables.
    pub fn diff(&mut self, from: DatabaseName<'_>, table: &str) -> Result<()> {
        let from = from.to_cstring()?;
        let table = str_to_cstring(table)?.as_ptr();
        unsafe {
            let mut errmsg = MaybeUninit::uninit();
            let r = ffi::sqlite3session_diff(self.s, from.as_ptr(), table, errmsg.as_mut_ptr());
            if r != ffi::SQLITE_OK {
                let errmsg: *mut c_char = errmsg.assume_init();
                let message = errmsg_to_string(&*errmsg);
                ffi::sqlite3_free(errmsg as *mut ::std::os::raw::c_void);
                return Err(error_from_sqlite_code(r, Some(message)));
            }
        }
        Ok(())
    }

    /// Test if a changeset has recorded any changes
    pub fn is_empty(&self) -> bool {
        unsafe { ffi::sqlite3session_isempty(self.s) != 0 }
    }

    /// Query the current state of the session
    pub fn is_enabled(&self) -> bool {
        unsafe { ffi::sqlite3session_enable(self.s, -1) != 0 }
    }

    /// Enable or disable the recording of changes
    pub fn set_enabled(&mut self, enabled: bool) {
        unsafe {
            ffi::sqlite3session_enable(self.s, if enabled { 1 } else { 0 });
        }
    }

    /// Query the current state of the indirect flag
    pub fn is_indirect(&self) -> bool {
        unsafe { ffi::sqlite3session_indirect(self.s, -1) != 0 }
    }

    /// Set or clear the indirect change flag
    pub fn set_indirect(&mut self, indirect: bool) {
        unsafe {
            ffi::sqlite3session_indirect(self.s, if indirect { 1 } else { 0 });
        }
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        if self.filter.is_some() {
            self.table_filter(None::<fn(&str) -> bool>);
        }
        unsafe { ffi::sqlite3session_delete(self.s) };
    }
}

/// `feature = "session"` Invert a changeset
pub fn invert_strm(input: &mut dyn Read, output: &mut dyn Write) -> Result<()> {
    let input_ref = &input;
    let output_ref = &output;
    check!(unsafe {
        ffi::sqlite3changeset_invert_strm(
            Some(x_input),
            input_ref as *const &mut dyn Read as *mut c_void,
            Some(x_output),
            output_ref as *const &mut dyn Write as *mut c_void,
        )
    });
    Ok(())
}

/// `feature = "session"` Combine two changesets
pub fn concat_strm(
    input_a: &mut dyn Read,
    input_b: &mut dyn Read,
    output: &mut dyn Write,
) -> Result<()> {
    let input_a_ref = &input_a;
    let input_b_ref = &input_b;
    let output_ref = &output;
    check!(unsafe {
        ffi::sqlite3changeset_concat_strm(
            Some(x_input),
            input_a_ref as *const &mut dyn Read as *mut c_void,
            Some(x_input),
            input_b_ref as *const &mut dyn Read as *mut c_void,
            Some(x_output),
            output_ref as *const &mut dyn Write as *mut c_void,
        )
    });
    Ok(())
}

/// `feature = "session"` Changeset or Patchset
pub struct Changeset {
    cs: *mut c_void,
    n: c_int,
}

impl Changeset {
    /// Invert a changeset
    pub fn invert(&self) -> Result<Changeset> {
        let mut n = 0;
        let mut cs = MaybeUninit::uninit();
        check!(unsafe { ffi::sqlite3changeset_invert(self.n, self.cs, &mut n, cs.as_mut_ptr()) });
        let cs: *mut c_void = unsafe { cs.assume_init() };
        Ok(Changeset { cs, n })
    }

    /// Create an iterator to traverse a changeset
    pub fn iter(&self) -> Result<ChangesetIter<'_>> {
        let mut it = MaybeUninit::uninit();
        check!(unsafe { ffi::sqlite3changeset_start(it.as_mut_ptr(), self.n, self.cs) });
        let it: *mut ffi::sqlite3_changeset_iter = unsafe { it.assume_init() };
        Ok(ChangesetIter {
            phantom: PhantomData,
            it,
            item: None,
        })
    }

    /// Concatenate two changeset objects
    pub fn concat(a: &Changeset, b: &Changeset) -> Result<Changeset> {
        let mut n = 0;
        let mut cs = MaybeUninit::uninit();
        check!(unsafe {
            ffi::sqlite3changeset_concat(a.n, a.cs, b.n, b.cs, &mut n, cs.as_mut_ptr())
        });
        let cs: *mut c_void = unsafe { cs.assume_init() };
        Ok(Changeset { cs, n })
    }
}

impl Drop for Changeset {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_free(self.cs);
        }
    }
}

/// `feature = "session"` Cursor for iterating over the elements of a changeset
/// or patchset.
pub struct ChangesetIter<'changeset> {
    phantom: PhantomData<&'changeset ()>,
    it: *mut ffi::sqlite3_changeset_iter,
    item: Option<ChangesetItem>,
}

impl ChangesetIter<'_> {
    /// Create an iterator on `input`
    pub fn start_strm<'input>(input: &&'input mut dyn Read) -> Result<ChangesetIter<'input>> {
        let mut it = MaybeUninit::uninit();
        check!(unsafe {
            ffi::sqlite3changeset_start_strm(
                it.as_mut_ptr(),
                Some(x_input),
                input as *const &mut dyn Read as *mut c_void,
            )
        });
        let it: *mut ffi::sqlite3_changeset_iter = unsafe { it.assume_init() };
        Ok(ChangesetIter {
            phantom: PhantomData,
            it,
            item: None,
        })
    }
}

impl FallibleStreamingIterator for ChangesetIter<'_> {
    type Error = crate::error::Error;
    type Item = ChangesetItem;

    fn advance(&mut self) -> Result<()> {
        let rc = unsafe { ffi::sqlite3changeset_next(self.it) };
        match rc {
            ffi::SQLITE_ROW => {
                self.item = Some(ChangesetItem { it: self.it });
                Ok(())
            }
            ffi::SQLITE_DONE => {
                self.item = None;
                Ok(())
            }
            code => Err(error_from_sqlite_code(code, None)),
        }
    }

    fn get(&self) -> Option<&ChangesetItem> {
        self.item.as_ref()
    }
}

/// `feature = "session"`
pub struct Operation<'item> {
    table_name: &'item str,
    number_of_columns: i32,
    code: Action,
    indirect: bool,
}

impl Operation<'_> {
    pub fn table_name(&self) -> &str {
        self.table_name
    }

    pub fn number_of_columns(&self) -> i32 {
        self.number_of_columns
    }

    pub fn code(&self) -> Action {
        self.code
    }

    pub fn indirect(&self) -> bool {
        self.indirect
    }
}

impl Drop for ChangesetIter<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3changeset_finalize(self.it);
        }
    }
}

/// `feature = "session"` An item passed to a conflict-handler by `Connection::apply`,
/// or an item generated by `ChangesetIter::next`.
// TODO enum ? Delete, Insert, Update, ...
pub struct ChangesetItem {
    it: *mut ffi::sqlite3_changeset_iter,
}

impl ChangesetItem {
    /// Obtain conflicting row values
    ///
    /// May only be called with an `SQLITE_CHANGESET_DATA` or
    /// `SQLITE_CHANGESET_CONFLICT` conflict handler callback.
    pub fn conflict(&self, col: usize) -> Result<ValueRef<'_>> {
        unsafe {
            let mut p_value = MaybeUninit::uninit();
            check!(ffi::sqlite3changeset_conflict(
                self.it,
                col as i32,
                p_value.as_mut_ptr()
            ));
            let p_value: *mut ffi::sqlite3_value = p_value.assume_init();
            Ok(ValueRef::from_value(p_value))
        }
    }

    /// Determine the number of foreign key constraint violations
    ///
    /// May only be called with an `SQLITE_CHANGESET_FOREIGN_KEY` conflict
    /// handler callback.
    pub fn fk_conflicts(&self) -> Result<i32> {
        unsafe {
            let mut p_out = 0;
            check!(ffi::sqlite3changeset_fk_conflicts(self.it, &mut p_out));
            Ok(p_out)
        }
    }

    /// Obtain new.* Values
    ///
    /// May only be called if the type of change is either `SQLITE_UPDATE` or
    /// `SQLITE_INSERT`.
    pub fn new_value(&self, col: usize) -> Result<ValueRef<'_>> {
        unsafe {
            let mut p_value = MaybeUninit::uninit();
            check!(ffi::sqlite3changeset_new(
                self.it,
                col as i32,
                p_value.as_mut_ptr()
            ));
            let p_value: *mut ffi::sqlite3_value = p_value.assume_init();
            Ok(ValueRef::from_value(p_value))
        }
    }

    /// Obtain old.* Values
    ///
    /// May only be called if the type of change is either `SQLITE_DELETE` or
    /// `SQLITE_UPDATE`.
    pub fn old_value(&self, col: usize) -> Result<ValueRef<'_>> {
        unsafe {
            let mut p_value = MaybeUninit::uninit();
            check!(ffi::sqlite3changeset_old(
                self.it,
                col as i32,
                p_value.as_mut_ptr()
            ));
            let p_value: *mut ffi::sqlite3_value = p_value.assume_init();
            Ok(ValueRef::from_value(p_value))
        }
    }

    /// Obtain the current operation
    pub fn op(&self) -> Result<Operation<'_>> {
        let mut number_of_columns = 0;
        let mut code = 0;
        let mut indirect = 0;
        let tab = unsafe {
            let mut pz_tab = MaybeUninit::uninit();
            check!(ffi::sqlite3changeset_op(
                self.it,
                pz_tab.as_mut_ptr(),
                &mut number_of_columns,
                &mut code,
                &mut indirect
            ));
            let pz_tab: *const c_char = pz_tab.assume_init();
            CStr::from_ptr(pz_tab)
        };
        let table_name = tab.to_str()?;
        Ok(Operation {
            table_name,
            number_of_columns,
            code: Action::from(code),
            indirect: indirect != 0,
        })
    }

    /// Obtain the primary key definition of a table
    pub fn pk(&self) -> Result<&[u8]> {
        let mut number_of_columns = 0;
        unsafe {
            let mut pks = MaybeUninit::uninit();
            check!(ffi::sqlite3changeset_pk(
                self.it,
                pks.as_mut_ptr(),
                &mut number_of_columns
            ));
            let pks: *mut c_uchar = pks.assume_init();
            Ok(from_raw_parts(pks, number_of_columns as usize))
        }
    }
}

/// `feature = "session"` Used to combine two or more changesets or
/// patchsets
pub struct Changegroup {
    cg: *mut ffi::sqlite3_changegroup,
}

impl Changegroup {
    pub fn new() -> Result<Self> {
        let mut cg = MaybeUninit::uninit();
        check!(unsafe { ffi::sqlite3changegroup_new(cg.as_mut_ptr()) });
        let cg: *mut ffi::sqlite3_changegroup = unsafe { cg.assume_init() };
        Ok(Changegroup { cg })
    }

    /// Add a changeset
    pub fn add(&mut self, cs: &Changeset) -> Result<()> {
        check!(unsafe { ffi::sqlite3changegroup_add(self.cg, cs.n, cs.cs) });
        Ok(())
    }

    /// Add a changeset read from `input` to this change group.
    pub fn add_stream(&mut self, input: &mut dyn Read) -> Result<()> {
        let input_ref = &input;
        check!(unsafe {
            ffi::sqlite3changegroup_add_strm(
                self.cg,
                Some(x_input),
                input_ref as *const &mut dyn Read as *mut c_void,
            )
        });
        Ok(())
    }

    /// Obtain a composite Changeset
    pub fn output(&mut self) -> Result<Changeset> {
        let mut n = 0;
        let mut output = MaybeUninit::uninit();
        check!(unsafe { ffi::sqlite3changegroup_output(self.cg, &mut n, output.as_mut_ptr()) });
        let output: *mut c_void = unsafe { output.assume_init() };
        Ok(Changeset { cs: output, n })
    }

    /// Write the combined set of changes to `output`.
    pub fn output_strm(&mut self, output: &mut dyn Write) -> Result<()> {
        let output_ref = &output;
        check!(unsafe {
            ffi::sqlite3changegroup_output_strm(
                self.cg,
                Some(x_output),
                output_ref as *const &mut dyn Write as *mut c_void,
            )
        });
        Ok(())
    }
}

impl Drop for Changegroup {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3changegroup_delete(self.cg);
        }
    }
}

impl Connection {
    /// `feature = "session"` Apply a changeset to a database
    pub fn apply<F, C>(&self, cs: &Changeset, filter: Option<F>, conflict: C) -> Result<()>
    where
        F: Fn(&str) -> bool + Send + RefUnwindSafe + 'static,
        C: Fn(ConflictType, ChangesetItem) -> ConflictAction + Send + RefUnwindSafe + 'static,
    {
        let db = self.db.borrow_mut().db;

        let filtered = filter.is_some();
        let tuple = &mut (filter, conflict);
        check!(unsafe {
            if filtered {
                ffi::sqlite3changeset_apply(
                    db,
                    cs.n,
                    cs.cs,
                    Some(call_filter::<F, C>),
                    Some(call_conflict::<F, C>),
                    tuple as *mut (Option<F>, C) as *mut c_void,
                )
            } else {
                ffi::sqlite3changeset_apply(
                    db,
                    cs.n,
                    cs.cs,
                    None,
                    Some(call_conflict::<F, C>),
                    tuple as *mut (Option<F>, C) as *mut c_void,
                )
            }
        });
        Ok(())
    }

    /// `feature = "session"` Apply a changeset to a database
    pub fn apply_strm<F, C>(
        &self,
        input: &mut dyn Read,
        filter: Option<F>,
        conflict: C,
    ) -> Result<()>
    where
        F: Fn(&str) -> bool + Send + RefUnwindSafe + 'static,
        C: Fn(ConflictType, ChangesetItem) -> ConflictAction + Send + RefUnwindSafe + 'static,
    {
        let input_ref = &input;
        let db = self.db.borrow_mut().db;

        let filtered = filter.is_some();
        let tuple = &mut (filter, conflict);
        check!(unsafe {
            if filtered {
                ffi::sqlite3changeset_apply_strm(
                    db,
                    Some(x_input),
                    input_ref as *const &mut dyn Read as *mut c_void,
                    Some(call_filter::<F, C>),
                    Some(call_conflict::<F, C>),
                    tuple as *mut (Option<F>, C) as *mut c_void,
                )
            } else {
                ffi::sqlite3changeset_apply_strm(
                    db,
                    Some(x_input),
                    input_ref as *const &mut dyn Read as *mut c_void,
                    None,
                    Some(call_conflict::<F, C>),
                    tuple as *mut (Option<F>, C) as *mut c_void,
                )
            }
        });
        Ok(())
    }
}

/// `feature = "session"` Constants passed to the conflict handler
#[repr(i32)]
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum ConflictType {
    UNKNOWN = -1,
    SQLITE_CHANGESET_DATA = ffi::SQLITE_CHANGESET_DATA,
    SQLITE_CHANGESET_NOTFOUND = ffi::SQLITE_CHANGESET_NOTFOUND,
    SQLITE_CHANGESET_CONFLICT = ffi::SQLITE_CHANGESET_CONFLICT,
    SQLITE_CHANGESET_CONSTRAINT = ffi::SQLITE_CHANGESET_CONSTRAINT,
    SQLITE_CHANGESET_FOREIGN_KEY = ffi::SQLITE_CHANGESET_FOREIGN_KEY,
}
impl From<i32> for ConflictType {
    fn from(code: i32) -> ConflictType {
        match code {
            ffi::SQLITE_CHANGESET_DATA => ConflictType::SQLITE_CHANGESET_DATA,
            ffi::SQLITE_CHANGESET_NOTFOUND => ConflictType::SQLITE_CHANGESET_NOTFOUND,
            ffi::SQLITE_CHANGESET_CONFLICT => ConflictType::SQLITE_CHANGESET_CONFLICT,
            ffi::SQLITE_CHANGESET_CONSTRAINT => ConflictType::SQLITE_CHANGESET_CONSTRAINT,
            ffi::SQLITE_CHANGESET_FOREIGN_KEY => ConflictType::SQLITE_CHANGESET_FOREIGN_KEY,
            _ => ConflictType::UNKNOWN,
        }
    }
}

/// `feature = "session"` Constants returned by the conflict handler
#[repr(i32)]
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum ConflictAction {
    SQLITE_CHANGESET_OMIT = ffi::SQLITE_CHANGESET_OMIT,
    SQLITE_CHANGESET_REPLACE = ffi::SQLITE_CHANGESET_REPLACE,
    SQLITE_CHANGESET_ABORT = ffi::SQLITE_CHANGESET_ABORT,
}

unsafe extern "C" fn call_filter<F, C>(p_ctx: *mut c_void, tbl_str: *const c_char) -> c_int
where
    F: Fn(&str) -> bool + Send + RefUnwindSafe + 'static,
    C: Fn(ConflictType, ChangesetItem) -> ConflictAction + Send + RefUnwindSafe + 'static,
{
    use std::str;

    let tuple: *mut (Option<F>, C) = p_ctx as *mut (Option<F>, C);
    let tbl_name = {
        let c_slice = CStr::from_ptr(tbl_str).to_bytes();
        str::from_utf8_unchecked(c_slice)
    };
    match *tuple {
        (Some(ref filter), _) => {
            if let Ok(true) = catch_unwind(|| filter(tbl_name)) {
                1
            } else {
                0
            }
        }
        _ => unimplemented!(),
    }
}

unsafe extern "C" fn call_conflict<F, C>(
    p_ctx: *mut c_void,
    e_conflict: c_int,
    p: *mut ffi::sqlite3_changeset_iter,
) -> c_int
where
    F: Fn(&str) -> bool + Send + RefUnwindSafe + 'static,
    C: Fn(ConflictType, ChangesetItem) -> ConflictAction + Send + RefUnwindSafe + 'static,
{
    let tuple: *mut (Option<F>, C) = p_ctx as *mut (Option<F>, C);
    let conflict_type = ConflictType::from(e_conflict);
    let item = ChangesetItem { it: p };
    if let Ok(action) = catch_unwind(|| (*tuple).1(conflict_type, item)) {
        action as c_int
    } else {
        ffi::SQLITE_CHANGESET_ABORT
    }
}

unsafe extern "C" fn x_input(p_in: *mut c_void, data: *mut c_void, len: *mut c_int) -> c_int {
    if p_in.is_null() {
        return ffi::SQLITE_MISUSE;
    }
    let bytes: &mut [u8] = from_raw_parts_mut(data as *mut u8, *len as usize);
    let input = p_in as *mut &mut dyn Read;
    match (*input).read(bytes) {
        Ok(n) => {
            *len = n as i32; // TODO Validate: n = 0 may not mean the reader will always no longer be able to
                             // produce bytes.
            ffi::SQLITE_OK
        }
        Err(_) => ffi::SQLITE_IOERR_READ, // TODO check if err is a (ru)sqlite Error => propagate
    }
}

unsafe extern "C" fn x_output(p_out: *mut c_void, data: *const c_void, len: c_int) -> c_int {
    if p_out.is_null() {
        return ffi::SQLITE_MISUSE;
    }
    // The sessions module never invokes an xOutput callback with the third
    // parameter set to a value less than or equal to zero.
    let bytes: &[u8] = from_raw_parts(data as *const u8, len as usize);
    let output = p_out as *mut &mut dyn Write;
    match (*output).write_all(bytes) {
        Ok(_) => ffi::SQLITE_OK,
        Err(_) => ffi::SQLITE_IOERR_WRITE, // TODO check if err is a (ru)sqlite Error => propagate
    }
}

#[cfg(test)]
mod test {
    use fallible_streaming_iterator::FallibleStreamingIterator;
    use std::io::Read;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{Changeset, ChangesetIter, ConflictAction, ConflictType, Session};
    use crate::hooks::Action;
    use crate::Connection;

    fn one_changeset() -> Changeset {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(t TEXT PRIMARY KEY NOT NULL);")
            .unwrap();

        let mut session = Session::new(&db).unwrap();
        assert!(session.is_empty());

        session.attach(None).unwrap();
        db.execute("INSERT INTO foo (t) VALUES (?);", &["bar"])
            .unwrap();

        session.changeset().unwrap()
    }

    fn one_changeset_strm() -> Vec<u8> {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(t TEXT PRIMARY KEY NOT NULL);")
            .unwrap();

        let mut session = Session::new(&db).unwrap();
        assert!(session.is_empty());

        session.attach(None).unwrap();
        db.execute("INSERT INTO foo (t) VALUES (?);", &["bar"])
            .unwrap();

        let mut output = Vec::new();
        session.changeset_strm(&mut output).unwrap();
        output
    }

    #[test]
    fn test_changeset() {
        let changeset = one_changeset();
        let mut iter = changeset.iter().unwrap();
        let item = iter.next().unwrap();
        assert!(item.is_some());

        let item = item.unwrap();
        let op = item.op().unwrap();
        assert_eq!("foo", op.table_name());
        assert_eq!(1, op.number_of_columns());
        assert_eq!(Action::SQLITE_INSERT, op.code());
        assert_eq!(false, op.indirect());

        let pk = item.pk().unwrap();
        assert_eq!(&[1], pk);

        let new_value = item.new_value(0).unwrap();
        assert_eq!(Ok("bar"), new_value.as_str());
    }

    #[test]
    fn test_changeset_strm() {
        let output = one_changeset_strm();
        assert!(!output.is_empty());
        assert_eq!(14, output.len());

        let input: &mut dyn Read = &mut output.as_slice();
        let mut iter = ChangesetIter::start_strm(&input).unwrap();
        let item = iter.next().unwrap();
        assert!(item.is_some());
    }

    #[test]
    fn test_changeset_apply() {
        let changeset = one_changeset();

        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(t TEXT PRIMARY KEY NOT NULL);")
            .unwrap();

        lazy_static::lazy_static! {
            static ref CALLED: AtomicBool = AtomicBool::new(false);
        }
        db.apply(
            &changeset,
            None::<fn(&str) -> bool>,
            |_conflict_type, _item| {
                CALLED.store(true, Ordering::Relaxed);
                ConflictAction::SQLITE_CHANGESET_OMIT
            },
        )
        .unwrap();

        assert!(!CALLED.load(Ordering::Relaxed));
        let check = db
            .query_row("SELECT 1 FROM foo WHERE t = ?", &["bar"], |row| {
                row.get::<_, i32>(0)
            })
            .unwrap();
        assert_eq!(1, check);

        // conflict expected when same changeset applied again on the same db
        db.apply(
            &changeset,
            None::<fn(&str) -> bool>,
            |conflict_type, item| {
                CALLED.store(true, Ordering::Relaxed);
                assert_eq!(ConflictType::SQLITE_CHANGESET_CONFLICT, conflict_type);
                let conflict = item.conflict(0).unwrap();
                assert_eq!(Ok("bar"), conflict.as_str());
                ConflictAction::SQLITE_CHANGESET_OMIT
            },
        )
        .unwrap();
        assert!(CALLED.load(Ordering::Relaxed));
    }

    #[test]
    fn test_changeset_apply_strm() {
        let output = one_changeset_strm();

        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(t TEXT PRIMARY KEY NOT NULL);")
            .unwrap();

        let mut input = output.as_slice();
        db.apply_strm(
            &mut input,
            None::<fn(&str) -> bool>,
            |_conflict_type, _item| ConflictAction::SQLITE_CHANGESET_OMIT,
        )
        .unwrap();

        let check = db
            .query_row("SELECT 1 FROM foo WHERE t = ?", &["bar"], |row| {
                row.get::<_, i32>(0)
            })
            .unwrap();
        assert_eq!(1, check);
    }

    #[test]
    fn test_session_empty() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo(t TEXT PRIMARY KEY NOT NULL);")
            .unwrap();

        let mut session = Session::new(&db).unwrap();
        assert!(session.is_empty());

        session.attach(None).unwrap();
        db.execute("INSERT INTO foo (t) VALUES (?);", &["bar"])
            .unwrap();

        assert!(!session.is_empty());
    }

    #[test]
    fn test_session_set_enabled() {
        let db = Connection::open_in_memory().unwrap();

        let mut session = Session::new(&db).unwrap();
        assert!(session.is_enabled());
        session.set_enabled(false);
        assert!(!session.is_enabled());
    }

    #[test]
    fn test_session_set_indirect() {
        let db = Connection::open_in_memory().unwrap();

        let mut session = Session::new(&db).unwrap();
        assert!(!session.is_indirect());
        session.set_indirect(true);
        assert!(session.is_indirect());
    }
}
