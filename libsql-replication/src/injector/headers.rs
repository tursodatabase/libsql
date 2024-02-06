use std::marker::PhantomData;

use libsql_sys::ffi::PgHdr;

pub struct Headers<'a> {
    ptr: *mut PgHdr,
    _pth: PhantomData<&'a ()>,
}

impl<'a> Headers<'a> {
    // safety: ptr is guaranteed to be valid for 'a
    pub(crate) unsafe fn new(ptr: *mut PgHdr) -> Self {
        Self {
            ptr,
            _pth: PhantomData,
        }
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut PgHdr {
        self.ptr
    }
}

impl Drop for Headers<'_> {
    fn drop(&mut self) {
        let mut current = self.ptr;
        while !current.is_null() {
            let h: Box<PgHdr> = unsafe { Box::from_raw(current as _) };
            current = h.pDirty;
        }
    }
}
