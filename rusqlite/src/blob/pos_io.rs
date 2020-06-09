use super::Blob;

use std::convert::TryFrom;
use std::mem::MaybeUninit;
use std::slice::from_raw_parts_mut;

use crate::ffi;
use crate::{Error, Result};

impl<'conn> Blob<'conn> {
    /// Write `buf` to `self` starting at `write_start`, returning an error if
    /// `write_start + buf.len()` is past the end of the blob.
    ///
    /// If an error is returned, no data is written.
    ///
    /// Note: the blob cannot be resized using this function -- that must be
    /// done using SQL (for example, an `UPDATE` statement).
    ///
    /// Note: This is part of the positional I/O API, and thus takes an absolute
    /// position write to, instead of using the internal position that can be
    /// manipulated by the `std::io` traits.
    pub fn write_at(&mut self, buf: &[u8], write_start: usize) -> Result<()> {
        let len = self.len();

        if buf.len().saturating_add(write_start) > len {
            return Err(Error::BlobSizeError);
        }
        // We know `len` fits in an `i32`, so either:
        //
        // 1. `buf.len() + write_start` overflows, in which case we'd hit the
        //    return above (courtesy of `saturating_add`).
        //
        // 2. `buf.len() + write_start` doesn't overflow but is larger than len,
        //    in which case ditto.
        //
        // 3. `buf.len() + write_start` doesn't overflow but is less than len.
        //    This means that both `buf.len()` and `write_start` can also be
        //    losslessly converted to i32, since `len` came from an i32.
        // Sanity check the above.
        debug_assert!(i32::try_from(write_start).is_ok() && i32::try_from(buf.len()).is_ok());
        unsafe {
            check!(ffi::sqlite3_blob_write(
                self.blob,
                buf.as_ptr() as *const _,
                buf.len() as i32,
                write_start as i32,
            ));
        }
        Ok(())
    }

    /// Read as much as possible from `offset` to `offset + buf.len()` out of
    /// `self`, writing into `buf`. On success, returns the number of bytes
    /// written.
    ///
    /// If there's insufficient data in `self`, then the returned value will be
    /// less than `buf.len()`.
    ///
    /// See also [`Blob::raw_read_at`], which can take an uninitialized buffer,
    /// or [`Blob::read_at_exact`] which returns an error if the entire `buf` is
    /// not read.
    ///
    /// Note: This is part of the positional I/O API, and thus takes an absolute
    /// position to read from, instead of using the internal position that can
    /// be manipulated by the `std::io` traits. Consequently, it does not change
    /// that value. either.
    pub fn read_at(&self, buf: &mut [u8], read_start: usize) -> Result<usize> {
        // Safety: this is safe because `raw_read_at` never stores uninitialized
        // data into `as_uninit`.
        let as_uninit: &mut [MaybeUninit<u8>] =
            unsafe { from_raw_parts_mut(buf.as_mut_ptr() as *mut _, buf.len()) };
        self.raw_read_at(as_uninit, read_start).map(|s| s.len())
    }

    /// Read as much as possible from `offset` to `offset + buf.len()` out of
    /// `self`, writing into `buf`. On success, returns the portion of `buf`
    /// which was initialized by this call.
    ///
    /// If there's insufficient data in `self`, then the returned value will be
    /// shorter than `buf`.
    ///
    /// See also [`Blob::read_at`], which takes a `&mut [u8]` buffer instead of
    /// a slice of `MaybeUninit<u8>`.
    ///
    /// Note: This is part of the positional I/O API, and thus takes an absolute
    /// position to read from, instead of using the internal position that can
    /// be manipulated by the `std::io` traits. Consequently, it does not change
    /// that value. either.
    pub fn raw_read_at<'a>(
        &self,
        buf: &'a mut [MaybeUninit<u8>],
        read_start: usize,
    ) -> Result<&'a mut [u8]> {
        let len = self.len();

        let read_len = match len.checked_sub(read_start) {
            None | Some(0) => 0,
            Some(v) => v.min(buf.len()),
        };

        if read_len == 0 {
            // We could return `Ok(&mut [])`, but it seems confusing that the
            // pointers don't match, so fabricate a empty slice of u8 with the
            // same base pointer as `buf`.
            let empty = unsafe { from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, 0) };
            return Ok(empty);
        }

        // At this point we believe `read_start as i32` is lossless because:
        //
        // 1. `len as i32` is known to be lossless, since it comes from a SQLite
        //    api returning an i32.
        //
        // 2. If we got here, `len.checked_sub(read_start)` was Some (or else
        //    we'd have hit the `if read_len == 0` early return), so `len` must
        //    be larger than `read_start`, and so it must fit in i32 as well.
        debug_assert!(i32::try_from(read_start).is_ok());

        // We also believe that `read_start + read_len <= len` because:
        //
        // 1. This is equivalent to `read_len <= len - read_start` via algebra.
        // 2. We know that `read_len` is `min(len - read_start, buf.len())`
        // 3. Expanding, this is `min(len - read_start, buf.len()) <= len - read_start`,
        //    or `min(A, B) <= A` which is clearly true.
        //
        // Note that this stuff is in debug_assert so no need to use checked_add
        // and such -- we'll always panic on overflow in debug builds.
        debug_assert!(read_start + read_len <= len);

        // These follow naturally.
        debug_assert!(
            buf.len() <= read_len
                && i32::try_from(buf.len()).is_ok()
                && i32::try_from(read_len).is_ok()
        );

        unsafe {
            check!(ffi::sqlite3_blob_read(
                self.blob,
                buf.as_mut_ptr() as *mut _,
                read_len as i32,
                read_start as i32,
            ));

            Ok(from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, read_len))
        }
    }

    /// Equivalent to [`Blob::read_at`], but returns a `BlobSizeError` if `buf`
    /// is not fully initialized.
    pub fn read_at_exact(&self, buf: &mut [u8], read_start: usize) -> Result<()> {
        let n = self.read_at(buf, read_start)?;
        if n != buf.len() {
            Err(Error::BlobSizeError)
        } else {
            Ok(())
        }
    }

    /// Equivalent to [`Blob::raw_read_at`], but returns a `BlobSizeError` if
    /// `buf` is not fully initialized.
    pub fn raw_read_at_exact<'a>(
        &self,
        buf: &'a mut [MaybeUninit<u8>],
        read_start: usize,
    ) -> Result<&'a mut [u8]> {
        let buflen = buf.len();
        let initted = self.raw_read_at(buf, read_start)?;
        if initted.len() != buflen {
            Err(Error::BlobSizeError)
        } else {
            Ok(initted)
        }
    }
}
