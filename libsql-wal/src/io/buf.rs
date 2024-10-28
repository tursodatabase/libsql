// from tokio uring

use std::{
    borrow::Borrow,
    marker::PhantomData,
    mem::{size_of, MaybeUninit},
};

use bytes::{Bytes, BytesMut};
use zerocopy::{AsBytes, FromBytes};

pub unsafe trait IoBuf: Unpin + 'static {
    /// Returns a raw pointer to the vector’s buffer.
    ///
    /// This method is to be used by the `tokio-uring` runtime and it is not
    /// expected for users to call it directly.
    ///
    /// The implementation must ensure that, while the `tokio-uring` runtime
    /// owns the value, the pointer returned by `stable_ptr` **does not**
    /// change.
    fn stable_ptr(&self) -> *const u8;

    /// Number of initialized bytes.
    ///
    /// This method is to be used by the `tokio-uring` runtime and it is not
    /// expected for users to call it directly.
    ///
    /// For `Vec`, this is identical to `len()`.
    fn bytes_init(&self) -> usize;

    /// Total size of the buffer, including uninitialized memory, if any.
    ///
    /// This method is to be used by the `tokio-uring` runtime and it is not
    /// expected for users to call it directly.
    ///
    /// For `Vec`, this is identical to `capacity()`.
    fn bytes_total(&self) -> usize;
}

/// A mutable`io-uring` compatible buffer.
///
/// The `IoBufMut` trait is implemented by buffer types that can be passed to
/// io-uring operations. Users will not need to use this trait directly.
///
/// # Safety
///
/// Buffers passed to `io-uring` operations must reference a stable memory
/// region. While the runtime holds ownership to a buffer, the pointer returned
/// by `stable_mut_ptr` must remain valid even if the `IoBufMut` value is moved.
pub unsafe trait IoBufMut: IoBuf {
    /// Returns a raw mutable pointer to the vector’s buffer.
    ///
    /// This method is to be used by the `tokio-uring` runtime and it is not
    /// expected for users to call it directly.
    ///
    /// The implementation must ensure that, while the `tokio-uring` runtime
    /// owns the value, the pointer returned by `stable_mut_ptr` **does not**
    /// change.
    fn stable_mut_ptr(&mut self) -> *mut u8;

    /// Updates the number of initialized bytes.
    ///
    /// The specified `pos` becomes the new value returned by
    /// `IoBuf::bytes_init`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that all bytes starting at `stable_mut_ptr()` up
    /// to `pos` are initialized and owned by the buffer.
    unsafe fn set_init(&mut self, pos: usize);
}

unsafe impl<T: IoBufMut> IoBufMut for Box<T> {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut().stable_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.as_mut().set_init(pos)
    }
}

unsafe impl IoBuf for BytesMut {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

unsafe impl IoBuf for Bytes {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.len()
    }
}

unsafe impl IoBufMut for BytesMut {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        unsafe { self.set_len(pos) }
    }
}

unsafe impl<T: IoBuf> IoBuf for Box<T> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ref().stable_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.as_ref().bytes_init()
    }

    fn bytes_total(&self) -> usize {
        self.as_ref().bytes_total()
    }
}

pub struct ZeroCopyBuf<T> {
    init: usize,
    inner: MaybeUninit<T>,
}

pub struct ZeroCopyBoxIoBuf<T> {
    inner: Box<T>,
    init: usize,
}

impl<T> ZeroCopyBoxIoBuf<T> {
    pub fn new(inner: Box<T>) -> Self {
        Self {
            init: size_of::<T>(),
            inner,
        }
    }

    pub fn new_uninit(inner: Box<T>) -> Self {
        Self { init: 0, inner }
    }

    /// same as new_uninit, but partially fills the buffer starting at offset
    ///
    /// # Safety: The caller must ensure that the remaining bytes are initialized
    pub unsafe fn new_uninit_partial(inner: Box<T>, offset: usize) -> Self {
        assert!(offset < size_of::<T>());
        Self {
            inner,
            init: offset,
        }
    }

    fn is_init(&self) -> bool {
        self.init == size_of::<T>()
    }

    pub fn into_inner(self) -> Box<T> {
        assert!(self.is_init());
        self.inner
    }
}

unsafe impl<T: AsBytes + Unpin + 'static> IoBuf for ZeroCopyBoxIoBuf<T> {
    fn stable_ptr(&self) -> *const u8 {
        T::as_bytes(&self.inner).as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.init
    }

    fn bytes_total(&self) -> usize {
        size_of::<T>()
    }
}

unsafe impl<T: AsBytes + FromBytes + Unpin + 'static> IoBufMut for ZeroCopyBoxIoBuf<T> {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        T::as_bytes_mut(&mut self.inner).as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.init = pos;
    }
}

impl<T> ZeroCopyBuf<T> {
    pub fn new_init(inner: T) -> Self {
        Self {
            inner: MaybeUninit::new(inner),
            init: size_of::<T>(),
        }
    }

    pub fn new_uninit() -> Self {
        Self {
            init: 0,
            inner: MaybeUninit::uninit(),
        }
    }

    pub fn map_slice<F>(self, f: F) -> MapSlice<Self, F, T>
    where
        for<'a> F: Fn(&'a Self) -> &'a [u8] + Unpin + 'static,
    {
        MapSlice::new(self, f)
    }

    #[inline]
    pub fn is_init(&self) -> bool {
        self.init == size_of::<T>()
    }

    /// returns a ref to the inner type
    /// # Panic
    /// panics if the inner type is uninitialized
    pub fn get_ref(&self) -> &T {
        assert!(self.is_init());
        unsafe { self.inner.assume_init_ref() }
    }

    pub fn get_mut(&mut self) -> &mut T {
        assert!(self.is_init());
        unsafe { self.inner.assume_init_mut() }
    }

    pub fn into_inner(self) -> T {
        assert!(self.is_init());
        unsafe { self.inner.assume_init() }
    }

    pub fn deinit(&mut self) {
        self.init = 0;
    }
}

pub struct MapSlice<T, F, U> {
    inner: T,
    f: F,
    _p: PhantomData<U>,
}

impl<T, F, U> MapSlice<T, F, U> {
    pub(crate) fn into_inner(self) -> T {
        self.inner
    }

    pub(crate) fn new(inner: T, f: F) -> Self {
        Self {
            inner,
            f,
            _p: PhantomData,
        }
    }
}

unsafe impl<T, F, U> IoBuf for MapSlice<T, F, U>
where
    for<'a> F: Fn(&'a ZeroCopyBuf<U>) -> &'a [u8] + Unpin + 'static,
    T: Borrow<ZeroCopyBuf<U>> + Unpin + 'static,
    U: AsBytes + Unpin + 'static,
{
    fn stable_ptr(&self) -> *const u8 {
        (self.f)(&self.inner.borrow()).as_ptr()
    }

    fn bytes_init(&self) -> usize {
        (self.f)(&self.inner.borrow()).len()
    }

    fn bytes_total(&self) -> usize {
        (self.f)(&self.inner.borrow()).len()
    }
}

unsafe impl<T: AsBytes + Unpin + 'static> IoBuf for ZeroCopyBuf<T> {
    fn stable_ptr(&self) -> *const u8 {
        self.inner.as_ptr() as *const _
    }

    fn bytes_init(&self) -> usize {
        self.init
    }

    fn bytes_total(&self) -> usize {
        size_of::<T>()
    }
}

unsafe impl<T: AsBytes + Unpin + 'static> IoBufMut for ZeroCopyBuf<T> {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.inner.as_mut_ptr() as *mut _
    }

    unsafe fn set_init(&mut self, pos: usize) {
        assert!(pos <= size_of::<T>());
        self.init = pos
    }
}

unsafe impl IoBufMut for Vec<u8> {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        if self.len() < init_len {
            self.set_len(init_len);
        }
    }
}

unsafe impl IoBuf for Vec<u8> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}
