// from tokio uring

use std::mem::{size_of, MaybeUninit};

use zerocopy::AsBytes;

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

#[repr(transparent)]
pub struct ZeroCopyBoxIoBuf<T>(pub Box<T>);

unsafe impl<T: AsBytes + Unpin + 'static> IoBuf for ZeroCopyBoxIoBuf<T> {
    fn stable_ptr(&self) -> *const u8 {
        T::as_bytes(&self.0).as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.bytes_total()
    }

    fn bytes_total(&self) -> usize {
        size_of::<T>()
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

    pub fn map_slice<F>(self, f: F) -> MapSlice<T, F>
    where
        for<'a> F: Fn(&'a Self) -> &'a [u8] + Unpin + 'static,
    {
        MapSlice { inner: self, f }
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

    pub fn into_inner(self) -> T {
        assert!(self.is_init());
        unsafe { self.inner.assume_init() }
    }

    pub fn deinit(&mut self) {
        self.init = 0;
    }
}

pub struct MapSlice<T, F> {
    inner: ZeroCopyBuf<T>,
    f: F,
}

impl<T, F> MapSlice<T, F> {
    pub(crate) fn into_inner(self) -> ZeroCopyBuf<T> {
        self.inner
    }
}

unsafe impl<T, F> IoBuf for MapSlice<T, F>
where
    for<'a> F: Fn(&'a ZeroCopyBuf<T>) -> &'a [u8] + Unpin + 'static,
    T: Unpin + 'static + AsBytes,
{
    fn stable_ptr(&self) -> *const u8 {
        (self.f)(&self.inner).as_ptr()
    }

    fn bytes_init(&self) -> usize {
        (self.f)(&self.inner).len()
    }

    fn bytes_total(&self) -> usize {
        (self.f)(&self.inner).len()
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
