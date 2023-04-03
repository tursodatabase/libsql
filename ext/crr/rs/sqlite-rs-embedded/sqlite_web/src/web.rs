extern crate alloc;

use core::alloc::GlobalAlloc;
use sqlite_nostd::SQLite3Allocator;
#[global_allocator]
static ALLOCATOR: SQLite3Allocator = SQLite3Allocator {};

use core::panic::PanicInfo;

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    core::intrinsics::abort()
}

use core::alloc::Layout;
#[alloc_error_handler]
fn oom(_: Layout) -> ! {
    core::intrinsics::abort()
}

#[no_mangle]
pub extern "C" fn __rust_alloc(size: usize, align: usize) -> *mut u8 {
    unsafe { ALLOCATOR.alloc(Layout::from_size_align_unchecked(size, align)) }
}

#[no_mangle]
pub extern "C" fn __rust_dealloc(ptr: *mut u8, size: usize, align: usize) {
    unsafe { ALLOCATOR.dealloc(ptr, Layout::from_size_align_unchecked(size, align)) }
}

#[no_mangle]
pub extern "C" fn __rust_realloc(
    ptr: *mut u8,
    old_size: usize,
    align: usize,
    size: usize,
) -> *mut u8 {
    unsafe {
        ALLOCATOR.realloc(
            ptr,
            Layout::from_size_align_unchecked(old_size, align),
            size,
        )
    }
}

#[no_mangle]
pub extern "C" fn __rust_alloc_zeroed(size: usize, align: usize) -> *mut u8 {
    unsafe { ALLOCATOR.alloc_zeroed(Layout::from_size_align_unchecked(size, align)) }
}

#[no_mangle]
pub fn __rust_alloc_error_handler(_: Layout) -> ! {
    core::intrinsics::abort()
}
