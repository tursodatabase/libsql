use core::alloc::{GlobalAlloc, Layout};
pub struct SQLite3Allocator {}

unsafe impl GlobalAlloc for SQLite3Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        sqlite3_capi::malloc(layout.size())
    }

    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        sqlite3_capi::free(ptr);
    }
}
