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

#[no_mangle]
pub fn __rust_alloc_error_handler(_: Layout) -> ! {
    core::intrinsics::abort()
}
