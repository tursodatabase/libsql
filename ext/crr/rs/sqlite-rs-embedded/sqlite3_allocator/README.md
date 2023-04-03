# sqlite3_allocator

Installs the sqlite3 memory system as the Rust default global allocator. Useful in no_std environments where there is no allocator and, if you want to use collections, you must bring your own.

https://docs.rust-embedded.org/book/intro/no-std.html

Reference:
https://github.com/rust-embedded/embedded-alloc
