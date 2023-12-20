cargo test -- --nocapture
cargo watch "test -- --nocapture"
export RUST_BACKTRACE=0/1


cargo test --test tableinfo

// why is it `static` feature?