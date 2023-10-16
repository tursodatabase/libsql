/* file: keep-stack-sizes.x */
SECTIONS
{
  /* `INFO` makes the section not allocatable so it won't be loaded into memory */
  .stack_sizes (INFO) :
  {
    KEEP(*(.stack_sizes));
  }
}
/* RUSTFLAGS="-Z emit-stack-sizes" cargo rustc --release -- -C link-arg=-Wl,-Tkeep-stack-sizes.x -C link-arg=-N */
/* https://doc.rust-lang.org/nightly/unstable-book/compiler-flags/emit-stack-sizes.html */
