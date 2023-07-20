static FIB_SRC: &str = r#"
(module 
    (type (;0;) (func (param i64) (result i64))) 
    (func $fib (type 0) (param i64) (result i64) 
    (local i64) 
    i64.const 0 
    local.set 1 
    block ;; label = @1 
    local.get 0 
    i64.const 2 
    i64.lt_u 
    br_if 0 (;@1;) 
    i64.const 0 
    local.set 1 
    loop ;; label = @2 
    local.get 0 
    i64.const -1 
    i64.add 
    call $fib 
    local.get 1 
    i64.add 
    local.set 1 
    local.get 0 
    i64.const -2 
    i64.add 
    local.tee 0 
    i64.const 1 
    i64.gt_u 
    br_if 0 (;@2;) 
    end 
    end 
    local.get 0 
    local.get 1 
    i64.add) 
    (memory (;0;) 16) 
    (global $__stack_pointer (mut i32) (i32.const 1048576)) 
    (global (;1;) i32 (i32.const 1048576)) 
    (global (;2;) i32 (i32.const 1048576)) 
    (export "memory" (memory 0)) 
    (export "fib" (func $fib)))
"#;

static GET_NULL_SRC: &str = r#"
(module
  (type (;0;) (func (result i32)))
  (func $get_null (type 0) (result i32)
    (local i32)
    i32.const 1
    memory.grow
    i32.const 16
    i32.shl
    local.tee 0
    i32.const 5
    i32.store8
    local.get 0)
  (memory (;0;) 16)
  (global $__stack_pointer (mut i32) (i32.const 1048576))
  (global (;1;) i32 (i32.const 1048576))
  (global (;2;) i32 (i32.const 1048576))
  (export "memory" (memory 0))
  (export "get_null" (func $get_null)))
"#;

pub fn fib_src() -> String {
    hex::encode(wabt::wat2wasm(FIB_SRC).unwrap())
}

pub fn contains_src() -> String {
    const CONTAINS_SRC: &[u8] = include_bytes!("./wasm/contains.wasm");
    hex::encode(CONTAINS_SRC)
}

pub fn concat3_src() -> String {
    const CONCAT3_SRC: &[u8] = include_bytes!("./wasm/concat3.wasm");
    hex::encode(CONCAT3_SRC)
}

pub fn reverse_blob_src() -> String {
    const REVERSE_BLOB_SRC: &[u8] = include_bytes!("./wasm/reverse_blob.wasm");
    hex::encode(REVERSE_BLOB_SRC)
}

pub fn get_null_src() -> String {
    hex::encode(wabt::wat2wasm(GET_NULL_SRC).unwrap())
}
