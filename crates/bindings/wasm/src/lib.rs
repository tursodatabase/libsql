use wasm_bindgen::prelude::*;

#[wasm_bindgen]
#[allow(unused)]
pub struct Database {
    inner: libsql::Database,
}

#[wasm_bindgen]
impl Database {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Database {
        Database {
            inner: libsql::Database::open(":memory:").unwrap(),
        }
    }

    pub fn all(&self, _sql: String, f: &js_sys::Function) {
        let this = JsValue::null();
        let _ = f.call0(&this);
    }
}
