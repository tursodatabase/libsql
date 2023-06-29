use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Database {
}

#[wasm_bindgen]
impl Database {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Database {
        Database { }
    }

    pub fn all(&self, sql: String, f: &js_sys::Function) {
        let this = JsValue::null();
        let _ = f.call0(&this);
    }
}
