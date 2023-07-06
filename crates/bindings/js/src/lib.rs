use libsql_core;
use neon::prelude::*;

struct Database {
    _db: libsql_core::Database,
}

impl Finalize for Database {}

impl Database {
    fn new(db: libsql_core::Database) -> Self {
        Database { _db: db }
    }

    fn js_new(mut cx: FunctionContext) -> JsResult<JsBox<Database>> {
        let url = cx.argument::<JsString>(0)?.value(&mut cx);
        let db = libsql_core::Database::open(url);
        let db = Database::new(db);
        Ok(cx.boxed(db))
    }
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("databaseNew", Database::js_new)?;
    Ok(())
}
