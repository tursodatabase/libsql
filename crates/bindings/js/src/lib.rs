use libsql;
use neon::prelude::*;

struct Database {
    _db: libsql::Database,
}

impl Finalize for Database {}

impl Database {
    fn new(db: libsql::Database) -> Self {
        Database { _db: db }
    }

    fn js_new(mut cx: FunctionContext) -> JsResult<JsBox<Database>> {
        let url = cx.argument::<JsString>(0)?.value(&mut cx);
        let db = libsql::Database::open(url);
        let db = Database::new(db);
        Ok(cx.boxed(db))
    }
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("databaseNew", Database::js_new)?;
    Ok(())
}
