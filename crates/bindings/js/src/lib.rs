use std::any::Any;

use libsql;
use neon::prelude::*;

struct Database {
    db: libsql::Database,
    conn: libsql::Connection,
}

impl Finalize for Database {}

impl Database {
    fn new(db: libsql::Database, conn: libsql::Connection) -> Self {
        Database { db, conn }
    }

    fn js_new(mut cx: FunctionContext) -> JsResult<JsBox<Database>> {
        let url = cx.argument::<JsString>(0)?.value(&mut cx);
        let db = libsql::Database::open(url.clone()).unwrap();
        let conn = db.connect().unwrap();
        let db = Database::new(db, conn);
        Ok(cx.boxed(db))
    }

    fn js_exec(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let db = cx.this().downcast_or_throw::<JsBox<Database>, _>(&mut cx)?;
        let sql = cx.argument::<JsString>(0)?.value(&mut cx);
        db.conn.execute(sql, ()).unwrap();
        Ok(cx.undefined())
    }

    fn js_prepare(mut cx: FunctionContext) -> JsResult<JsBox<Statement>> {
        let db = cx.this().downcast_or_throw::<JsBox<Database>, _>(&mut cx)?;
        let sql = cx.argument::<JsString>(0)?.value(&mut cx);
        let stmt = db.conn.prepare(sql).unwrap();
        let stmt = Statement { stmt };
        Ok(cx.boxed(stmt))
    }
}

struct Statement {
    stmt: libsql::Statement,
}

impl Finalize for Statement {}

fn js_value_to_value(cx: &mut FunctionContext, v: Handle<'_, JsValue>) -> libsql::Value {
    if v.is_a::<JsNumber, _>(cx) {
        let v = v.downcast_or_throw::<JsNumber, _>(cx).unwrap();
        let v = v.value(cx);
        libsql::Value::Integer(v as i64)
    } else if v.is_a::<JsString, _>(cx) {
        let v = v.downcast_or_throw::<JsString, _>(cx).unwrap();
        let v = v.value(cx);
        libsql::Value::Text(v)
    } else {
        todo!("unsupported type");
    }
}

impl Statement {
    fn js_get(mut cx: FunctionContext) -> JsResult<JsObject> {
        let stmt = cx
            .this()
            .downcast_or_throw::<JsBox<Statement>, _>(&mut cx)?;
        let mut params = vec![];
        for i in 0..cx.len() {
            let v = cx.argument::<JsValue>(i)?;
            let v = js_value_to_value(&mut cx, v);
            params.push(v);
        }
        let params = libsql::Params::Positional(params);
        let rows = stmt.stmt.execute(&params).unwrap();
        let row = rows.next().unwrap().unwrap();
        let result = cx.empty_object();
        for idx in 0..rows.column_count() {
            let v = row.get_value(idx).unwrap();
            let column_name = rows.column_name(idx);
            let key = cx.string(column_name);
            let v: Handle<'_, JsValue> = match v {
                libsql::Value::Null => cx.null().upcast(),
                libsql::Value::Integer(v) => cx.number(v as f64).upcast(),
                libsql::Value::Float(v) => cx.number(v).upcast(),
                libsql::Value::Text(v) => cx.string(v).upcast(),
                libsql::Value::Blob(v) => todo!("unsupported type"),
            };
            result.set(&mut cx, key, v)?;
        }
        Ok(result)
    }
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("databaseNew", Database::js_new)?;
    cx.export_function("databaseExec", Database::js_exec)?;
    cx.export_function("databasePrepare", Database::js_prepare)?;
    cx.export_function("statementGet", Statement::js_get)?;
    Ok(())
}
