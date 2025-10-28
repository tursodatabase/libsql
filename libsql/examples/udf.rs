use std::sync::Arc;

use libsql::{Builder, ScalarFunctionDef};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Builder::new_local(":memory:").build().await?.connect()?;

    db.create_scalar_function(ScalarFunctionDef {
        name: "log".to_string(),
        num_args: 1,
        deterministic: false,
        innocuous: true,
        direct_only: false,
        callback: Arc::new(|args| {
            println!("Log from SQL: {:?}", args.first().unwrap());
            Ok(libsql::Value::Null)
        }),
    })?;

    db.query("select log('hello world')", ()).await?;

    Ok(())
}
