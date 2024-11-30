// Example of using a offline writes with libSQL.

use libsql::Builder;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // The local database path where the data will be stored.
    let db_path = std::env::var("LIBSQL_DB_PATH")
        .map_err(|_| {
            eprintln!(
                "Please set the LIBSQL_DB_PATH environment variable to set to local database path."
            )
        })
        .unwrap();

    // The remote sync URL to use.
    let sync_url = std::env::var("LIBSQL_SYNC_URL")
        .map_err(|_| {
            eprintln!(
                "Please set the LIBSQL_SYNC_URL environment variable to set to remote sync URL."
            )
        })
        .unwrap();

    // The authentication token to use.
    let auth_token = std::env::var("LIBSQL_AUTH_TOKEN").unwrap_or("".to_string());

    let db_builder = Builder::new_synced_database(db_path, sync_url, auth_token);

    let db = match db_builder.build().await {
        Ok(db) => db,
        Err(error) => {
            eprintln!("Error connecting to remote sync server: {}", error);
            return;
        }
    };

    println!("Syncing database from remote...");
    db.sync().await.unwrap();

    let conn = db.connect().unwrap();
    let mut results = conn
        .query("SELECT * FROM guest_book_entries", ())
        .await
        .unwrap();
    println!("Guest book entries:");
    while let Some(row) = results.next().await.unwrap() {
        let text: String = row.get(0).unwrap();
        println!("  {}", text);
    }

    println!("Done!");
}
