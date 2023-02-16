use anyhow::Result;
use libsql_client::{params, Connection, QueryResult, ResultSet, Statement};
use rand::prelude::SliceRandom;

fn result_to_string(query_result: QueryResult) -> Result<String> {
    let mut ret = String::new();
    let ResultSet { columns, rows } = query_result.into_result_set()?;
    for column in &columns {
        ret += &format!("| {column:16} |");
    }
    ret += "\n| -------------------------------------------------------- |\n";
    for row in rows {
        for column in &columns {
            ret += &format!("| {:16} |", row.cells[column].to_string());
        }
        ret += "\n";
    }
    Ok(ret)
}

// Bumps a counter for one of the geographic locations picked at random.
async fn bump_counter(db: impl Connection) -> Result<String> {
    // Recreate the tables if they do not exist yet
    db.batch([
        "CREATE TABLE IF NOT EXISTS counter(country TEXT, city TEXT, value, PRIMARY KEY(country, city)) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS coordinates(lat INT, long INT, airport TEXT, PRIMARY KEY (lat, long))"
    ]).await?;

    // For demo purposes, let's pick a pseudorandom location
    const FAKE_LOCATIONS: &[(&str, &str, &str, f64, f64)] = &[
        ("WAW", "PL", "Warsaw", 52.22959, 21.0067),
        ("EWR", "US", "Newark", 42.99259, -81.3321),
        ("HAM", "DE", "Hamburg", 50.118801, 7.684300),
        ("HEL", "FI", "Helsinki", 60.3183, 24.9497),
        ("NSW", "AU", "Sydney", -33.9500, 151.1819),
    ];

    let (airport, country, city, latitude, longitude) =
        *FAKE_LOCATIONS.choose(&mut rand::thread_rng()).unwrap();

    db.transaction([
        Statement::with_params(
            "INSERT OR IGNORE INTO counter VALUES (?, ?, 0)",
            // Parameters that have a single type can be passed as a regular slice
            &[country, city],
        ),
        Statement::with_params(
            "UPDATE counter SET value = value + 1 WHERE country = ? AND city = ?",
            &[country, city],
        ),
        Statement::with_params(
            "INSERT OR IGNORE INTO coordinates VALUES (?, ?, ?)",
            // Parameters with different types can be passed to a convenience macro - params!()
            params!(latitude, longitude, airport),
        ),
    ])
    .await?;

    let counter_response = db.execute("SELECT * FROM counter").await?;
    let scoreboard = result_to_string(counter_response)?;
    let html = format!("Scoreboard:\n{scoreboard}");
    Ok(html)
}

#[tokio::main]
async fn main() {
    match libsql_client::reqwest::Connection::connect_from_env() {
        Ok(remote_db) => {
            match bump_counter(remote_db).await {
                Ok(response) => println!("Remote:\n{response}"),
                Err(e) => println!("Remote database query failed: {e}"),
            };
        }
        Err(e) => println!("Failed to fetch from a remote database: {e}"),
    }

    let mut path_buf = std::env::temp_dir();
    path_buf.push("libsql_client_test_db.db");
    let local_db = libsql_client::local::Connection::connect(path_buf.as_path()).unwrap();
    match bump_counter(local_db).await {
        Ok(response) => println!("Local:\n{response}"),
        Err(e) => println!("Local database query failed: {e}"),
    };
}
