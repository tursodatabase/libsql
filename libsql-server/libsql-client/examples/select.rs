use libsql_client::{Connection, QueryResult, Statement, Value};
use rand::prelude::SliceRandom;

fn result_to_string(result: QueryResult) -> String {
    let mut ret = String::new();
    match result {
        QueryResult::Error((msg, _)) => return format!("Error: {msg}"),
        QueryResult::Success((result, _)) => {
            for column in &result.columns {
                ret += &format!("| {column:16} |");
            }
            ret += "\n| -------------------------------------------------------- |\n";
            for row in result.rows {
                for column in &result.columns {
                    ret += &format!("| {:16} |", row.cells[column].to_string());
                }
                ret += "\n";
            }
        }
    };
    ret
}

// Bumps a counter for one of the geographic locations picked at random.
async fn bump_counter(db: impl Connection) -> String {
    // Recreate the tables if they do not exist yet
    db.batch([
        "CREATE TABLE IF NOT EXISTS counter(country TEXT, city TEXT, value, PRIMARY KEY(country, city)) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS coordinates(lat INT, long INT, airport TEXT, PRIMARY KEY (lat, long))"
    ]).await.ok();

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
        Statement::with_params("INSERT INTO counter VALUES (?, ?, 0)", &[country, city]),
        Statement::with_params(
            "UPDATE counter SET value = value + 1 WHERE country = ? AND city = ?",
            &[country, city],
        ),
        Statement::with_params(
            "INSERT INTO coordinates VALUES (?, ?, ?)",
            &[
                Value::Float(latitude),
                Value::Float(longitude),
                airport.into(),
            ],
        ),
    ])
    .await
    .ok();

    let counter_response = match db.execute("SELECT * FROM counter").await {
        Ok(resp) => resp,
        Err(e) => return format!("Error: {e}"),
    };
    let scoreboard = result_to_string(counter_response);
    let html = format!("Scoreboard:\n{scoreboard}");
    html
}

#[tokio::main]
async fn main() {
    let db = libsql_client::reqwest::Connection::connect_from_env().unwrap();
    let response = bump_counter(db).await;
    println!("{response}")
}
