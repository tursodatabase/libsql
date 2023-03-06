use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use clap::Parser;
use rusqlite::{types::ValueRef, Connection, Statement};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

#[derive(Debug, Parser)]
#[command(name = "libsql")]
#[command(about = "libSQL client", long_about = None)]
struct Args {
    #[clap()]
    db_path: Option<String>,
}

// Presents libSQL values in human-readable form
fn format_value(v: ValueRef) -> String {
    match v {
        ValueRef::Null => "null".to_owned(),
        ValueRef::Integer(i) => format!("{i}"),
        ValueRef::Real(r) => format!("{r}"),
        ValueRef::Text(s) => std::str::from_utf8(s).unwrap().to_owned(),
        ValueRef::Blob(b) => format!("0x{}", general_purpose::STANDARD_NO_PAD.encode(b)),
    }
}

// Executes a libSQL statement
// TODO: introduce paging for presenting large results, get rid of Vec
fn execute(stmt: &mut Statement) -> Result<Vec<Vec<String>>> {
    let column_count = stmt.column_count();

    let rows = stmt.query_map((), |row| {
        let row = (0..column_count)
            .map(|idx| format_value(row.get_ref(idx).unwrap()))
            .collect::<Vec<String>>();
        Ok(row)
    })?;
    Ok(rows.map(|r| r.unwrap()).collect())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let mut rl = DefaultEditor::new()?;

    let mut history = home::home_dir().unwrap_or_default();
    history.push(".libsql_history");
    rl.load_history(history.as_path()).ok();

    println!("libSQL version 0.2.0");
    let connection = match args.db_path.as_deref() {
        None | Some("") | Some(":memory:") => {
            println!("Connected to a transient in-memory database.");
            Connection::open_in_memory()?
        }
        Some(path) => Connection::open(path)?,
    };

    let mut leftovers = String::new();
    loop {
        let prompt = if leftovers.is_empty() {
            "libsql> "
        } else {
            "...   > "
        };
        let readline = rl.readline(prompt);
        match readline {
            Ok(line) => {
                let line = leftovers + line.trim_end();
                if line.ends_with(';') {
                    leftovers = String::new();
                } else {
                    leftovers = line + " ";
                    continue;
                };
                rl.add_history_entry(&line).ok();
                let mut stmt = match connection.prepare(&line) {
                    Ok(stmt) => stmt,
                    Err(e) => {
                        println!("Error: {e}");
                        continue;
                    }
                };
                let rows = match execute(&mut stmt) {
                    Ok(rows) => rows,
                    Err(e) => {
                        println!("Error: {e}");
                        continue;
                    }
                };
                if rows.is_empty() {
                    continue;
                }
                let mut builder = tabled::builder::Builder::new();
                builder.set_columns(stmt.column_names());
                for row in rows {
                    builder.add_record(row);
                }
                let mut table = builder.build();
                table.with(tabled::Style::psql());
                println!("{table}")
            }
            Err(ReadlineError::Interrupted) => {
                leftovers = String::new();
                continue;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history(history.as_path()).ok();
    Ok(())
}
