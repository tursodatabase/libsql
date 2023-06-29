use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use clap::Parser;
use rusqlite::{types::ValueRef, Connection, Statement};
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::{DefaultEditor, Editor};
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "libsql")]
#[command(about = "libSQL client", long_about = None)]
struct Cli {
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

struct StrStatements {
    value: String,
}

impl Iterator for StrStatements {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let mut embedded = false;
        let mut pos = 0;
        for (index, char) in self.value.chars().enumerate() {
            if char == '\'' {
                embedded = !embedded;
                continue;
            }
            if embedded || char != ';' {
                continue;
            }
            let str_statement = self.value[pos..index + 1].to_string();
            if str_statement.starts_with(';') || str_statement.is_empty() {
                pos = index + 1;
                continue;
            }
            self.value = self.value[index + 1..].to_string();
            return Some(str_statement.trim().to_string());
        }
        None
    }
}

fn get_str_statements(str: String) -> StrStatements {
    StrStatements { value: str }
}

/// State information about the database connection is contained in an
/// instance of the following structure.
struct Shell {
    /// The database
    db: Connection,
    /// Write results here
    out: Out,

    echo: bool,
    eqp: bool,
    explain: ExplainMode,
    headers: bool,
    mode: OutputMode,
    null_value: String,
    output: PathBuf,
    stats: StatsMode,
    width: [usize; 5],
    filename: PathBuf,

    colseparator: String,
    rowseparator: String,
}

enum Out {
    Stdout,
    File(std::fs::File),
}

impl Write for Out {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Out::Stdout => std::io::stdout().write(buf),
            Out::File(file) => file.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Out::Stdout => std::io::stdout().flush(),
            Out::File(file) => file.flush(),
        }
    }
}

#[derive(Debug)]
enum ExplainMode {
    Off,
    On,
    Auto,
}

#[derive(Debug)]
enum OutputMode {
    /// Columns/rows delimited by 0x1F and 0x1E
    Ascii,
    /// Tables using unicode box-drawing characters
    Box,
    /// Comma-separated values
    Csv,
    /// Output in columns. (see .width)
    Column,
    /// HTML <table> code
    Html,
    /// SQL insert statements for TABLE
    Insert,
    /// Results in a JSON array
    Json,
    /// One value per line
    Line,
    /// Values delimited by "|"
    List,
    /// Markdown table format
    Markdown,
    /// Escape answers as for SQL
    Quote,
    /// ASCII-art table
    Table,
    /// Tab-separated valeus
    Tabs,
    /// TCL list elements
    Tcl,
}

#[derive(Debug)]
enum StatsMode {
    /// Turn off automatic stat display
    Off,
    /// Turn on automatic stat display
    On,
    /// Show statement stats
    Stmt,
    /// Show the virtual machine step count only
    Vmstep,
}

impl Shell {
    fn new(db_path: Option<String>) -> Self {
        let connection = match db_path.as_deref() {
            None | Some("") | Some(":memory:") => {
                println!("Connected to a transient in-memory database.");
                Connection::open_in_memory().expect("Failed to open in-memory database")
            }
            Some(path) => Connection::open(path).expect("Failed to open database"),
        };

        Self {
            db: connection,
            out: Out::Stdout,
            echo: false,
            eqp: false,
            explain: ExplainMode::Auto,
            headers: false,
            mode: OutputMode::Column,
            stats: StatsMode::Off,
            width: [0; 5],
            null_value: String::new(),
            output: PathBuf::new(),
            filename: PathBuf::from(db_path.unwrap_or_else(|| ":memory:".to_string())),
            colseparator: String::new(),
            rowseparator: String::new(),
        }
    }

    fn run_command(&mut self, command: &str, args: &[&str]) {
        match command {
            ".help" => self.show_help(args),
            ".quit" => std::process::exit(0),
            ".show" => {
                _ = writeln!(
                    self.out,
                    "{:>12}: {}
{:>12}: {}
{:>12}: {:?}
{:>12}: {}
{:>12}: {:?}
{:>12}: {}
{:>12}: {:?}
{:>12}: {}
{:>12}: {}
{:>12}: {:?}
{:>12}: {:?}
{:>12}: {}",
                    "echo",
                    self.echo,
                    "eqp",
                    self.eqp,
                    "explain",
                    self.explain,
                    "headers",
                    self.headers,
                    "mode",
                    self.mode,
                    "nullvalue",
                    self.null_value,
                    "output",
                    self.output,
                    "colseparator",
                    self.colseparator,
                    "rowseparator",
                    self.rowseparator,
                    "stats",
                    self.stats,
                    "width",
                    self.width,
                    "filename",
                    self.filename.display()
                );
            }
            ".tables" => self.list_tables(args.get(0).copied()),
            _ => println!(
                "Error: unknown command or invalid arguments: \"{}\". Enter \".help\" for help",
                command
            ),
        }
    }

    fn run_statement(&self, statement: String) {
        for str_statement in get_str_statements(statement) {
            let mut stmt = match self.db.prepare(&str_statement) {
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
            if self.headers {
                builder.set_columns(stmt.column_names());
            }
            for row in rows {
                builder.add_record(row);
            }
            let mut table = builder.build();
            table.with(tabled::Style::psql());
            println!("{table}")
        }
    }

    fn run(mut self, rl: &mut Editor<(), FileHistory>) -> Result<()> {
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
                    if line.ends_with(';') || line.starts_with('.') {
                        leftovers = String::new();
                    } else {
                        leftovers = line + " ";
                        continue;
                    };
                    rl.add_history_entry(&line).ok();
                    if line.starts_with('.') {
                        let split = line.split_whitespace().collect::<Vec<&str>>();
                        let prev_header_settings = self.headers;
                        self.headers = false;
                        self.run_command(split[0], &split[1..]);
                        self.headers = prev_header_settings;
                    } else {
                        self.run_statement(line)
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    leftovers.clear();
                }
                Err(ReadlineError::Eof) => {
                    println!("^D");
                    break;
                }
                Err(err) => {
                    println!("Error: {:?}", err);
                    break;
                }
            }
        }
        Ok(())
    }

    fn list_tables(&self, pattern: Option<&str>) {
        let mut statement = String::from(
            "SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%'",
        );
        match pattern {
            Some(p) => statement.push_str(format!("AND name LIKE {p};").as_str()),
            None => statement.push(';'),
        }
        self.run_statement(statement)
    }

    // TODO: implement `-all` option: print detailed flags for each command
    // TODO: implement `?PATTERN?` : allow narrowing using prefix search.
    fn show_help(&self, _args: &[&str]) {
        println!(
            r#"
.auth ON|OFF             Show authorizer callbacks
.backup ?DB? FILE        Backup DB (default "main") to FILE
.bail on|off             Stop after hitting an error.  Default OFF
.binary on|off           Turn binary output on or off.  Default OFF
.cd DIRECTORY            Change the working directory to DIRECTORY
.changes on|off          Show number of rows changed by SQL
.check GLOB              Fail if output since .testcase does not match
.clone NEWDB             Clone data into NEWDB from the existing database
.connection [close] [#]  Open or close an auxiliary database connection
.databases               List names and files of attached databases
.dbconfig ?op? ?val?     List or change sqlite3_db_config() options
.dbinfo ?DB?             Show status information about the database
.dump ?OBJECTS?          Render database content as SQL
.echo on|off             Turn command echo on or off
.eqp on|off|full|...     Enable or disable automatic EXPLAIN QUERY PLAN
.excel                   Display the output of next command in spreadsheet
.exit ?CODE?             Exit this program with return-code CODE
.expert                  EXPERIMENTAL. Suggest indexes for queries
.explain ?on|off|auto?   Change the EXPLAIN formatting mode.  Default: auto
.filectrl CMD ...        Run various sqlite3_file_control() operations
.fullschema ?--indent?   Show schema and the content of sqlite_stat tables
.headers on|off          Turn display of headers on or off
.help ?-all? ?PATTERN?   Show help text for PATTERN
.import FILE TABLE       Import data from FILE into TABLE
.imposter INDEX TABLE    Create imposter table TABLE on index INDEX
.indexes ?TABLE?         Show names of indexes
.limit ?LIMIT? ?VAL?     Display or change the value of an SQLITE_LIMIT
.lint OPTIONS            Report potential schema issues.
.log FILE|off            Turn logging on or off.  FILE can be stderr/stdout
.mode MODE ?TABLE?       Set output mode
.nonce STRING            Disable safe mode for one command if the nonce matches
.nullvalue STRING        Use STRING in place of NULL values
.once ?OPTIONS? ?FILE?   Output for the next SQL command only to FILE
.open ?OPTIONS? ?FILE?   Close existing database and reopen FILE
.output ?FILE?           Send output to FILE or stdout if FILE is omitted
.parameter CMD ...       Manage SQL parameter bindings
.print STRING...         Print literal STRING
.progress N              Invoke progress handler after every N opcodes
.prompt MAIN CONTINUE    Replace the standard prompts
.quit                    Exit this program
.read FILE               Read input from FILE
.recover                 Recover as much data as possible from corrupt db.
.restore ?DB? FILE       Restore content of DB (default "main") from FILE
.save FILE               Write in-memory database into FILE
.scanstats on|off        Turn sqlite3_stmt_scanstatus() metrics on or off
.schema ?PATTERN?        Show the CREATE statements matching PATTERN
.selftest ?OPTIONS?      Run tests defined in the SELFTEST table
.separator COL ?ROW?     Change the column and row separators
.session ?NAME? CMD ...  Create or control sessions
.sha3sum ...             Compute a SHA3 hash of database content
.shell CMD ARGS...       Run CMD ARGS... in a system shell
.show                    Show the current values for various settings
.stats ?ARG?             Show stats or turn stats on or off
.system CMD ARGS...      Run CMD ARGS... in a system shell
.tables ?TABLE?          List names of tables matching LIKE pattern TABLE
.testcase NAME           Begin redirecting output to 'testcase-out.txt'
.testctrl CMD ...        Run various sqlite3_test_control() operations
.timeout MS              Try opening locked tables for MS milliseconds
.timer on|off            Turn SQL timer on or off
.trace ?OPTIONS?         Output each SQL statement as it is run
.vfsinfo ?AUX?           Information about the top-level VFS
.vfslist                 List all available VFSes
.vfsname ?AUX?           Print the name of the VFS stack
.width NUM1 NUM2 ...     Set minimum column widths for columnar output
"#
        )
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();

    let mut rl = DefaultEditor::new()?;

    let mut history = home::home_dir().unwrap_or_default();
    history.push(".libsql_history");
    rl.load_history(history.as_path()).ok();

    // TODO: load settings

    println!("libSQL version 0.2.0");
    let shell = Shell::new(args.db_path);
    let result = shell.run(&mut rl);
    rl.save_history(history.as_path()).ok();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_statements_itterator() {
        let mut str_statements_iterator =
            get_str_statements(String::from("SELECT ';' FROM test; SELECT * FROM test;;"));
        assert_eq!(
            str_statements_iterator.next(),
            Some("SELECT ';' FROM test;".to_owned())
        );
        assert_eq!(
            str_statements_iterator.next(),
            Some("SELECT * FROM test;".to_owned())
        );
        assert_eq!(str_statements_iterator.next(), None);
    }
}
