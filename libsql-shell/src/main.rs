#![allow(dead_code)]

use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use clap::Parser;
use rusqlite::Params;
use rusqlite::{types::ValueRef, Connection, Statement};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::{CompletionType, Config, Context, Editor};
use rustyline_derive::{Helper, Highlighter, Hinter, Validator};
use std::fmt::Display;
use std::io::Write;
use std::path::PathBuf;
use tabled::settings::Style;
use tabled::Table;

#[derive(Debug, Parser)]
#[command(name = "libsql")]
#[command(about = "libSQL client", long_about = None)]
struct Cli {
    #[clap()]
    db_path: Option<String>,

    /// Print inputs before execution
    #[arg(long, default_value = "false")]
    echo: bool,
    /// Run "COMMAND" before reading stdin
    #[arg(long = "cmd", action = clap::ArgAction::Append)]
    command: Option<Vec<String>>,
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
            if str_statement.trim().starts_with(';') {
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
    stats: StatsMode,
    width: [usize; 5],
    filename: PathBuf,

    commands_before_repl: Option<Vec<String>>,
    colseparator: String,
    rowseparator: String,
    main_prompt: String,
    continuation_prompt: String,
}

enum Out {
    Stdout,
    File(std::fs::File, PathBuf),
}

impl Write for Out {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Out::Stdout => std::io::stdout().write(buf),
            Out::File(file, _) => file.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Out::Stdout => std::io::stdout().flush(),
            Out::File(file, _) => file.flush(),
        }
    }
}

impl Display for Out {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Out::Stdout => write!(f, "stdout"),
            Out::File(_, path) => write!(f, "{}", path.display()),
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
    fn new(args: Cli) -> Self {
        let connection = match args.db_path.as_deref() {
            None | Some("") | Some(":memory:") => {
                println!("Connected to a transient in-memory database.");
                Connection::open_in_memory().expect("Failed to open in-memory database")
            }
            Some(path) => Connection::open(path).expect("Failed to open database"),
        };

        Self {
            db: connection,
            out: Out::Stdout,
            echo: args.echo,
            eqp: false,
            explain: ExplainMode::Auto,
            headers: true,
            mode: OutputMode::Column,
            stats: StatsMode::Off,
            width: [0; 5],
            null_value: String::new(),
            filename: PathBuf::from(args.db_path.unwrap_or_else(|| ":memory:".to_string())),
            commands_before_repl: args.command,
            colseparator: String::new(),
            rowseparator: String::new(),
            main_prompt: "libsql> ".to_string(),
            continuation_prompt: "   ...> ".to_string(),
        }
    }

    fn parse_and_run_command(&mut self, line: &str) {
        // split line on whitespace, but not inside quotes.
        let mut split = vec![];
        for (i, chunk) in line.split_terminator(&['\'', '"']).enumerate() {
            if i % 2 != 0 {
                split.push(chunk);
            } else {
                split.extend(chunk.split_whitespace())
            }
        }
        self.run_command(split[0], &split[1..]);
    }

    fn run(mut self, rl: &mut Editor<ShellHelper, FileHistory>) -> Result<()> {
        if let Some(commands) = self.commands_before_repl.take() {
            for command in commands {
                self.parse_and_run_command(&command);
            }
        }

        let mut leftovers = String::new();
        loop {
            let prompt = if leftovers.is_empty() {
                self.main_prompt.as_str()
            } else {
                self.continuation_prompt.as_str()
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
                    if self.echo {
                        writeln!(self.out, "{}", line).unwrap();
                    }
                    if line.starts_with('.') {
                        self.parse_and_run_command(&line);
                    } else {
                        for str_statement in get_str_statements(line) {
                            let table = self.run_statement(str_statement, (), false);
                            match table {
                                Ok(table) => {
                                    if self.headers && table.count_rows() == 1
                                        || !self.headers && table.count_rows() == 0
                                    {
                                        continue;
                                    }
                                    writeln!(self.out, "{}", table)?;
                                }
                                Err(e) => {
                                    println!("Error: {}", e);
                                }
                            }
                        }
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

    fn run_command(&mut self, command: &str, args: &[&str]) {
        let mut result = None;
        match command {
            ".echo" => {
                if args.len() != 1 {
                    writeln!(self.out, "Usage: .echo on|off").unwrap();
                    return;
                }
                match args[0].to_lowercase().as_str() {
                    "on" | "true" => self.echo = true,
                    "off" | "false" => self.echo = false,
                    txt => {
                        self.echo = false;
                        writeln!(
                            self.out,
                            "ERROR: Not a boolean value: \"{}\". Assuming \"no\"",
                            txt
                        )
                        .unwrap()
                    }
                }
            }
            ".headers" => {
                if args.len() != 1 {
                    writeln!(self.out, "Usage: .headers on|off").unwrap();
                    return;
                }
                match args[0].to_lowercase().as_str() {
                    "on" | "true" => self.headers = true,
                    "off" | "false" => self.headers = false,
                    txt => {
                        self.headers = false;
                        writeln!(
                            self.out,
                            "ERROR: Not a boolean value: \"{}\". Assuming \"no\"",
                            txt
                        )
                        .unwrap()
                    }
                }
            }
            ".help" => self.show_help(args),
            ".indexes" => result = Some(self.list_tables(args.first().copied(), true)),
            ".nullvalue" => {
                if args.len() != 1 {
                    writeln!(self.out, "Usage: .nullvalue STRING").unwrap();
                    return;
                }
                self.null_value = args[0].to_string();
            }
            ".print" => {
                writeln!(self.out, "{}", args.join(" ")).unwrap();
            }
            ".prompt" => {
                if !args.is_empty() {
                    self.main_prompt = args[0].to_string();
                }
                if args.len() > 1 {
                    self.continuation_prompt = args[1].to_string();
                }
            }
            ".quit" => std::process::exit(0),
            ".show" => {
                if !args.is_empty() {
                    writeln!(self.out, "Usage: .show").unwrap();
                    return;
                }
                let out_name = format!("{}", self.out);
                _ = writeln!(
                    self.out,
                    "{:>12}: {}
{:>12}: {}
{:>12}: {:?}
{:>12}: {}
{:>12}: {:?}
{:>12}: \"{}\"
{:>12}: {}
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
                    out_name,
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
            ".tables" => result = Some(self.list_tables(args.first().copied(), false)),
            _ => println!(
                "Error: unknown command or invalid arguments: \"{}\". Enter \".help\" for help",
                command
            ),
        }
        match result {
            Some(Ok(mut table)) => {
                if table.count_rows() == 0 {
                    return;
                }
                table.with(Style::blank());
                _ = writeln!(self.out, "{}", table);
            }
            Some(Err(e)) => {
                println!("Error: {e}");
            }
            None => {}
        }
    }

    fn run_statement<P>(&self, statement: String, params: P, is_command: bool) -> Result<Table>
    where
        P: Params,
    {
        let mut stmt: Statement<'_> = self.db.prepare(&statement)?;
        // TODO: introduce paging for presenting large results, get rid of Vec
        let rows: Vec<Vec<String>> = {
            let column_count = stmt.column_count();

            let rows = stmt.query_map(params, |row| {
                let row = (0..column_count)
                    .map(|idx| self.format_value(row.get_ref(idx).unwrap()))
                    .collect::<Vec<String>>();
                Ok(row)
            })?;

            let mut mapped_rows = vec![];
            for row in rows {
                if let Ok(r) = row {
                    mapped_rows.push(r);
                }
            }
            mapped_rows
        };

        let mut builder = tabled::builder::Builder::new();
        // TODO: switch style based on mode.
        let style = Style::ascii();
        if self.headers && !is_command {
            // if we use a SQL statement to execute a command, don't include the headers.
            // affected commands: .tables
            builder.set_header(stmt.column_names());
        }
        for row in rows {
            builder.push_record(row);
        }
        let mut table = builder.build();
        table.with(style);
        Ok(table)
    }

    // helper functions

    // Presents libSQL values in human-readable form
    fn format_value(&self, v: ValueRef) -> String {
        match v {
            ValueRef::Null => self.null_value.clone(),
            ValueRef::Integer(i) => format!("{i}"),
            ValueRef::Real(r) => format!("{r}"),
            ValueRef::Text(s) => std::str::from_utf8(s).unwrap().to_owned(),
            ValueRef::Blob(b) => format!("0x{}", general_purpose::STANDARD_NO_PAD.encode(b)),
        }
    }
    // COMMANDS

    fn list_tables(&self, pattern: Option<&str>, is_index: bool) -> Result<Table> {
        let mut statement =
            String::from("SELECT name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'");
        if is_index {
            statement.push_str("AND type='index' ")
        } else {
            statement.push_str("AND type IN ('table','view') ")
        }
        match pattern {
            Some(p) => {
                if is_index {
                    statement.push_str("AND tbl_name LIKE :name;");
                } else {
                    statement.push_str("AND name NOT LIKE 'sqlite_%' AND name LIKE :name;");
                }
                self.run_statement(statement, &[(":name", p)], true)
            }
            None => {
                statement.push(';');
                self.run_statement(statement, (), true)
            }
        }
    }

    // TODO: implement `-all` option: print detailed flags for each command
    // TODO: implement `?PATTERN?` : allow narrowing using prefix search.
    fn show_help(&mut self, _args: &[&str]) {
        _ = writeln!(
            self.out,
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
        );
    }
}

#[derive(Default)]
struct ShellCompleter {}

impl ShellCompleter {
    fn new() -> Self {
        Self::default()
    }

    fn complete(
        &self,
        line: &str,
        _pos: usize,
        _: &Context,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let mut pairs: Vec<Pair> = vec![];
        let commands = vec![
            ".echo",
            ".headers",
            ".help",
            ".indexes",
            ".nullvalue",
            ".print",
            ".prompt",
            ".quit",
            ".show",
            ".tables",
        ];
        for command in commands {
            if command.starts_with(line) {
                pairs.push(Pair {
                    display: command.to_string(),
                    replacement: command.to_string(),
                })
            }
        }
        Ok((0, pairs))
    }
}

#[derive(Helper, Hinter, Validator, Highlighter)]
struct ShellHelper {
    #[rustyline(Completer)]
    completer: ShellCompleter,
}

impl Completer for ShellHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        self.completer.complete(line, pos, ctx)
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();

    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::Circular)
        .build();
    let mut rl = Editor::with_config(config)?;

    let helper = ShellHelper {
        completer: ShellCompleter::new(),
    };
    rl.set_helper(Some(helper));

    let mut history = home::home_dir().unwrap_or_default();
    history.push(".libsql_history");
    rl.load_history(history.as_path()).ok();

    println!("libSQL version 0.2.0");
    let shell = Shell::new(args);
    let result = shell.run(&mut rl);
    rl.save_history(history.as_path()).ok();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_statements_iterator() {
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

        let mut str_statements_iterator = get_str_statements(String::from(";;;"));
        assert_eq!(str_statements_iterator.next(), None);

        let mut str_statements_iterator = get_str_statements(String::from("        "));
        assert_eq!(str_statements_iterator.next(), None);

        let mut str_statements_iterator = get_str_statements(String::from("   ;    ;    ;  "));
        assert_eq!(str_statements_iterator.next(), None);
    }

    #[test]
    fn test_empty_statement() {
        let cli = Cli {
            db_path: Some(":memory:".to_string()),
            echo: false,
            command: None,
        };
        let shell = Shell::new(cli);
        assert!(shell.headers);
        let result = shell.run_statement(" ; ; ;".to_string(), [], false);
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table.count_rows(), 1);
    }
}
