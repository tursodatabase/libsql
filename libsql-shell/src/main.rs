use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use clap::Parser;
use once_cell::sync::Lazy;
use rusqlite::ffi::{
    sqlite3_changes64, sqlite3_db_config, sqlite3_total_changes64, SQLITE_DBCONFIG_DEFENSIVE,
    SQLITE_DBCONFIG_DQS_DDL, SQLITE_DBCONFIG_DQS_DML, SQLITE_DBCONFIG_ENABLE_FKEY,
    SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
    SQLITE_DBCONFIG_ENABLE_QPSG, SQLITE_DBCONFIG_ENABLE_TRIGGER, SQLITE_DBCONFIG_ENABLE_VIEW,
    SQLITE_DBCONFIG_LEGACY_ALTER_TABLE, SQLITE_DBCONFIG_LEGACY_FILE_FORMAT,
    SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, SQLITE_DBCONFIG_RESET_DATABASE,
    SQLITE_DBCONFIG_REVERSE_SCANORDER, SQLITE_DBCONFIG_STMT_SCANSTATUS,
    SQLITE_DBCONFIG_TRIGGER_EQP, SQLITE_DBCONFIG_TRUSTED_SCHEMA, SQLITE_DBCONFIG_WRITABLE_SCHEMA,
};
use rusqlite::{types::ValueRef, Connection, LoadExtensionGuard, OpenFlags, Statement};
use rusqlite::{DatabaseName, Params};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::{CompletionType, Config, Context, Editor};
use rustyline_derive::{Helper, Highlighter, Hinter, Validator};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
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
    /// Refuse to open symbolic links to database files
    #[arg(long = "nofollow", default_value = "false")]
    no_follow: bool,
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

    bail: bool,
    changes: bool,
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

#[allow(dead_code)]
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

impl fmt::Display for Out {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Out::Stdout => write!(f, "stdout"),
            Out::File(_, path) => write!(f, "{}", path.display()),
        }
    }
}

#[allow(dead_code)]
enum ExplainMode {
    Off,
    On,
    Auto,
}

impl fmt::Display for ExplainMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                ExplainMode::Off => "off",
                ExplainMode::On => "on",
                ExplainMode::Auto => "auto",
            }
        )
    }
}

#[allow(dead_code)]
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

impl fmt::Display for OutputMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

#[allow(dead_code)]
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

impl fmt::Display for StatsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                StatsMode::Off => "off",
                StatsMode::On => "on",
                StatsMode::Stmt => "stmt",
                StatsMode::Vmstep => "vmstep",
            }
        )
    }
}
impl Shell {
    fn new(args: Cli) -> Result<Self> {
        let connection = match args.db_path.as_deref() {
            None | Some("") | Some(":memory:") => {
                println!("Connected to a transient in-memory database.");
                Connection::open_in_memory()?
            }
            Some(path) => {
                let mut flags = OpenFlags::default();
                if args.no_follow {
                    flags.insert(OpenFlags::SQLITE_OPEN_NOFOLLOW);
                }
                Connection::open_with_flags(path, flags)?
            }
        };

        Ok(Self {
            bail: false,
            changes: false,
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
            colseparator: String::from("|"),
            rowseparator: String::from("\n"),
            main_prompt: "libsql> ".to_string(),
            continuation_prompt: "   ...> ".to_string(),
        })
    }

    fn parse_and_run_command(&mut self, line: &str) -> Result<()> {
        // split line on whitespace, but not inside quotes.
        let mut split = vec![];
        for (i, chunk) in line.split_terminator(&['\'', '"']).enumerate() {
            if i % 2 != 0 {
                split.push(chunk);
            } else {
                split.extend(chunk.split_whitespace())
            }
        }
        self.run_command(split[0], &split[1..])
    }

    fn run(mut self, rl: &mut Editor<ShellHelper, FileHistory>) -> Result<()> {
        if let Some(commands) = self.commands_before_repl.take() {
            for command in commands {
                self.parse_and_run_command(&command)?;
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
                        writeln!(self.out, "{}", line)?;
                    }
                    if line.starts_with('.') {
                        self.parse_and_run_command(&line)?;
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
                                    if self.changes {
                                        unsafe {
                                            let db = self.db.handle();
                                            let changes = sqlite3_changes64(db);
                                            let total_changes = sqlite3_total_changes64(db);
                                            writeln!(
                                                self.out,
                                                "changes: {}   total_changes: {}",
                                                changes, total_changes
                                            )?;
                                        }
                                    }
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

    fn run_command(&mut self, command: &str, args: &[&str]) -> Result<()> {
        let mut result = None;
        match command {
            ".bail" => toggle_option(command, &mut self.bail, args),
            ".changes" => toggle_option(command, &mut self.changes, args),
            ".databases" => {
                let statement = "pragma database_list;";
                let mut stmt = self.db.prepare(statement)?;
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    let name = row.get::<_, String>(1)?;
                    let file = row.get::<_, String>(2)?;
                    let db_name = match name.as_str() {
                        "main" => DatabaseName::Main,
                        "temp" => DatabaseName::Temp,
                        s => DatabaseName::Attached(s),
                    };
                    let readonly = if self.db.is_readonly(db_name)? {
                        "r/o"
                    } else {
                        "r/w"
                    };
                    writeln!(self.out, "{}: {:?} {}", name, file, readonly)?;
                }
            }
            ".dbconfig" => {
                static DBCONFIG: Lazy<BTreeMap<&str, i32>> = Lazy::new(|| {
                    [
                        ("defensive", SQLITE_DBCONFIG_DEFENSIVE),
                        ("dqs_ddl", SQLITE_DBCONFIG_DQS_DDL),
                        ("dqs_dml", SQLITE_DBCONFIG_DQS_DML),
                        ("enable_fkey", SQLITE_DBCONFIG_ENABLE_FKEY),
                        ("enable_qpsg", SQLITE_DBCONFIG_ENABLE_QPSG),
                        ("enable_trigger", SQLITE_DBCONFIG_ENABLE_TRIGGER),
                        ("enable_view", SQLITE_DBCONFIG_ENABLE_VIEW),
                        ("fts3_tokenizer", SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER),
                        ("legacy_alter_table", SQLITE_DBCONFIG_LEGACY_ALTER_TABLE),
                        ("legacy_file_format", SQLITE_DBCONFIG_LEGACY_FILE_FORMAT),
                        ("load_extension", SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION),
                        ("no_ckpt_on_close", SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE),
                        ("reset_database", SQLITE_DBCONFIG_RESET_DATABASE),
                        ("reverse_scanorder", SQLITE_DBCONFIG_REVERSE_SCANORDER),
                        ("stmt_scanstatus", SQLITE_DBCONFIG_STMT_SCANSTATUS),
                        ("trigger_eqp", SQLITE_DBCONFIG_TRIGGER_EQP),
                        ("trusted_schema", SQLITE_DBCONFIG_TRUSTED_SCHEMA),
                        ("writable_schema", SQLITE_DBCONFIG_WRITABLE_SCHEMA),
                    ]
                    .into_iter()
                    .collect()
                });
                let db = unsafe { self.db.handle() };
                if args.is_empty() {
                    for (name, opt) in DBCONFIG.iter() {
                        let enabled = 0;
                        unsafe {
                            sqlite3_db_config(db, *opt, -1, &enabled);
                        }
                        writeln!(
                            self.out,
                            "{:>19} {}",
                            name,
                            if enabled == 0 { "off" } else { "on" }
                        )?;
                    }
                } else {
                    match DBCONFIG.get(args[0]) {
                        Some(opt) => match args.len() {
                            1 => {
                                let enabled = 0;
                                unsafe {
                                    sqlite3_db_config(db, *opt, -1, &enabled);
                                }
                                writeln!(
                                    self.out,
                                    "{:>19} {}",
                                    args[0],
                                    if enabled == 0 { "off" } else { "on" }
                                )?;
                            }
                            2 => {
                                let enabled = match args[1].to_lowercase().as_str() {
                                    "on" | "true" | "yes" => true,
                                    "off" | "false" | "no" => false,
                                    arg => {
                                        if arg.chars().all(|a| a.is_ascii_digit()) {
                                            arg != "0"
                                        } else {
                                            println!("ERROR: Not a boolean value: \"{}\". Assuming \"no\"", arg);
                                            false
                                        }
                                    }
                                };
                                unsafe {
                                    sqlite3_db_config(db, *opt, enabled as i32, 0);
                                }
                                writeln!(
                                    self.out,
                                    "{:>19} {}",
                                    args[0],
                                    if enabled { "on" } else { "off" }
                                )?;
                            }
                            _ => println!("Usage: .dbconfig ?op? ?val?     List or change sqlite3_db_config() options"),
                        },
                        None => {
                            println!("Error: unknown dbconfig {:?}\nEnter \".dbconfig\" with no arguments for a list", args[0]);
                        }
                    }
                }
            }
            ".echo" => toggle_option(command, &mut self.echo, args),
            ".exit" => {
                if args.len() != 1 {
                    exit(0);
                }

                let mut chars = args[0].bytes();
                let mut code = 0;
                let neg = match chars.next() {
                    Some(b'-') => true,
                    Some(c) if c.is_ascii_digit() => {
                        code = code * 10 + (c - b'0') as i32;
                        false
                    }
                    _ => exit(0),
                };

                for c in chars {
                    if c.is_ascii_digit() {
                        code = code * 10 + (c - b'0') as i32;
                    } else {
                        exit(0);
                    }
                }
                if neg {
                    code = -code;
                }

                // exit code is in range [0, 255]
                exit(code);
            }
            ".headers" => {
                if args.len() != 1 {
                    writeln!(self.out, "Usage: .headers on|off")?;
                    return Ok(());
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
                        )?
                    }
                }
            }
            ".help" => self.show_help(args),
            ".indexes" => result = Some(self.list_tables(args.first().copied(), true)),
            ".load" => {
                let (filename, entrypoint) = match args.len() {
                    1 => (args[0], None),
                    2 => (args[0], Some(args[1])),
                    _ => {
                        writeln!(self.out, "Usage: .load FILENAME ?ENTRYPOINT?")?;
                        return Ok(());
                    }
                };
                if let Err(e) = load_my_extension(&self.db, filename, entrypoint) {
                    println!("Error: {}", e);
                }
            }
            ".nullvalue" => {
                if args.len() != 1 {
                    writeln!(self.out, "Usage: .nullvalue STRING")?;
                    return Ok(());
                }
                self.null_value = args[0].to_string();
            }
            ".print" => {
                writeln!(self.out, "{}", args.join(" "))?;
            }
            ".prompt" => {
                if !args.is_empty() {
                    self.main_prompt = args[0].to_string();
                }
                if args.len() > 1 {
                    self.continuation_prompt = args[1].to_string();
                }
            }
            ".quit" => exit(0),
            ".open" => {
                // .open ?OPTIONS? ?FILE?
                let mut filename = None;
                let mut flags = OpenFlags::default();
                for arg in args {
                    match *arg {
                        "--append" | "--deserialize" | "--hexdb" | "--maxsize" | "--zip" => {
                            println!("`{}` is not supported yet", arg);
                            return Ok(());
                        }
                        "--new" => flags.insert(OpenFlags::SQLITE_OPEN_CREATE),
                        "--nofollow" => flags.insert(OpenFlags::SQLITE_OPEN_NOFOLLOW),
                        "--readonly" => {
                            flags.remove(OpenFlags::SQLITE_OPEN_CREATE);
                            flags.remove(OpenFlags::SQLITE_OPEN_READ_WRITE);
                            flags.insert(OpenFlags::SQLITE_OPEN_READ_ONLY);
                        }
                        arg => {
                            if arg.starts_with('-') {
                                println!("unknown option: {}", arg);
                                return Ok(());
                            }

                            if filename.is_some() {
                                println!("extra argument: \"{}\"", arg);
                                return Ok(());
                            }

                            filename = Some(arg);
                        }
                    }
                }

                (self.filename, self.db) = match filename {
                    Some(path) => {
                        let db = match Connection::open_with_flags(path, flags) {
                            Ok(con) => con,
                            Err(e) => {
                                println!("Error: unable to open database \"{}\": {}\nNotice: using substitute in-memory database instead of \"{}\"", path, e, path);
                                return Ok(());
                            }
                        };
                        (path.into(), db)
                    }
                    None => {
                        let db = match Connection::open_in_memory() {
                            Ok(con) => con,
                            Err(_e) => {
                                println!("Error: unable to open database in memory");
                                return Ok(());
                            }
                        };
                        ("".into(), db)
                    }
                };
            }
            ".read" => {
                if args.len() != 1 {
                    writeln!(self.out, "Usage: .read FILE")?;
                    return Ok(());
                }

                let filename = args[0];
                let reader = match std::fs::File::open(filename) {
                    Ok(file) => BufReader::new(file),
                    Err(_e) => {
                        println!("Error: cannot open \"{}\"", args[0]);
                        return Ok(());
                    }
                };
                for (i, line) in reader.lines().enumerate() {
                    let statement = line?;
                    match self.run_statement(statement, (), false) {
                        Ok(table) => {
                            if !table.is_empty() {
                                writeln!(self.out, "{}", table)?;
                            }
                            if self.changes {
                                unsafe {
                                    let db = self.db.handle();
                                    let changes = sqlite3_changes64(db);
                                    let total_changes = sqlite3_total_changes64(db);
                                    writeln!(
                                        self.out,
                                        "changes: {}   total_changes: {}",
                                        changes, total_changes
                                    )?;
                                }
                            }
                        }
                        Err(e) => {
                            println!("Parse error near line {}: {}", i + 1, e);
                            if self.bail {
                                break;
                            }
                        }
                    }
                }
            }
            ".show" => {
                if !args.is_empty() {
                    writeln!(self.out, "Usage: .show")?;
                    return Ok(());
                }

                let out_name = format!("{}", self.out);
                write!(
                    self.out,
                    r#"{:>12}: {}
{:>12}: {}
{:>12}: {}
{:>12}: {}
{:>12}: {}
{:>12}: "{}"
{:>12}: {}
{:>12}: {:?}
{:>12}: {:?}
{:>12}: {}
{:>12}: {:?}
{:>12}: {}
"#,
                    "echo",
                    if self.echo { "on" } else { "off" },
                    "eqp",
                    if self.eqp { "on" } else { "off" },
                    "explain",
                    self.explain,
                    "headers",
                    if self.headers { "on" } else { "off" },
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
                )?;
            }
            ".tables" => result = Some(self.list_tables(args.first().copied(), false)),
            _ => println!(
                "Error: unknown command or invalid arguments: \"{}\". Enter \".help\" for help",
                command
            ),
        }
        match result {
            Some(Ok(mut table)) => {
                if table.count_rows() != 0 {
                    table.with(Style::blank());
                    writeln!(self.out, "{}", table)?;
                }
            }
            Some(Err(e)) => {
                println!("Error: {e}");
            }
            None => {}
        }
        Ok(())
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
            for row in rows.flatten() {
                mapped_rows.push(row);
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
        let help = r#"
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
.load FILE ?ENTRY?       Load an extension library
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
"#;
        _ = writeln!(self.out, "{}", help.trim());
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
    let shell = Shell::new(args)?;
    let result = shell.run(&mut rl);
    rl.save_history(history.as_path()).ok();
    result
}

fn toggle_option(name: &str, value: &mut bool, args: &[&str]) {
    if args.len() != 1 {
        println!("Usage: {} on|off", name);
        return;
    }

    match args[0].to_lowercase().as_str() {
        "on" | "true" | "yes" => *value = true,
        "off" | "false" | "no" => *value = false,
        arg => {
            // FIXME Run with `.bail '123"`, it should not be legal, but `split_terminator`
            // return args as ["123"] which however work here.
            // It's not a big problem, but it doesn't behave same as `sqlite`.
            if arg.chars().all(|a| a.is_ascii_digit()) {
                *value = arg != "0";
            } else {
                *value = false;
                println!("ERROR: Not a boolean value: \"{}\". Assuming \"no\"", arg)
            }
        }
    }
}

fn load_my_extension<P: AsRef<Path>>(
    conn: &Connection,
    dylib_path: P,
    entry_point: Option<&str>,
) -> rusqlite::Result<()> {
    let _guard = unsafe { LoadExtensionGuard::new(conn)? };
    unsafe { conn.load_extension(dylib_path, entry_point) }
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
            no_follow: false,
            command: None,
        };
        let shell = Shell::new(cli).unwrap();
        assert!(shell.headers);
        let result = shell.run_statement(" ; ; ;".to_string(), [], false);
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table.count_rows(), 1);
    }
}
