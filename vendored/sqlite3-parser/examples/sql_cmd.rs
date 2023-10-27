use fallible_iterator::FallibleIterator;
use sqlite3_parser::lexer::sql::Parser;

/// Parse a string.
// RUST_LOG=sqlite3Parser=debug
fn main() {
    env_logger::init();
    let arg = "PRAGMA parser_trace=ON;";
    let mut parser = Parser::new(arg.as_bytes());
    loop {
        match parser.next() {
            Ok(None) => break,
            Err(err) => {
                eprintln!("Err: {err} in {arg}");
                break;
            }
            Ok(Some(cmd)) => {
                println!("{cmd}");
            }
        }
    }
}
