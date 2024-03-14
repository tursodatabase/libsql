#[macro_use]
extern crate afl;
use fallible_iterator::FallibleIterator;

use sqlite3_parser::lexer::sql::Parser;

fn main() {
    let mut args = std::env::args();
    match args.nth(1).as_deref() {
        Some("parser") => {
            fuzz!(|data: &[u8]| {
                let mut parser = Box::new(Parser::new(data));
                while let Ok(Some(_)) = parser.next() { }
            });
        }
        _ => panic!(),
    }
}
