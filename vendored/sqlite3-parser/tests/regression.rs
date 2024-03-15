use fallible_iterator::FallibleIterator;
use libsql_sqlite3_parser::lexer::sql::Parser;

const TESTCASES: &[&[u8]] = &[
    include_bytes!("./regression_files/on_missing_join.sql"),
    include_bytes!("./regression_files/bad_table_arg1.sql"),
    include_bytes!("./regression_files/bad_table_arg2.sql"),
    include_bytes!("./regression_files/bad_table_arg3.sql"),
];

#[test]
fn regressions() {
    for test_case in TESTCASES {
        let mut parser = Parser::new(test_case);
        loop {
            match parser.next() {
                Ok(Some(_)) => (),
                Ok(None) | Err(_) => break,
            }
        }
    }
}
