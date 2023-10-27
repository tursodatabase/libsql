use fallible_iterator::FallibleIterator;

use super::{Error, Parser};
use crate::parser::{
    ast::{Cmd, Name, ParameterInfo, QualifiedName, Stmt, ToTokens},
    ParserError,
};

#[test]
fn count_placeholders() -> Result<(), Error> {
    let sql = "SELECT ? WHERE 1 = ?";
    let mut parser = Parser::new(sql.as_bytes());
    let ast = parser.next()?.unwrap();
    let mut info = ParameterInfo::default();
    ast.to_tokens(&mut info).unwrap();
    assert_eq!(info.count, 2);
    Ok(())
}

#[test]
fn count_numbered_placeholders() -> Result<(), Error> {
    let sql = "SELECT ?1 WHERE 1 = ?2 AND 0 = ?1";
    let mut parser = Parser::new(sql.as_bytes());
    let ast = parser.next()?.unwrap();
    let mut info = ParameterInfo::default();
    ast.to_tokens(&mut info).unwrap();
    assert_eq!(info.count, 2);
    Ok(())
}

#[test]
fn count_unused_placeholders() -> Result<(), Error> {
    let sql = "SELECT ?1 WHERE 1 = ?3";
    let mut parser = Parser::new(sql.as_bytes());
    let ast = parser.next()?.unwrap();
    let mut info = ParameterInfo::default();
    ast.to_tokens(&mut info).unwrap();
    assert_eq!(info.count, 3);
    Ok(())
}

#[test]
fn count_named_placeholders() -> Result<(), Error> {
    let sql = "SELECT :x, :y WHERE 1 = :y";
    let mut parser = Parser::new(sql.as_bytes());
    let ast = parser.next()?.unwrap();
    let mut info = ParameterInfo::default();
    ast.to_tokens(&mut info).unwrap();
    assert_eq!(info.count, 2);
    assert_eq!(info.names.len(), 2);
    assert!(info.names.contains(":x"));
    assert!(info.names.contains(":y"));
    Ok(())
}

#[test]
fn duplicate_column() {
    let sql = "CREATE TABLE t (x TEXT, x TEXT)";
    let mut parser = Parser::new(sql.as_bytes());
    let r = parser.next();
    let Error::ParserError(ParserError::Custom(msg), _) = r.unwrap_err() else {
        panic!("unexpected error type")
    };
    assert!(msg.contains("duplicate column name"));
}

#[test]
fn vtab_args() -> Result<(), Error> {
    let sql = r#"CREATE VIRTUAL TABLE mail USING fts3(
  subject VARCHAR(256) NOT NULL,
  body TEXT CHECK(length(body)<10240)
);"#;
    let mut parser = Parser::new(sql.as_bytes());
    let Cmd::Stmt(Stmt::CreateVirtualTable {
        tbl_name: QualifiedName {
            name: Name(tbl_name),
            ..
        },
        module_name: Name(module_name),
        args: Some(args),
        ..
    }) = parser.next()?.unwrap()
    else {
        panic!("unexpected AST")
    };
    assert_eq!(tbl_name, "mail");
    assert_eq!(module_name, "fts3");
    assert_eq!(args.len(), 2);
    assert_eq!(args[0], "subject VARCHAR(256) NOT NULL");
    assert_eq!(args[1], "body TEXT CHECK(length(body)<10240)");
    Ok(())
}

#[test]
fn only_semicolons_no_statements() {
    let sqls = ["", ";", ";;;"];
    for sql in sqls.iter() {
        let mut parser = Parser::new(sql.as_bytes());
        assert_eq!(parser.next().unwrap(), None);
    }
}

#[test]
fn extra_semicolons_between_statements() {
    let sqls = [
        "SELECT 1; SELECT 2",
        "SELECT 1; SELECT 2;",
        "; SELECT 1; SELECT 2",
        ";; SELECT 1;; SELECT 2;;",
    ];
    for sql in sqls.iter() {
        let mut parser = Parser::new(sql.as_bytes());
        assert!(matches!(
            parser.next().unwrap(),
            Some(Cmd::Stmt(Stmt::Select { .. }))
        ));
        assert!(matches!(
            parser.next().unwrap(),
            Some(Cmd::Stmt(Stmt::Select { .. }))
        ));
        assert_eq!(parser.next().unwrap(), None);
    }
}
