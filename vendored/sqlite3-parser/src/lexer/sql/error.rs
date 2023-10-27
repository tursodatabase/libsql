use std::error;
use std::fmt;
use std::io;

use crate::lexer::scan::ScanError;
use crate::parser::ParserError;

#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    /// I/O Error
    Io(io::Error),
    UnrecognizedToken(Option<(u64, usize)>),
    UnterminatedLiteral(Option<(u64, usize)>),
    UnterminatedBracket(Option<(u64, usize)>),
    UnterminatedBlockComment(Option<(u64, usize)>),
    BadVariableName(Option<(u64, usize)>),
    BadNumber(Option<(u64, usize)>),
    ExpectedEqualsSign(Option<(u64, usize)>),
    MalformedBlobLiteral(Option<(u64, usize)>),
    MalformedHexInteger(Option<(u64, usize)>),
    ParserError(ParserError, Option<(u64, usize)>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::Io(ref err) => err.fmt(f),
            Error::UnrecognizedToken(pos) => write!(f, "unrecognized token at {:?}", pos.unwrap()),
            Error::UnterminatedLiteral(pos) => {
                write!(f, "non-terminated literal at {:?}", pos.unwrap())
            }
            Error::UnterminatedBracket(pos) => {
                write!(f, "non-terminated bracket at {:?}", pos.unwrap())
            }
            Error::UnterminatedBlockComment(pos) => {
                write!(f, "non-terminated block comment at {:?}", pos.unwrap())
            }
            Error::BadVariableName(pos) => write!(f, "bad variable name at {:?}", pos.unwrap()),
            Error::BadNumber(pos) => write!(f, "bad number at {:?}", pos.unwrap()),
            Error::ExpectedEqualsSign(pos) => write!(f, "expected = sign at {:?}", pos.unwrap()),
            Error::MalformedBlobLiteral(pos) => {
                write!(f, "malformed blob literal at {:?}", pos.unwrap())
            }
            Error::MalformedHexInteger(pos) => {
                write!(f, "malformed hex integer at {:?}", pos.unwrap())
            }
            Error::ParserError(ref msg, pos) => write!(f, "{} at {:?}", msg, pos.unwrap()),
        }
    }
}

impl error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<ParserError> for Error {
    fn from(err: ParserError) -> Error {
        Error::ParserError(err, None)
    }
}

impl ScanError for Error {
    fn position(&mut self, line: u64, column: usize) {
        match *self {
            Error::Io(_) => {}
            Error::UnrecognizedToken(ref mut pos) => *pos = Some((line, column)),
            Error::UnterminatedLiteral(ref mut pos) => *pos = Some((line, column)),
            Error::UnterminatedBracket(ref mut pos) => *pos = Some((line, column)),
            Error::UnterminatedBlockComment(ref mut pos) => *pos = Some((line, column)),
            Error::BadVariableName(ref mut pos) => *pos = Some((line, column)),
            Error::BadNumber(ref mut pos) => *pos = Some((line, column)),
            Error::ExpectedEqualsSign(ref mut pos) => *pos = Some((line, column)),
            Error::MalformedBlobLiteral(ref mut pos) => *pos = Some((line, column)),
            Error::MalformedHexInteger(ref mut pos) => *pos = Some((line, column)),
            Error::ParserError(_, ref mut pos) => *pos = Some((line, column)),
        }
    }
}
