extern crate alloc;

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use bytes::{Buf, BufMut};
use core::slice;
#[cfg(not(feature = "std"))]
use num_traits::FromPrimitive;
use sqlite_nostd as sqlite;
use sqlite_nostd::{ColumnType, Context, ResultCode, Stmt, Value};

pub extern "C" fn crsql_pack_columns(
    ctx: *mut sqlite::context,
    argc: i32,
    argv: *mut *mut sqlite::value,
) {
    let args = sqlite::args!(argc, argv);

    match pack_columns(args) {
        Err(code) => {
            ctx.result_error("Failed to pack columns");
            ctx.result_error_code(code);
        }
        Ok(blob) => {
            // TODO: pass a destructor so we don't have to copy the blob
            ctx.result_blob_owned(blob);
        }
    }
}

fn pack_columns(args: &[*mut sqlite::value]) -> Result<Vec<u8>, ResultCode> {
    let mut buf = vec![];
    /*
     * Format:
     * [num_columns:u8,...[(type(0-3),num_bytes?(3-7)):u8, length?:i32, ...bytes:u8[]]]
     *
     * The byte used for column type also encodes the number of bytes used for the integer.
     * e.g.: (type(0-3),num_bytes?(3-7)):u8
     * first 3 bits are type
     * last 5 encode how long the following integer, if there is a following integer, is. 1, 2, 3, ... 8 bytes.
     *
     * Not packing an integer into the minimal number of bytes required is rather wasteful.
     * E.g., the number `0` would take 8 bytes rather than 1 byte.
     */
    let len_result: Result<u8, _> = args.len().try_into();
    if let Ok(len) = len_result {
        buf.put_u8(len);
        for value in args {
            match value.value_type() {
                ColumnType::Blob => {
                    let len = value.bytes();
                    let num_bytes_for_len = num_bytes_needed_i32(len);
                    let type_byte = num_bytes_for_len << 3 | (ColumnType::Blob as u8);
                    buf.put_u8(type_byte);
                    buf.put_int(len as i64, num_bytes_for_len as usize);
                    buf.put_slice(value.blob());
                }
                ColumnType::Null => {
                    buf.put_u8(ColumnType::Null as u8);
                }
                ColumnType::Float => {
                    buf.put_u8(ColumnType::Float as u8);
                    buf.put_f64(value.double());
                }
                ColumnType::Integer => {
                    let val = value.int64();
                    let num_bytes_for_int = num_bytes_needed_i64(val);
                    let type_byte = num_bytes_for_int << 3 | (ColumnType::Integer as u8);
                    buf.put_u8(type_byte);
                    buf.put_int(val, num_bytes_for_int as usize);
                }
                ColumnType::Text => {
                    let len = value.bytes();
                    let num_bytes_for_len = num_bytes_needed_i32(len);
                    let type_byte = num_bytes_for_len << 3 | (ColumnType::Text as u8);
                    buf.put_u8(type_byte);
                    buf.put_int(len as i64, num_bytes_for_len as usize);
                    buf.put_slice(value.blob());
                }
            }
        }
        Ok(buf)
    } else {
        Err(ResultCode::ABORT)
    }
}

fn num_bytes_needed_i32(val: i32) -> u8 {
    if val & 0xFF000000u32 as i32 != 0 {
        return 4;
    } else if val & 0x00FF0000 != 0 {
        return 3;
    } else if val & 0x0000FF00 != 0 {
        return 2;
    } else if val * 0x000000FF != 0 {
        return 1;
    } else {
        return 0;
    }
}

fn num_bytes_needed_i64(val: i64) -> u8 {
    if val & 0xFF00000000000000u64 as i64 != 0 {
        return 8;
    } else if val & 0x00FF000000000000 != 0 {
        return 7;
    } else if val & 0x0000FF0000000000 != 0 {
        return 6;
    } else if val & 0x000000FF00000000 != 0 {
        return 5;
    } else {
        return num_bytes_needed_i32(val as i32);
    }
}

pub enum ColumnValue {
    Blob(Vec<u8>),
    Float(f64),
    Integer(i64),
    Null,
    Text(String),
}

// TODO: make a table valued function that can be used to extract a row per packed column?
pub fn unpack_columns(data: &[u8]) -> Result<Vec<ColumnValue>, ResultCode> {
    let mut ret = vec![];
    let mut buf = data;
    let num_columns = buf.get_u8();

    for _i in 0..num_columns {
        if !buf.has_remaining() {
            return Err(ResultCode::ABORT);
        }
        let column_type_and_maybe_intlen = buf.get_u8();
        let column_type = ColumnType::from_u8(column_type_and_maybe_intlen & 0x07);
        let intlen = (column_type_and_maybe_intlen >> 3 & 0xFF) as usize;

        match column_type {
            Some(ColumnType::Blob) => {
                if buf.remaining() < intlen {
                    return Err(ResultCode::ABORT);
                }
                let len = buf.get_int(intlen) as usize;
                if buf.remaining() < len {
                    return Err(ResultCode::ABORT);
                }
                let bytes = buf.copy_to_bytes(len);
                ret.push(ColumnValue::Blob(bytes.to_vec()));
            }
            Some(ColumnType::Float) => {
                if buf.remaining() < 8 {
                    return Err(ResultCode::ABORT);
                }
                ret.push(ColumnValue::Float(buf.get_f64()));
            }
            Some(ColumnType::Integer) => {
                if buf.remaining() < intlen {
                    return Err(ResultCode::ABORT);
                }
                ret.push(ColumnValue::Integer(buf.get_int(intlen)));
            }
            Some(ColumnType::Null) => {
                ret.push(ColumnValue::Null);
            }
            Some(ColumnType::Text) => {
                if buf.remaining() < intlen {
                    return Err(ResultCode::ABORT);
                }
                let len = buf.get_int(intlen) as usize;
                if buf.remaining() < len {
                    return Err(ResultCode::ABORT);
                }
                let bytes = buf.copy_to_bytes(len);
                ret.push(ColumnValue::Text(unsafe {
                    String::from_utf8_unchecked(bytes.to_vec())
                }))
            }
            None => return Err(ResultCode::MISUSE),
        }
    }

    Ok(ret)
}

pub fn bind_package_to_stmt(
    stmt: *mut sqlite::stmt,
    values: &Vec<crate::ColumnValue>,
    offset: usize,
) -> Result<ResultCode, ResultCode> {
    for (i, val) in values.iter().enumerate() {
        bind_slot(i + 1 + offset, val, stmt)?;
    }
    Ok(ResultCode::OK)
}

fn bind_slot(
    slot_num: usize,
    val: &ColumnValue,
    stmt: *mut sqlite::stmt,
) -> Result<ResultCode, ResultCode> {
    match val {
        ColumnValue::Blob(b) => stmt.bind_blob(slot_num as i32, b, sqlite::Destructor::STATIC),
        ColumnValue::Float(f) => stmt.bind_double(slot_num as i32, *f),
        ColumnValue::Integer(i) => stmt.bind_int64(slot_num as i32, *i),
        ColumnValue::Null => stmt.bind_null(slot_num as i32),
        ColumnValue::Text(t) => stmt.bind_text(slot_num as i32, t, sqlite::Destructor::STATIC),
    }
}
