use alloc::format;
use alloc::string::String;
use core::ffi::c_int;
use sqlite::value;
use sqlite::Value;
use sqlite_nostd as sqlite;

// TODO: add an integration test that ensures NULL == NULL!
pub fn crsql_compare_sqlite_values(l: *mut sqlite::value, r: *mut sqlite::value) -> c_int {
    let l_type = l.value_type();
    let r_type = r.value_type();

    if l_type != r_type {
        // We swap the compare since we want null to be _less than_ all things
        // and null is assigned to ordinal 5 (greatest thing).
        return (r_type as i32) - (l_type as i32);
    }

    match l_type {
        sqlite::ColumnType::Blob => l.blob().cmp(r.blob()) as c_int,
        sqlite::ColumnType::Float => {
            let l_double = l.double();
            let r_double = r.double();
            if l_double < r_double {
                return -1;
            } else if l_double > r_double {
                return 1;
            }
            return 0;
        }
        sqlite::ColumnType::Integer => {
            let l_int = l.int64();
            let r_int = r.int64();
            // no subtraction since that could overflow the c_int return type
            if l_int < r_int {
                return -1;
            } else if l_int > r_int {
                return 1;
            }
            return 0;
        }
        sqlite::ColumnType::Null => 0,
        sqlite::ColumnType::Text => l.text().cmp(r.text()) as c_int,
    }
}

pub fn any_value_changed(left: &[*mut value], right: &[*mut value]) -> Result<bool, String> {
    if left.len() != right.len() {
        return Err(format!(
            "left and right values must have the same length: {} != {}",
            left.len(),
            right.len()
        ));
    }

    for (l, r) in left.iter().zip(right.iter()) {
        if crsql_compare_sqlite_values(*l, *r) != 0 {
            return Ok(true);
        }
    }

    Ok(false)
}
