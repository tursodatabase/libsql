#![allow(clippy::missing_safety_doc)]

pub enum ValueType {
    Integer,
    Float,
    Blob,
    Text,
    Null,
}

impl ValueType {
    pub fn from(val_type: i32) -> ValueType {
        match val_type as u32 {
            crate::ffi::SQLITE_INTEGER => ValueType::Integer,
            crate::ffi::SQLITE_FLOAT => ValueType::Float,
            crate::ffi::SQLITE_BLOB => ValueType::Blob,
            crate::ffi::SQLITE_TEXT => ValueType::Text,
            crate::ffi::SQLITE_NULL => ValueType::Null,
            _ => todo!(),
        }
    }
}

pub struct Value {
    pub raw_value: *mut crate::ffi::sqlite3_value,
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        let raw_type = unsafe { crate::ffi::sqlite3_value_type(self.raw_value) };
        ValueType::from(raw_type)
    }

    pub fn int(&self) -> i32 {
        unsafe { crate::ffi::sqlite3_value_int(self.raw_value) }
    }

    pub fn text(&self) -> *const u8 {
        unsafe { crate::ffi::sqlite3_value_text(self.raw_value) }
    }
}
