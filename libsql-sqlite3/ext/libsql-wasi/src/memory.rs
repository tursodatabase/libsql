// Shamelessly stolen from Honza - thx man!!!

use crate::{Error, Result};

pub type Ptr = i32;

pub fn slice(memory: &[u8], ptr: Ptr, len: usize) -> Result<&[u8]> {
    let ptr = ptr as usize;
    assert!(ptr != 0 && ptr <= memory.len(), "Invalid pointer");
    assert!(ptr + len <= memory.len(), "Invalid pointer and length");
    Ok(&memory[ptr..][..len])
}

pub fn slice_mut(memory: &mut [u8], ptr: Ptr, len: usize) -> Result<&mut [u8]> {
    let ptr = ptr as usize;
    assert!(ptr != 0 && ptr <= memory.len(), "Invalid pointer");
    assert!(ptr + len <= memory.len(), "Invalid pointer and length");
    Ok(&mut memory[ptr..][..len])
}

pub fn read_vec(memory: &[u8], ptr: Ptr, len: usize) -> Result<Vec<u8>> {
    slice(memory, ptr, len).map(|slice| slice.to_vec())
}

pub fn read_cstr(memory: &[u8], cstr_ptr: Ptr) -> Result<String> {
    let Some(data) = read_cstr_bytes(memory, cstr_ptr) else {
        return Err(Error::MemoryError("Invalid pointer to C string"));
    };
    String::from_utf8(data).map_err(|_| Error::MemoryError("Invalid UTF-8 in C string"))
}

pub fn read_cstr_or_null(memory: &[u8], cstr_ptr: Ptr) -> Result<Option<String>> {
    if cstr_ptr != 0 {
        read_cstr(memory, cstr_ptr).map(Some)
    } else {
        Ok(None)
    }
}

pub fn read_cstr_lossy(memory: &[u8], cstr_ptr: Ptr) -> String {
    match read_cstr_bytes(memory, cstr_ptr) {
        Some(data) => match String::from_utf8(data) {
            Ok(string) => string,
            Err(err) => String::from_utf8_lossy(err.as_bytes()).into_owned(),
        },
        None => String::new(),
    }
}

pub fn read_cstr_bytes(memory: &[u8], cstr_ptr: Ptr) -> Option<Vec<u8>> {
    let cstr_ptr = cstr_ptr as usize;
    if cstr_ptr == 0 || cstr_ptr >= memory.len() {
        return None;
    }

    let data = &memory[cstr_ptr..];
    let mut strlen = 0;
    loop {
        match data.get(strlen) {
            None => return None,
            Some(0) => break,
            Some(_) => strlen += 1,
        }
    }

    Some(data[..strlen].to_vec())
}
