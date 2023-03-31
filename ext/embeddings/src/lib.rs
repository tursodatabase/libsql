#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{c_char, c_int, c_void, CStr};

fn sentence_embeddings_rs(sentence: &str) -> Result<Vec<f32>, rust_bert::RustBertError> {
    use rust_bert::pipelines::sentence_embeddings::{
        SentenceEmbeddingsBuilder, SentenceEmbeddingsModelType,
    };
    let model = SentenceEmbeddingsBuilder::remote(SentenceEmbeddingsModelType::AllMiniLmL12V2)
        .create_model()
        .unwrap();

    let sentences = [sentence.to_string()];

    let mut embeddings = model.encode(&sentences)?;
    Ok(embeddings.pop().unwrap())
}

extern "C" {
    fn embeddings_c_init(a: *mut c_void, b: *mut c_void, c: *mut c_void, d: *mut c_void);
}

#[no_mangle]
pub fn sqlite3_embeddings_init(a: *mut c_void, b: *mut c_void, c: *mut c_void, d: *mut c_void) {
    unsafe { embeddings_c_init(a, b, c, d) }
}

#[no_mangle]
pub extern "C" fn sentence_embeddings(
    sentence: *const c_char,
    sentence_len: c_int,
    out: *mut c_char,
) -> c_int {
    let sentence = unsafe { CStr::from_ptr(sentence).to_str().unwrap() };
    let embeddings = match sentence_embeddings_rs(sentence) {
        Ok(embeddings) => embeddings,
        Err(_) => {
            return 1;
        }
    };
    let embeddings = embeddings.as_ptr();
    let embeddings = embeddings as *const c_char;
    unsafe {
        std::ptr::copy(
            embeddings,
            out,
            sentence_len as usize * std::mem::size_of::<f32>(),
        );
    }
    0
}

#[cfg(test)]
mod tests {
    #[test]
    fn embeddings1() {
        for sentence in ["apple", "banana", "orange"].iter() {
            let embedding = super::sentence_embeddings_rs(sentence);
            println!("Embedding for {sentence}: {embedding:?}");
        }
    }
}
