#![cfg(test)]

//! Tests to verify wincode binary compatibility with bincode 1.3.3.
//!
//! These tests ensure that the migration from bincode to wincode maintains
//! wire protocol compatibility with existing clients (particularly the Go client).

use crate::query::Value;

#[test]
fn test_wincode_produces_same_bytes_as_bincode() {
    // Test that wincode produces identical bytes to what bincode 1.3.3 would produce.
    // This is critical for maintaining wire protocol compatibility.
    
    let test_cases = vec![
        ("Null", Value::Null, vec![0, 0, 0, 0]),
        (
            "Integer(42)",
            Value::Integer(42),
            vec![1, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            "Integer(i64::MAX)",
            Value::Integer(i64::MAX),
            vec![1, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 127],
        ),
        (
            "Real(3.14159)",
            Value::Real(3.14159),
            vec![2, 0, 0, 0, 110, 134, 27, 240, 249, 33, 9, 64],
        ),
        (
            "Text(empty)",
            Value::Text(String::new()),
            vec![3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            "Text(Hello)",
            Value::Text("Hello".to_string()),
            vec![3, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 72, 101, 108, 108, 111],
        ),
        (
            "Blob([1,2,3])",
            Value::Blob(vec![1, 2, 3]),
            vec![4, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3],
        ),
    ];

    for (name, value, expected_bytes) in test_cases {
        let serialised = wincode::serialize(&value).expect("wincode serialisation failed");
        
        assert_eq!(
            serialised, expected_bytes,
            "{}: wincode output differs from expected bincode output",
            name
        );

        // Also verify roundtrip
        let deserialised: Value = wincode::deserialize(&serialised)
            .expect("wincode deserialisation failed");
        
        assert_eq!(
            format!("{:?}", value),
            format!("{:?}", deserialised),
            "{}: roundtrip failed",
            name
        );
    }
}

#[test]
fn test_wincode_roundtrip_all_variants() {
    // Verify that all Value variants can be serialised and deserialised correctly.
    let values = vec![
        Value::Null,
        Value::Integer(0),
        Value::Integer(i64::MIN),
        Value::Integer(i64::MAX),
        Value::Real(0.0),
        Value::Real(std::f64::consts::PI),
        Value::Real(f64::MIN),
        Value::Real(f64::MAX),
        Value::Text(String::new()),
        Value::Text("test".to_string()),
        Value::Text("unicode: 🦀🚀".to_string()),
        Value::Blob(vec![]),
        Value::Blob(vec![0u8, 255u8]),
        Value::Blob(vec![1, 2, 3, 4, 5]),
    ];

    for value in &values {
        let encoded = wincode::serialize(value).expect("encode failed");
        let decoded: Value = wincode::deserialize(&encoded).expect("decode failed");
        assert_eq!(
            format!("{:?}", value),
            format!("{:?}", decoded),
            "roundtrip failed for {:?}",
            value
        );
    }
}
