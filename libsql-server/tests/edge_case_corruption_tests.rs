//! Edge case corruption tests for maximum bug discovery
//! 
//! Created by hamisionesmus for Turso bug bounty program
//! These tests target specific edge cases and boundary conditions
//! that are most likely to expose data corruption bugs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use libsql::Database;
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::{Barrier, Notify};
use tokio::time::{sleep, timeout};
use turmoil::{Builder, Sim};

use crate::common::http::Client;
use crate::common::net::{init_tracing, SimServer, TestServer, TurmoilAcceptor, TurmoilConnector};

/// Test for boundary value corruption in integer fields
/// This test targets integer overflow/underflow scenarios
#[test]
fn integer_boundary_corruption_test() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(200))
        .build();

    let tmp = tempdir().unwrap();
    let db_path = tmp.path().to_owned();

    init_tracing();
    sim.host("primary", move || {
        let path = db_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 6,
                    max_log_duration: Some(0.3),
                    ..Default::default()
                },
                user_api_config: crate::config::UserApiConfig::default(),
                admin_api_config: Some(crate::config::AdminApiConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                    connector: TurmoilConnector,
                    disable_metrics: false,
                    auth_key: None,
                }),
                rpc_server_config: Some(crate::config::RpcServerConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await?,
                    tls_config: None,
                }),
                disable_namespaces: false,
                disable_default_namespace: true,
                ..Default::default()
            };

            server.start_sim(8080).await?;
            Ok(())
        }
    });

    sim.client("boundary_tester", async move {
        let client = Client::new();
        
        client
            .post("http://primary:9090/v1/namespaces/testdb/create", json!({}))
            .await?;

        let db = Database::open_remote_with_connector(
            "http://testdb.primary:8080", 
            "", 
            TurmoilConnector
        )?;
        let conn = db.connect()?;

        // Create table with various integer types
        conn.execute(
            "CREATE TABLE boundary_test (
                id INTEGER PRIMARY KEY,
                tiny_int INTEGER,
                big_int INTEGER,
                counter INTEGER DEFAULT 0,
                checksum TEXT NOT NULL
            )", 
            ()
        ).await?;

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(4));

        let mut handles = vec![];

        // Test boundary values with concurrent access
        for worker_id in 0..3 {
            let barrier = barrier.clone();
            let corruption_detected = corruption_detected.clone();
            
            let handle = tokio::spawn(async move {
                let db = Database::open_remote_with_connector(
                    "http://testdb.primary:8080", 
                    "", 
                    TurmoilConnector
                ).unwrap();
                let conn = db.connect().unwrap();

                barrier.wait().await;

                // Test various boundary values
                let boundary_values = vec![
                    i64::MAX,
                    i64::MIN,
                    i64::MAX - 1,
                    i64::MIN + 1,
                    0,
                    -1,
                    1,
                    i32::MAX as i64,
                    i32::MIN as i64,
                    u32::MAX as i64,
                ];

                for (iteration, &value) in boundary_values.iter().enumerate() {
                    let record_id = worker_id * 1000 + iteration as i64;
                    let checksum = format!("boundary_chk_{}_{}", worker_id, value);
                    
                    // Insert boundary value
                    conn.execute(
                        "INSERT INTO boundary_test (id, tiny_int, big_int, checksum) VALUES (?, ?, ?, ?)",
                        (record_id, value % 256, value, checksum)
                    ).await.unwrap();
                    
                    // Perform arithmetic operations that might overflow
                    conn.execute(
                        "UPDATE boundary_test SET counter = counter + ? WHERE id = ?",
                        (value / 1000000, record_id)
                    ).await.unwrap();
                    
                    // Read back and verify
                    let mut stmt = conn.prepare("SELECT tiny_int, big_int, counter, checksum FROM boundary_test WHERE id = ?").await.unwrap();
                    let mut rows = stmt.query([record_id]).await.unwrap();
                    
                    if let Some(row) = rows.next().await.unwrap() {
                        let tiny_int: i64 = row.get(0).unwrap();
                        let big_int: i64 = row.get(1).unwrap();
                        let counter: i64 = row.get(2).unwrap();
                        let retrieved_checksum: String = row.get(3).unwrap();
                        
                        // Verify values
                        if big_int != value {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("BOUNDARY CORRUPTION: big_int mismatch for worker {} value {}: expected {}, got {}", 
                                worker_id, value, value, big_int);
                        }
                        
                        if tiny_int != value % 256 {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("BOUNDARY CORRUPTION: tiny_int mismatch for worker {} value {}: expected {}, got {}", 
                                worker_id, value, value % 256, tiny_int);
                        }
                        
                        if retrieved_checksum != checksum {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("BOUNDARY CORRUPTION: checksum mismatch for worker {} value {}", worker_id, value);
                        }
                    }
                    
                    sleep(Duration::from_millis(50)).await;
                }
                
                worker_id
            });
            
            handles.push(handle);
        }

        // Network disruption during boundary operations
        let disruption_handle = tokio::spawn(async move {
            barrier.wait().await;
            
            for _ in 0..5 {
                sleep(Duration::from_secs(8)).await;
                turmoil::hold("primary");
                sleep(Duration::from_millis(400)).await;
                turmoil::release("primary");
            }
        });

        for handle in handles {
            handle.await.unwrap();
        }
        
        disruption_handle.await.unwrap();

        // Final verification
        let mut verify_stmt = conn.prepare("SELECT id, tiny_int, big_int, counter FROM boundary_test ORDER BY id").await?;
        let mut verify_rows = verify_stmt.query([]).await?;
        
        let mut record_count = 0;
        while let Some(row) = verify_rows.next().await? {
            let id: i64 = row.get(0)?;
            let tiny_int: i64 = row.get(1)?;
            let big_int: i64 = row.get(2)?;
            let counter: i64 = row.get(3)?;
            
            record_count += 1;
            
            // Verify data consistency
            if tiny_int < -128 || tiny_int > 127 {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("FINAL BOUNDARY CHECK FAILED: Record {} tiny_int out of range: {}", id, tiny_int);
            }
        }

        if corruption_detected.load(Ordering::SeqCst) {
            panic!("INTEGER BOUNDARY CORRUPTION DETECTED!");
        }

        eprintln!("Boundary test completed successfully with {} records", record_count);

        Ok(())
    });

    sim.run().unwrap();
}

/// Test for Unicode and special character corruption
/// This test targets text encoding/decoding edge cases
#[test]
fn unicode_corruption_test() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(250))
        .build();

    let tmp = tempdir().unwrap();
    let db_path = tmp.path().to_owned();

    init_tracing();
    sim.host("primary", move || {
        let path = db_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 5,
                    max_log_duration: Some(0.2),
                    ..Default::default()
                },
                user_api_config: crate::config::UserApiConfig::default(),
                admin_api_config: Some(crate::config::AdminApiConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                    connector: TurmoilConnector,
                    disable_metrics: false,
                    auth_key: None,
                }),
                rpc_server_config: Some(crate::config::RpcServerConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await?,
                    tls_config: None,
                }),
                disable_namespaces: false,
                disable_default_namespace: true,
                ..Default::default()
            };

            server.start_sim(8080).await?;
            Ok(())
        }
    });

    sim.client("unicode_tester", async move {
        let client = Client::new();
        
        client
            .post("http://primary:9090/v1/namespaces/testdb/create", json!({}))
            .await?;

        let db = Database::open_remote_with_connector(
            "http://testdb.primary:8080", 
            "", 
            TurmoilConnector
        )?;
        let conn = db.connect()?;

        // Create table for Unicode testing
        conn.execute(
            "CREATE TABLE unicode_test (
                id INTEGER PRIMARY KEY,
                unicode_text TEXT NOT NULL,
                binary_data BLOB NOT NULL,
                text_length INTEGER NOT NULL,
                hash TEXT NOT NULL
            )", 
            ()
        ).await?;

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(4));

        let mut handles = vec![];

        // Test various Unicode scenarios
        for worker_id in 0..3 {
            let barrier = barrier.clone();
            let corruption_detected = corruption_detected.clone();
            
            let handle = tokio::spawn(async move {
                let db = Database::open_remote_with_connector(
                    "http://testdb.primary:8080", 
                    "", 
                    TurmoilConnector
                ).unwrap();
                let conn = db.connect().unwrap();

                barrier.wait().await;

                // Various problematic Unicode strings
                let test_strings = vec![
                    "🚀🔥💯🎉🌟", // Emojis
                    "Ñoño niño año", // Spanish accents
                    "Москва Россия", // Cyrillic
                    "北京中国", // Chinese
                    "العربية", // Arabic
                    "עברית", // Hebrew
                    "🏳️‍🌈🏳️‍⚧️", // Complex emojis with ZWJ
                    "\u{0000}\u{0001}\u{0002}", // Control characters
                    "\"'\\`\n\r\t", // Escape characters
                    "SELECT * FROM users; DROP TABLE users;--", // SQL injection attempt
                    "a".repeat(10000), // Very long string
                    "", // Empty string
                    "\u{FEFF}BOM test", // Byte Order Mark
                    "🤔🤯🥴😵‍💫🤮", // More complex emojis
                    "𝕳𝖊𝖑𝖑𝖔 𝖂𝖔𝖗𝖑𝖉", // Mathematical script
                ];

                for (iteration, test_string) in test_strings.iter().enumerate() {
                    let record_id = worker_id * 1000 + iteration as i64;
                    let binary_data = test_string.as_bytes();
                    let text_length = test_string.chars().count() as i64;
                    let hash = format!("unicode_hash_{}_{}", worker_id, iteration);
                    
                    // Insert Unicode data
                    conn.execute(
                        "INSERT INTO unicode_test (id, unicode_text, binary_data, text_length, hash) VALUES (?, ?, ?, ?, ?)",
                        (record_id, test_string, binary_data, text_length, hash)
                    ).await.unwrap();
                    
                    // Read back immediately
                    let mut stmt = conn.prepare("SELECT unicode_text, binary_data, text_length, hash FROM unicode_test WHERE id = ?").await.unwrap();
                    let mut rows = stmt.query([record_id]).await.unwrap();
                    
                    if let Some(row) = rows.next().await.unwrap() {
                        let retrieved_text: String = row.get(0).unwrap();
                        let retrieved_binary: Vec<u8> = row.get(1).unwrap();
                        let retrieved_length: i64 = row.get(2).unwrap();
                        let retrieved_hash: String = row.get(3).unwrap();
                        
                        // Verify Unicode integrity
                        if retrieved_text != *test_string {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("UNICODE CORRUPTION: Text mismatch for worker {} iteration {}", worker_id, iteration);
                            eprintln!("Expected: {:?}", test_string);
                            eprintln!("Got: {:?}", retrieved_text);
                        }
                        
                        if retrieved_binary != binary_data {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("UNICODE CORRUPTION: Binary data mismatch for worker {} iteration {}", worker_id, iteration);
                        }
                        
                        if retrieved_length != text_length {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("UNICODE CORRUPTION: Length mismatch for worker {} iteration {}: expected {}, got {}", 
                                worker_id, iteration, text_length, retrieved_length);
                        }
                        
                        if retrieved_hash != hash {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("UNICODE CORRUPTION: Hash mismatch for worker {} iteration {}", worker_id, iteration);
                        }
                    }
                    
                    // Update with more Unicode
                    let updated_text = format!("{}🔄{}", test_string, worker_id);
                    conn.execute(
                        "UPDATE unicode_test SET unicode_text = ?, text_length = ? WHERE id = ?",
                        (updated_text, updated_text.chars().count() as i64, record_id)
                    ).await.unwrap();
                    
                    sleep(Duration::from_millis(30)).await;
                }
                
                worker_id
            });
            
            handles.push(handle);
        }

        // Network chaos during Unicode operations
        let chaos_handle = tokio::spawn(async move {
            barrier.wait().await;
            
            for cycle in 0..8 {
                sleep(Duration::from_secs(6)).await;
                
                // Various disruption patterns
                match cycle % 4 {
                    0 => {
                        turmoil::hold("primary");
                        sleep(Duration::from_millis(300)).await;
                        turmoil::release("primary");
                    }
                    1 => {
                        for _ in 0..5 {
                            turmoil::hold("primary");
                            sleep(Duration::from_millis(100)).await;
                            turmoil::release("primary");
                            sleep(Duration::from_millis(50)).await;
                        }
                    }
                    2 => {
                        turmoil::hold("primary");
                        sleep(Duration::from_secs(1)).await;
                        turmoil::release("primary");
                    }
                    3 => {
                        for _ in 0..10 {
                            turmoil::hold("primary");
                            sleep(Duration::from_millis(50)).await;
                            turmoil::release("primary");
                            sleep(Duration::from_millis(25)).await;
                        }
                    }
                    _ => {}
                }
            }
        });

        for handle in handles {
            handle.await.unwrap();
        }
        
        chaos_handle.await.unwrap();

        // Final Unicode integrity verification
        let mut verify_stmt = conn.prepare("SELECT id, unicode_text, LENGTH(unicode_text), text_length FROM unicode_test ORDER BY id").await?;
        let mut verify_rows = verify_stmt.query([]).await?;
        
        let mut record_count = 0;
        while let Some(row) = verify_rows.next().await? {
            let id: i64 = row.get(0)?;
            let text: String = row.get(1)?;
            let byte_length: i64 = row.get(2)?;
            let char_length: i64 = row.get(3)?;
            
            record_count += 1;
            
            // Verify Unicode consistency
            let actual_char_count = text.chars().count() as i64;
            if actual_char_count != char_length {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("FINAL UNICODE CHECK FAILED: Record {} character count mismatch: stored {}, actual {}", 
                    id, char_length, actual_char_count);
            }
            
            // Check for invalid UTF-8 sequences
            if !text.is_ascii() && text.chars().any(|c| c == '\u{FFFD}') {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("FINAL UNICODE CHECK FAILED: Record {} contains replacement characters", id);
            }
        }

        if corruption_detected.load(Ordering::SeqCst) {
            panic!("UNICODE CORRUPTION DETECTED!");
        }

        eprintln!("Unicode test completed successfully with {} records", record_count);

        Ok(())
    });

    sim.run().unwrap();
}

/// Test for NULL value handling corruption
/// This test targets NULL/NOT NULL constraint edge cases
#[test]
fn null_handling_corruption_test() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(180))
        .build();

    let tmp = tempdir().unwrap();
    let db_path = tmp.path().to_owned();

    init_tracing();
    sim.host("primary", move || {
        let path = db_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 7,
                    max_log_duration: Some(0.4),
                    ..Default::default()
                },
                user_api_config: crate::config::UserApiConfig::default(),
                admin_api_config: Some(crate::config::AdminApiConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9090)).await?,
                    connector: TurmoilConnector,
                    disable_metrics: false,
                    auth_key: None,
                }),
                rpc_server_config: Some(crate::config::RpcServerConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 4567)).await?,
                    tls_config: None,
                }),
                disable_namespaces: false,
                disable_default_namespace: true,
                ..Default::default()
            };

            server.start_sim(8080).await?;
            Ok(())
        }
    });

    sim.client("null_tester", async move {
        let client = Client::new();
        
        client
            .post("http://primary:9090/v1/namespaces/testdb/create", json!({}))
            .await?;

        let db = Database::open_remote_with_connector(
            "http://testdb.primary:8080", 
            "", 
            TurmoilConnector
        )?;
        let conn = db.connect()?;

        // Create table with mixed NULL/NOT NULL constraints
        conn.execute(
            "CREATE TABLE null_test (
                id INTEGER PRIMARY KEY,
                required_field TEXT NOT NULL,
                optional_field TEXT,
                nullable_int INTEGER,
                non_null_int INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active'
            )", 
            ()
        ).await?;

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(4));

        let mut handles = vec![];

        // Test NULL handling with concurrent operations
        for worker_id in 0..3 {
            let barrier = barrier.clone();
            let corruption_detected = corruption_detected.clone();
            
            let handle = tokio::spawn(async move {
                let db = Database::open_remote_with_connector(
                    "http://testdb.primary:8080", 
                    "", 
                    TurmoilConnector
                ).unwrap();
                let conn = db.connect().unwrap();

                barrier.wait().await;

                for iteration in 0..40 {
                    let record_id = worker_id * 1000 + iteration;
                    let required_field = format!("required_{}_{}", worker_id, iteration);
                    
                    // Test various NULL scenarios
                    match iteration % 4 {
                        0 => {
                            // Insert with all fields
                            conn.execute(
                                "INSERT INTO null_test (id, required_field, optional_field, nullable_int, non_null_int) VALUES (?, ?, ?, ?, ?)",
                                (record_id, &required_field, Some(format!("optional_{}", iteration)), Some(iteration), iteration + 100)
                            ).await.unwrap();
                        }
                        1 => {
                            // Insert with NULLs in nullable fields
                            conn.execute(
                                "INSERT INTO null_test (id, required_field, optional_field, nullable_int, non_null_int) VALUES (?, ?, ?, ?, ?)",
                                (record_id, &required_field, None::<String>, None::<i64>, iteration + 200)
                            ).await.unwrap();
                        }
                        2 => {
                            // Insert with defaults
                            conn.execute(
                                "INSERT INTO null_test (id, required_field) VALUES (?, ?)",
                                (record_id, &required_field)
                            ).await.unwrap();
                        }
                        3 => {
                            // Insert then update to NULL
                            conn.execute(
                                "INSERT INTO null_test (id, required_field, optional_field, nullable_int, non_null_int) VALUES (?, ?, ?, ?, ?)",
                                (record_id, &required_field, Some("temp".to_string()), Some(999), iteration + 300)
                            ).await.unwrap();
                            
                            // Update nullable fields to NULL
                            conn.execute(
                                "UPDATE null_test SET optional_field = NULL, nullable_int = NULL WHERE id = ?",
                                (record_id,)
                            ).await.unwrap();
                        }
                        _ => {}
                    }
                    
                    // Verify the record
                    let mut stmt = conn.prepare("SELECT required_field, optional_field, nullable_int, non_null_int, status FROM null_test WHERE id = ?").await.unwrap();
                    let mut rows = stmt.query([record_id]).await.unwrap();
                    
                    if let Some(row) = rows.next().await.unwrap() {
                        let req_field: String = row.get(0).unwrap();
                        let opt_field: Option<String> = row.get(1).unwrap();
                        let nullable_int: Option<i64> = row.get(2).unwrap();
                        let non_null_int: i64 = row.get(3).unwrap();
                        let status: String = row.get(4).unwrap();
                        
                        // Verify required field is never NULL
                        if req_field != required_field {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("NULL CORRUPTION: Required field mismatch for worker {} iteration {}", worker_id, iteration);
                        }
                        
                        // Verify non-null integer is never NULL (this should never be NULL)
                        // The fact that we can retrieve it as i64 means it's not NULL, which is correct
                        
                        // Verify status has default value when not explicitly set
                        if status.is_empty() {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("NULL CORRUPTION: Status field is empty for worker {} iteration {}", worker_id, iteration);
                        }
                    }
                    
                    // Try to violate NOT NULL constraints (these should fail)
                    let violation_result = conn.execute(
                        "UPDATE null_test SET required_field = NULL WHERE id = ?",
                        (record_id,)
                    ).await;
                    
                    // This should fail - if it succeeds, we have a constraint violation bug
                    if violation_result.is_ok() {
                        corruption_detected.store(true, Ordering::SeqCst);
                        eprintln!("NULL CONSTRAINT VIOLATION: Successfully set required_field to NULL for record {}", record_id);
                    }
                    
                    sleep(Duration::from_millis(25)).await;
                }
                
                worker_id
            });
            
            handles.push(handle);
        }

        // Network disruption during NULL operations
        let disruption_handle = tokio::spawn(async move {
            barrier.wait().await;
            
            for _ in 0..6 {
                sleep(Duration::from_secs(5)).await;
                turmoil::hold("primary");
                sleep(Duration::from_millis(250)).await;
                turmoil::release("primary");
            }
        });

        for handle in handles {
            handle.await.unwrap();
        }
        
        disruption_handle.await.unwrap();

        // Final NULL constraint verification
        let mut verify_stmt = conn.prepare("SELECT id, required_field, optional_field, nullable_int, non_null_int, status FROM null_test ORDER BY id").await?;
        let mut verify_rows = verify_stmt.query([]).await?;
        
        let mut record_count = 0;
        while let Some(row) = verify_rows.next().await? {
            let id: i64 = row.get(0)?;
            let required_field: String = row.get(1)?;
            let optional_field: Option<String> = row.get(2)?;
            let nullable_int: Option<i64> = row.get(3)?;
            let non_null_int: i64 = row.get(4)?;
            let status: String = row.get(5)?;
            
            record_count += 1;
            
            // Verify NOT NULL constraints are maintained
            if required_field.is_empty() {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("FINAL NULL CHECK FAILED: Record {} has empty required_field", id);
            }
            
            if status.is_empty() {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("FINAL NULL CHECK FAILED: Record {} has empty status", id);
            }
            
            // non_null_int should never be NULL (if we can read it as i64, it's not NULL)
            // This is implicitly verified by the successful row.get(4) call above
        }

        if corruption_detected.load(Ordering::SeqCst) {
            panic!("NULL HANDLING CORRUPTION DETECTED!");
        }

        eprintln!("NULL handling test completed successfully with {} records", record_count);

        Ok(())
    });

    sim.run().unwrap();
}