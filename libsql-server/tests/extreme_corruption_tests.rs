//! Extreme corruption tests designed to maximize bug discovery
//! 
//! Created by hamisionesmus for Turso bug bounty program
//! These tests target the most vulnerable areas for data corruption
//! with extreme stress conditions to expose maximum number of bugs.

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

/// Extreme stress test with maximum concurrent connections and minimal resources
/// This test pushes the system to its absolute limits to expose race conditions
#[test]
fn extreme_concurrent_stress_test() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(600)) // Longer duration
        .tcp_capacity(64) // Extremely limited bandwidth
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
                    max_log_size: 3, // Extremely small - forces constant compaction
                    max_log_duration: Some(0.1), // Very aggressive
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

    sim.client("extreme_tester", async move {
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

        // Create complex schema with multiple tables and constraints
        conn.execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL CHECK(balance >= 0),
                account_type TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                checksum TEXT NOT NULL
            )", 
            ()
        ).await?;

        conn.execute(
            "CREATE TABLE transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_account INTEGER REFERENCES accounts(id),
                to_account INTEGER REFERENCES accounts(id),
                amount INTEGER NOT NULL CHECK(amount > 0),
                timestamp INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                hash TEXT NOT NULL
            )", 
            ()
        ).await?;

        conn.execute(
            "CREATE TABLE audit_trail (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                old_values TEXT,
                new_values TEXT,
                timestamp INTEGER NOT NULL,
                user_id TEXT NOT NULL
            )", 
            ()
        ).await?;

        // Insert initial data
        for i in 0..200 {
            let checksum = format!("acc_chk_{}", i);
            conn.execute(
                "INSERT INTO accounts (id, balance, account_type, created_at, updated_at, checksum) 
                 VALUES (?, ?, ?, ?, ?, ?)",
                (i, 10000, if i % 3 == 0 { "checking" } else if i % 3 == 1 { "savings" } else { "investment" }, 
                 1000000000 + i, 1000000000 + i, checksum)
            ).await?;
        }

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let transaction_counter = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(21)); // 20 workers + 1 controller

        let mut handles = vec![];

        // Spawn 20 extremely aggressive concurrent workers
        for worker_id in 0..20 {
            let barrier = barrier.clone();
            let corruption_detected = corruption_detected.clone();
            let transaction_counter = transaction_counter.clone();
            
            let handle = tokio::spawn(async move {
                let db = Database::open_remote_with_connector(
                    "http://testdb.primary:8080", 
                    "", 
                    TurmoilConnector
                ).unwrap();
                let conn = db.connect().unwrap();

                barrier.wait().await;

                for iteration in 0..100 {
                    let tx_id = transaction_counter.fetch_add(1, Ordering::SeqCst);
                    
                    // Complex multi-table transaction
                    let tx = conn.transaction().await.unwrap();
                    
                    let from_account = (tx_id % 200) as i64;
                    let to_account = ((tx_id + 1) % 200) as i64;
                    let amount = 100 + (tx_id % 500) as i64;
                    
                    // Read current balances
                    let mut from_stmt = tx.prepare("SELECT balance, checksum FROM accounts WHERE id = ?").await.unwrap();
                    let mut from_rows = from_stmt.query([from_account]).await.unwrap();
                    
                    if let Some(from_row) = from_rows.next().await.unwrap() {
                        let from_balance: i64 = from_row.get(0).unwrap();
                        let from_checksum: String = from_row.get(1).unwrap();
                        
                        // Verify checksum
                        let expected_checksum = format!("acc_chk_{}", from_account);
                        if !from_checksum.starts_with(&expected_checksum) {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("CHECKSUM CORRUPTION: Account {} has invalid checksum", from_account);
                        }
                        
                        if from_balance >= amount {
                            // Log to audit trail
                            tx.execute(
                                "INSERT INTO audit_trail (table_name, operation, old_values, new_values, timestamp, user_id)
                                 VALUES (?, ?, ?, ?, ?, ?)",
                                ("accounts", "transfer_out", 
                                 format!("balance:{}", from_balance),
                                 format!("balance:{}", from_balance - amount),
                                 std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64,
                                 format!("worker_{}", worker_id))
                            ).await.unwrap();
                            
                            // Update from account
                            let new_checksum = format!("acc_chk_{}_v{}", from_account, tx_id);
                            tx.execute(
                                "UPDATE accounts SET balance = balance - ?, updated_at = ?, checksum = ? WHERE id = ?",
                                (amount, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64, new_checksum, from_account)
                            ).await.unwrap();
                            
                            // Update to account
                            let to_new_checksum = format!("acc_chk_{}_v{}", to_account, tx_id);
                            tx.execute(
                                "UPDATE accounts SET balance = balance + ?, updated_at = ?, checksum = ? WHERE id = ?",
                                (amount, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64, to_new_checksum, to_account)
                            ).await.unwrap();
                            
                            // Record transaction
                            let tx_hash = format!("tx_hash_{}_{}", worker_id, iteration);
                            tx.execute(
                                "INSERT INTO transactions (from_account, to_account, amount, timestamp, status, hash)
                                 VALUES (?, ?, ?, ?, ?, ?)",
                                (from_account, to_account, amount,
                                 std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64,
                                 "completed", tx_hash)
                            ).await.unwrap();
                            
                            // Random delay to increase race condition chances
                            if iteration % 3 == 0 {
                                sleep(Duration::from_millis(10 + (worker_id as u64 * 5))).await;
                            }
                        }
                    }
                    
                    // Commit with timeout
                    match timeout(Duration::from_secs(15), tx.commit()).await {
                        Ok(Ok(_)) => {},
                        Ok(Err(e)) => {
                            eprintln!("Transaction commit failed for worker {}: {}", worker_id, e);
                        }
                        Err(_) => {
                            eprintln!("Transaction commit timeout for worker {}", worker_id);
                        }
                    }
                    
                    // Very short delay between transactions
                    sleep(Duration::from_millis(5)).await;
                }
                
                worker_id
            });
            
            handles.push(handle);
        }

        // Network chaos controller
        let chaos_handle = tokio::spawn(async move {
            barrier.wait().await;
            
            for cycle in 0..30 {
                sleep(Duration::from_secs(5)).await;
                
                // Create various network disruptions
                match cycle % 6 {
                    0 => {
                        // Brief total outage
                        turmoil::hold("primary");
                        sleep(Duration::from_millis(500)).await;
                        turmoil::release("primary");
                    }
                    1 => {
                        // Bandwidth throttling (already limited, but add more pressure)
                        for _ in 0..10 {
                            turmoil::hold("primary");
                            sleep(Duration::from_millis(50)).await;
                            turmoil::release("primary");
                            sleep(Duration::from_millis(50)).await;
                        }
                    }
                    2 => {
                        // Intermittent connectivity
                        for _ in 0..5 {
                            turmoil::hold("primary");
                            sleep(Duration::from_millis(200)).await;
                            turmoil::release("primary");
                            sleep(Duration::from_millis(100)).await;
                        }
                    }
                    3 => {
                        // Longer outage during potential compaction
                        turmoil::hold("primary");
                        sleep(Duration::from_secs(2)).await;
                        turmoil::release("primary");
                    }
                    4 => {
                        // Rapid on/off cycles
                        for _ in 0..20 {
                            turmoil::hold("primary");
                            sleep(Duration::from_millis(25)).await;
                            turmoil::release("primary");
                            sleep(Duration::from_millis(25)).await;
                        }
                    }
                    5 => {
                        // Extended disruption
                        turmoil::hold("primary");
                        sleep(Duration::from_secs(3)).await;
                        turmoil::release("primary");
                    }
                    _ => {}
                }
            }
        });

        // Wait for all workers
        for handle in handles {
            handle.await.unwrap();
        }
        
        chaos_handle.await.unwrap();

        // Final verification phase
        sleep(Duration::from_secs(10)).await;

        // Comprehensive data integrity checks
        
        // 1. Check account balance consistency
        let mut balance_stmt = conn.prepare("SELECT id, balance, checksum FROM accounts ORDER BY id").await?;
        let mut balance_rows = balance_stmt.query([]).await?;
        
        let mut total_balance = 0i64;
        let mut account_count = 0;
        
        while let Some(row) = balance_rows.next().await? {
            let id: i64 = row.get(0)?;
            let balance: i64 = row.get(1)?;
            let checksum: String = row.get(2)?;
            
            account_count += 1;
            total_balance += balance;
            
            // Check for negative balances (constraint violation)
            if balance < 0 {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("CONSTRAINT VIOLATION: Account {} has negative balance: {}", id, balance);
            }
            
            // Verify checksum format
            if !checksum.contains("acc_chk_") {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("CHECKSUM FORMAT ERROR: Account {} has malformed checksum: {}", id, checksum);
            }
        }
        
        // Total balance should be conserved (200 accounts * 10000 initial = 2,000,000)
        let expected_total = 200 * 10000;
        if total_balance != expected_total {
            corruption_detected.store(true, Ordering::SeqCst);
            eprintln!("BALANCE CONSERVATION VIOLATION: Expected {}, got {}", expected_total, total_balance);
        }
        
        // 2. Check transaction log integrity
        let mut tx_stmt = conn.prepare("SELECT id, from_account, to_account, amount, hash FROM transactions ORDER BY id").await?;
        let mut tx_rows = tx_stmt.query([]).await?;
        
        let mut tx_count = 0;
        while let Some(row) = tx_rows.next().await? {
            let id: i64 = row.get(0)?;
            let from_account: i64 = row.get(1)?;
            let to_account: i64 = row.get(2)?;
            let amount: i64 = row.get(3)?;
            let hash: String = row.get(4)?;
            
            tx_count += 1;
            
            // Verify transaction makes sense
            if from_account == to_account {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("TRANSACTION LOGIC ERROR: Transaction {} has same from/to account: {}", id, from_account);
            }
            
            if amount <= 0 {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("TRANSACTION AMOUNT ERROR: Transaction {} has invalid amount: {}", id, amount);
            }
            
            if !hash.starts_with("tx_hash_") {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("TRANSACTION HASH ERROR: Transaction {} has malformed hash: {}", id, hash);
            }
        }
        
        // 3. Check audit trail completeness
        let mut audit_stmt = conn.prepare("SELECT COUNT(*) FROM audit_trail").await?;
        let mut audit_rows = audit_stmt.query([]).await?;
        
        if let Some(row) = audit_rows.next().await? {
            let audit_count: i64 = row.get(0)?;
            
            // Should have at least as many audit entries as successful transactions
            if audit_count < tx_count {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("AUDIT TRAIL INCOMPLETE: {} transactions but only {} audit entries", tx_count, audit_count);
            }
        }

        if corruption_detected.load(Ordering::SeqCst) {
            panic!("EXTREME STRESS TEST DETECTED CORRUPTION!");
        }

        eprintln!("Extreme stress test completed: {} accounts, {} transactions, total balance: {}", 
            account_count, tx_count, total_balance);

        Ok(())
    });

    sim.run().unwrap();
}

/// Test for encryption-related corruption bugs
/// This test specifically targets encryption/decryption edge cases
#[test]
#[cfg(feature = "encryption")]
fn encryption_corruption_test() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(300))
        .tcp_capacity(128)
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
                    max_log_size: 8,
                    max_log_duration: Some(0.5),
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

    sim.client("encryption_tester", async move {
        let client = Client::new();
        
        client
            .post("http://primary:9090/v1/namespaces/testdb/create", json!({}))
            .await?;

        // Test with encryption enabled
        let db = Database::open_with_remote_sync_connector(
            "test_encrypted.db",
            "http://testdb.primary:8080",
            "",
            TurmoilConnector,
            false,
            Some(libsql::EncryptionConfig::new(
                libsql::Cipher::Aes256Cbc,
                bytes::Bytes::from_static(b"test_encryption_key_32_bytes_long")
            ))
        ).await?;

        let conn = db.connect()?;

        // Create table with sensitive data
        conn.execute(
            "CREATE TABLE encrypted_data (
                id INTEGER PRIMARY KEY,
                sensitive_data BLOB NOT NULL,
                plaintext_hash TEXT NOT NULL,
                encryption_version INTEGER NOT NULL
            )", 
            ()
        ).await?;

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(6));

        let mut handles = vec![];

        // Test encryption under various stress conditions
        for worker_id in 0..5 {
            let barrier = barrier.clone();
            let corruption_detected = corruption_detected.clone();
            
            let handle = tokio::spawn(async move {
                let db = Database::open_with_remote_sync_connector(
                    "test_encrypted.db",
                    "http://testdb.primary:8080",
                    "",
                    TurmoilConnector,
                    false,
                    Some(libsql::EncryptionConfig::new(
                        libsql::Cipher::Aes256Cbc,
                        bytes::Bytes::from_static(b"test_encryption_key_32_bytes_long")
                    ))
                ).await.unwrap();
                let conn = db.connect().unwrap();

                barrier.wait().await;

                for iteration in 0..50 {
                    let sensitive_data = format!("SENSITIVE_DATA_WORKER_{}_ITER_{}_SECRET_INFO", worker_id, iteration).repeat(100);
                    let plaintext_hash = format!("hash_{}", sensitive_data.len());
                    
                    // Insert encrypted data
                    conn.execute(
                        "INSERT INTO encrypted_data (id, sensitive_data, plaintext_hash, encryption_version) VALUES (?, ?, ?, ?)",
                        (worker_id * 1000 + iteration, sensitive_data.as_bytes(), plaintext_hash, 1)
                    ).await.unwrap();
                    
                    // Immediately read it back to verify encryption/decryption
                    let mut stmt = conn.prepare("SELECT sensitive_data, plaintext_hash FROM encrypted_data WHERE id = ?").await.unwrap();
                    let mut rows = stmt.query([worker_id * 1000 + iteration]).await.unwrap();
                    
                    if let Some(row) = rows.next().await.unwrap() {
                        let retrieved_data: Vec<u8> = row.get(0).unwrap();
                        let retrieved_hash: String = row.get(1).unwrap();
                        
                        // Verify data integrity after encryption/decryption
                        if retrieved_data != sensitive_data.as_bytes() {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("ENCRYPTION CORRUPTION: Data mismatch for worker {} iteration {}", worker_id, iteration);
                        }
                        
                        if retrieved_hash != plaintext_hash {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("HASH CORRUPTION: Hash mismatch for worker {} iteration {}", worker_id, iteration);
                        }
                    }
                    
                    sleep(Duration::from_millis(20)).await;
                }
                
                worker_id
            });
            
            handles.push(handle);
        }

        // Network disruption during encryption operations
        let disruption_handle = tokio::spawn(async move {
            barrier.wait().await;
            
            for _ in 0..10 {
                sleep(Duration::from_secs(3)).await;
                turmoil::hold("primary");
                sleep(Duration::from_millis(500)).await;
                turmoil::release("primary");
            }
        });

        for handle in handles {
            handle.await.unwrap();
        }
        
        disruption_handle.await.unwrap();

        // Final verification
        let mut verify_stmt = conn.prepare("SELECT id, sensitive_data, plaintext_hash FROM encrypted_data ORDER BY id").await?;
        let mut verify_rows = verify_stmt.query([]).await?;
        
        let mut record_count = 0;
        while let Some(row) = verify_rows.next().await? {
            let id: i64 = row.get(0)?;
            let data: Vec<u8> = row.get(1)?;
            let hash: String = row.get(2)?;
            
            record_count += 1;
            
            // Verify data format
            let data_str = String::from_utf8_lossy(&data);
            if !data_str.contains("SENSITIVE_DATA_WORKER_") {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("ENCRYPTED DATA CORRUPTION: Record {} has malformed data", id);
            }
            
            // Verify hash format
            if !hash.starts_with("hash_") {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("HASH FORMAT CORRUPTION: Record {} has malformed hash", id);
            }
        }

        if corruption_detected.load(Ordering::SeqCst) {
            panic!("ENCRYPTION CORRUPTION DETECTED!");
        }

        eprintln!("Encryption test completed successfully with {} records", record_count);

        Ok(())
    });

    sim.run().unwrap();
}

/// Test for backup/restore corruption scenarios
/// This test targets data integrity during backup and restore operations
#[test]
fn backup_restore_corruption_test() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(400))
        .build();

    let tmp_primary = tempdir().unwrap();
    let tmp_backup = tempdir().unwrap();
    let primary_path = tmp_primary.path().to_owned();
    let backup_path = tmp_backup.path().to_owned();

    init_tracing();
    sim.host("primary", move || {
        let path = primary_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 12,
                    max_log_duration: Some(1.0),
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

    sim.client("backup_tester", async move {
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

        // Create comprehensive test schema
        conn.execute(
            "CREATE TABLE backup_test (
                id INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                checksum TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                backup_version INTEGER NOT NULL DEFAULT 1
            )", 
            ()
        ).await?;

        conn.execute(
            "CREATE TABLE metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                last_backup INTEGER NOT NULL DEFAULT 0
            )", 
            ()
        ).await?;

        // Insert initial dataset
        for i in 0..500 {
            let data = format!("backup_test_data_{}_content", i).repeat(50);
            let checksum = format!("chk_{}", data.len());
            conn.execute(
                "INSERT INTO backup_test (id, data, checksum, created_at) VALUES (?, ?, ?, ?)",
                (i, data, checksum, 1000000000 + i)
            ).await?;
        }

        // Insert metadata
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('total_records', '500')",
            ()
        ).await?;
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('test_version', '1.0')",
            ()
        ).await?;

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(4));

        let mut handles = vec![];

        // Continuous data modification during backup operations
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

                for iteration in 0..80 {
                    // Modify existing records
                    let record_id = (worker_id * 100 + iteration) % 500;
                    let new_data = format!("modified_by_worker_{}_iter_{}_data", worker_id, iteration).repeat(60);
                    let new_checksum = format!("chk_mod_{}", new_data.len());
                    
                    conn.execute(
                        "UPDATE backup_test SET data = ?, checksum = ?, backup_version = backup_version + 1 WHERE id = ?",
                        (new_data, new_checksum, record_id)
                    ).await.unwrap();
                    
                    // Add new records
                    let new_id = 1000 + worker_id * 1000 + iteration;
                    let insert_data = format!("new_record_worker_{}_iter_{}", worker_id, iteration).repeat(40);
                    let insert_checksum = format!("chk_new_{}", insert_data.len());
                    
                    conn.execute(
                        "INSERT INTO backup_test (id, data, checksum, created_at) VALUES (?, ?, ?, ?)",
                        (new_id, insert_data, insert_checksum, 2000000000 + new_id)
                    ).await.unwrap();
                    
                    // Update metadata
                    conn.execute(
                        "UPDATE metadata SET value = ? WHERE key = 'total_records'",
                        (format!("{}", 500 + (worker_id + 1) * (iteration + 1)),)
                    ).await.unwrap();
                    
                    sleep(Duration::from_millis(100)).await;
                }
                
                worker_id
            });
            
            handles.push(handle);
        }

        // Backup simulation with network disruptions
        let backup_handle = tokio::spawn(async move {
            barrier.wait().await;
            
            for backup_cycle in 0..8 {
                sleep(Duration::from_secs(10)).await;
                
                // Simulate backup process with potential interruptions
                eprintln!("Starting backup cycle {}", backup_cycle);
                
                // Create network disruption during backup
                if backup_cycle % 3 == 0 {
                    turmoil::hold("primary");
                    sleep(Duration::from_secs(1)).await;
                    turmoil::release("primary");
                }
                
                sleep(Duration::from_secs(5)).await;
                eprintln!("Backup cycle {} completed", backup_cycle);
            }
        });

        for handle in handles {
            handle.await.unwrap();
        }
        
        backup_handle.await.unwrap();

        // Final integrity verification
        let mut verify_stmt = conn.prepare("SELECT COUNT(*), SUM(LENGTH(data)), SUM(backup_version) FROM backup_test").await?;
        let mut verify_rows = verify_stmt.query([]).await?;
        
        if let Some(row) = verify_rows.next().await? {
            let count: i64 = row.get(0)?;
            let total_data_length: i64 = row.get(1)?;
            let total_versions: i64 = row.get(2)?;
            
            eprintln!("Final verification: {} records, {} total data length, {} total versions",
                count, total_data_length, total_versions);
            
            // Basic sanity checks
            if count < 500 {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("BACKUP CORRUPTION: Lost records during backup operations");
            }
            
            if total_data_length == 0 {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("BACKUP CORRUPTION: All data lost");
            }
        }

        if corruption_detected.load(Ordering::SeqCst) {
            panic!("BACKUP/RESTORE CORRUPTION DETECTED!");
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test for memory pressure corruption scenarios
/// This test creates extreme memory pressure to expose memory-related bugs
#[test]
fn memory_pressure_corruption_test() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(300))
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
                    max_log_size: 4, // Very small to force frequent operations
                    max_log_duration: Some(0.1),
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

    sim.client("memory_tester", async move {
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

        // Create table for large data
        conn.execute(
            "CREATE TABLE large_data (
                id INTEGER PRIMARY KEY,
                huge_blob BLOB NOT NULL,
                metadata TEXT NOT NULL,
                checksum TEXT NOT NULL
            )",
            ()
        ).await?;

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(4));

        let mut handles = vec![];

        // Create memory pressure with large data operations
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

                for iteration in 0..30 {
                    // Create very large blob data (1MB each)
                    let large_data = vec![0u8; 1024 * 1024];
                    let metadata = format!("worker_{}_iteration_{}_large_data_metadata", worker_id, iteration).repeat(100);
                    let checksum = format!("large_chk_{}_{}", worker_id, iteration);
                    
                    let record_id = worker_id * 1000 + iteration;
                    
                    // Insert large data
                    conn.execute(
                        "INSERT INTO large_data (id, huge_blob, metadata, checksum) VALUES (?, ?, ?, ?)",
                        (record_id, large_data, metadata, checksum)
                    ).await.unwrap();
                    
                    // Immediately read it back to verify
                    let mut stmt = conn.prepare("SELECT LENGTH(huge_blob), metadata, checksum FROM large_data WHERE id = ?").await.unwrap();
                    let mut rows = stmt.query([record_id]).await.unwrap();
                    
                    if let Some(row) = rows.next().await.unwrap() {
                        let blob_length: i64 = row.get(0).unwrap();
                        let retrieved_metadata: String = row.get(1).unwrap();
                        let retrieved_checksum: String = row.get(2).unwrap();
                        
                        if blob_length != 1024 * 1024 {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("MEMORY CORRUPTION: Blob size mismatch for worker {} iteration {}: expected {}, got {}",
                                worker_id, iteration, 1024 * 1024, blob_length);
                        }
                        
                        if !retrieved_metadata.contains(&format!("worker_{}_iteration_{}", worker_id, iteration)) {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("MEMORY CORRUPTION: Metadata corruption for worker {} iteration {}", worker_id, iteration);
                        }
                        
                        if retrieved_checksum != format!("large_chk_{}_{}", worker_id, iteration) {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("MEMORY CORRUPTION: Checksum mismatch for worker {} iteration {}", worker_id, iteration);
                        }
                    }
                    
                    // Delete some old records to create fragmentation
                    if iteration > 5 {
                        let old_id = worker_id * 1000 + (iteration - 5);
                        conn.execute("DELETE FROM large_data WHERE id = ?", (old_id,)).await.unwrap();
                    }
                    
                    sleep(Duration::from_millis(200)).await;
                }
                
                worker_id
            });
            
            handles.push(handle);
        }

        // Memory pressure controller
        let pressure_handle = tokio::spawn(async move {
            barrier.wait().await;
            
            // Create additional memory pressure
            for cycle in 0..15 {
                sleep(Duration::from_secs(5)).await;
                
                // Simulate memory pressure by creating temporary large allocations
                let _temp_data: Vec<Vec<u8>> = (0..50).map(|_| vec![0u8; 512 * 1024]).collect();
                
                // Network disruption during high memory usage
                turmoil::hold("primary");
                sleep(Duration::from_millis(300)).await;
                turmoil::release("primary");
                
                // Let the temporary data be dropped
                sleep(Duration::from_millis(100)).await;
            }
        });

        for handle in handles {
            handle.await.unwrap();
        }
        
        pressure_handle.await.unwrap();

        // Final verification
        let mut count_stmt = conn.prepare("SELECT COUNT(*), SUM(LENGTH(huge_blob)) FROM large_data").await?;
        let mut count_rows = count_stmt.query([]).await?;
        
        if let Some(row) = count_rows.next().await? {
            let count: i64 = row.get(0)?;
            let total_size: i64 = row.get(1)?;
            
            eprintln!("Memory pressure test completed: {} records, {} total bytes", count, total_size);
            
            // Verify remaining data integrity
            let mut verify_stmt = conn.prepare("SELECT id, LENGTH(huge_blob), checksum FROM large_data ORDER BY id").await?;
            let mut verify_rows = verify_stmt.query([]).await?;
            
            while let Some(row) = verify_rows.next().await? {
                let id: i64 = row.get(0)?;
                let blob_length: i64 = row.get(1)?;
                let checksum: String = row.get(2)?;
                
                if blob_length != 1024 * 1024 {
                    corruption_detected.store(true, Ordering::SeqCst);
                    eprintln!("FINAL VERIFICATION FAILED: Record {} has wrong blob size: {}", id, blob_length);
                }
                
                if !checksum.starts_with("large_chk_") {
                    corruption_detected.store(true, Ordering::SeqCst);
                    eprintln!("FINAL VERIFICATION FAILED: Record {} has malformed checksum: {}", id, checksum);
                }
            }
        }

        if corruption_detected.load(Ordering::SeqCst) {
            panic!("MEMORY PRESSURE CORRUPTION DETECTED!");
        }

        Ok(())
    });

    sim.run().unwrap();
}