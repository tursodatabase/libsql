//! Advanced data corruption simulation tests
//! 
//! This module contains sophisticated simulation tests designed to expose
//! data corruption bugs that might survive the current deterministic testing.
//! 
//! The tests focus on:
//! 1. Concurrent transaction handling with network failures
//! 2. Replication consistency under various failure scenarios
//! 3. WAL corruption and recovery scenarios
//! 4. Schema migration data integrity
//! 5. Snapshot compaction race conditions

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

/// Test concurrent transactions with network partitions during commit phase
/// This test aims to expose race conditions in the commit protocol that could
/// lead to data corruption when network failures occur at critical moments.
#[test]
fn concurrent_transactions_with_network_partition_during_commit() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(300))
        .tcp_capacity(1024) // Limited bandwidth to increase chance of partial writes
        .build();

    let tmp_primary = tempdir().unwrap();
    let tmp_replica = tempdir().unwrap();
    let primary_path = tmp_primary.path().to_owned();
    let replica_path = tmp_replica.path().to_owned();

    // Setup primary with aggressive log rotation to trigger more compactions
    init_tracing();
    sim.host("primary", move || {
        let path = primary_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 10, // Very small to force frequent compactions
                    max_log_duration: Some(1.0), // Aggressive rotation
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

    // Setup replica
    sim.host("replica", move || {
        let path = replica_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 10,
                    max_log_duration: Some(1.0),
                    ..Default::default()
                },
                user_api_config: crate::config::UserApiConfig::default(),
                admin_api_config: Some(crate::config::AdminApiConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9091)).await?,
                    connector: TurmoilConnector,
                    disable_metrics: false,
                    auth_key: None,
                }),
                rpc_client_config: Some(crate::config::RpcClientConfig {
                    remote_url: "http://primary:4567".into(),
                    connector: TurmoilConnector,
                    tls_config: None,
                }),
                disable_namespaces: false,
                disable_default_namespace: true,
                ..Default::default()
            };

            server.start_sim(8081).await?;
            Ok(())
        }
    });

    sim.client("corruption_tester", async move {
        let client = Client::new();
        
        // Create namespace
        client
            .post("http://primary:9090/v1/namespaces/testdb/create", json!({}))
            .await?;

        // Wait for replica to sync
        sleep(Duration::from_secs(2)).await;

        // Setup test data structure
        let primary_db = Database::open_remote_with_connector(
            "http://testdb.primary:8080", 
            "", 
            TurmoilConnector
        )?;
        let primary_conn = primary_db.connect()?;

        // Create a table with constraints to detect corruption
        primary_conn.execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL CHECK(balance >= 0),
                version INTEGER NOT NULL DEFAULT 0,
                checksum TEXT NOT NULL
            )", 
            ()
        ).await?;

        // Insert initial data with checksums
        for i in 0..100 {
            let checksum = format!("chk_{}", i * 1000);
            primary_conn.execute(
                "INSERT INTO accounts (id, balance, checksum) VALUES (?, ?, ?)",
                (i, 1000, checksum)
            ).await?;
        }

        // Create multiple concurrent connections
        let barrier = Arc::new(Barrier::new(10));
        let corruption_detected = Arc::new(AtomicBool::new(false));
        let transaction_counter = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];

        // Spawn concurrent transaction workers
        for worker_id in 0..10 {
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

                // Perform many concurrent transactions
                for iteration in 0..50 {
                    let tx_id = transaction_counter.fetch_add(1, Ordering::SeqCst);
                    
                    // Simulate complex transaction with multiple operations
                    let tx = conn.transaction().await.unwrap();
                    
                    // Read current state
                    let mut stmt = tx.prepare("SELECT id, balance, version, checksum FROM accounts WHERE id = ?").await.unwrap();
                    let account_id = (tx_id % 100) as i64;
                    let mut rows = stmt.query([account_id]).await.unwrap();
                    
                    if let Some(row) = rows.next().await.unwrap() {
                        let current_balance: i64 = row.get(1).unwrap();
                        let current_version: i64 = row.get(2).unwrap();
                        let current_checksum: String = row.get(3).unwrap();
                        
                        // Verify checksum integrity
                        let expected_checksum = if current_version == 0 {
                            format!("chk_{}", account_id * 1000)
                        } else {
                            format!("chk_{}_{}", account_id, current_version)
                        };
                        
                        if current_checksum != expected_checksum {
                            corruption_detected.store(true, Ordering::SeqCst);
                            eprintln!("CORRUPTION DETECTED: Account {} has invalid checksum. Expected: {}, Got: {}", 
                                account_id, expected_checksum, current_checksum);
                        }
                        
                        // Perform transfer operation
                        let transfer_amount = 10;
                        let new_balance = current_balance - transfer_amount;
                        let new_version = current_version + 1;
                        let new_checksum = format!("chk_{}_{}", account_id, new_version);
                        
                        if new_balance >= 0 {
                            tx.execute(
                                "UPDATE accounts SET balance = ?, version = ?, checksum = ? WHERE id = ? AND version = ?",
                                (new_balance, new_version, new_checksum, account_id, current_version)
                            ).await.unwrap();
                            
                            // Add artificial delay to increase chance of network issues during commit
                            if iteration % 10 == 0 {
                                sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                    
                    // Commit with potential network failure
                    match timeout(Duration::from_secs(5), tx.commit()).await {
                        Ok(Ok(_)) => {
                            // Success
                        }
                        Ok(Err(e)) => {
                            eprintln!("Transaction commit failed for worker {}: {}", worker_id, e);
                        }
                        Err(_) => {
                            eprintln!("Transaction commit timeout for worker {}", worker_id);
                        }
                    }
                    
                    // Small delay between transactions
                    sleep(Duration::from_millis(10)).await;
                }
                
                worker_id
            });
            
            handles.push(handle);
        }

        // Introduce network partitions during execution
        let partition_handle = tokio::spawn(async move {
            for _ in 0..5 {
                sleep(Duration::from_secs(10)).await;
                
                // Simulate network partition
                turmoil::partition("primary", "replica");
                sleep(Duration::from_secs(2)).await;
                turmoil::repair("primary", "replica");
                
                sleep(Duration::from_secs(5)).await;
            }
        });

        // Wait for all workers to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        partition_handle.abort();

        // Final consistency check
        sleep(Duration::from_secs(5)).await;

        // Verify data integrity on primary
        let mut stmt = primary_conn.prepare("SELECT id, balance, version, checksum FROM accounts ORDER BY id").await?;
        let mut rows = stmt.query([]).await?;
        let mut accounts = HashMap::new();
        
        while let Some(row) = rows.next().await? {
            let id: i64 = row.get(0)?;
            let balance: i64 = row.get(1)?;
            let version: i64 = row.get(2)?;
            let checksum: String = row.get(3)?;
            
            accounts.insert(id, (balance, version, checksum));
        }

        // Verify replica consistency
        let replica_db = Database::open_remote_with_connector(
            "http://testdb.replica:8081", 
            "", 
            TurmoilConnector
        )?;
        let replica_conn = replica_db.connect()?;
        
        let mut replica_stmt = replica_conn.prepare("SELECT id, balance, version, checksum FROM accounts ORDER BY id").await?;
        let mut replica_rows = replica_stmt.query([]).await?;
        
        while let Some(row) = replica_rows.next().await? {
            let id: i64 = row.get(0)?;
            let balance: i64 = row.get(1)?;
            let version: i64 = row.get(2)?;
            let checksum: String = row.get(3)?;
            
            if let Some((primary_balance, primary_version, primary_checksum)) = accounts.get(&id) {
                if balance != *primary_balance || version != *primary_version || checksum != *primary_checksum {
                    corruption_detected.store(true, Ordering::SeqCst);
                    eprintln!("REPLICA INCONSISTENCY: Account {} - Primary: ({}, {}, {}), Replica: ({}, {}, {})",
                        id, primary_balance, primary_version, primary_checksum, balance, version, checksum);
                }
            }
        }

        // Check for any corruption
        if corruption_detected.load(Ordering::SeqCst) {
            panic!("Data corruption detected during concurrent transaction test!");
        }

        // Verify total balance conservation
        let total_balance: i64 = accounts.values().map(|(balance, _, _)| balance).sum();
        let expected_total = 100 * 1000; // 100 accounts * 1000 initial balance
        
        // Allow for some transactions to have occurred, but total should be reasonable
        if total_balance > expected_total || total_balance < expected_total - 50000 {
            panic!("Balance conservation violated! Expected around {}, got {}", expected_total, total_balance);
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test WAL corruption scenarios during log compaction
/// This test specifically targets the log compaction process to expose
/// potential data corruption during snapshot creation and log rotation.
#[test]
fn wal_corruption_during_compaction() {
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
                    max_log_size: 5, // Very aggressive compaction
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

    sim.client("compaction_tester", async move {
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

        // Create table with integrity constraints
        conn.execute(
            "CREATE TABLE integrity_test (
                id INTEGER PRIMARY KEY,
                data BLOB NOT NULL,
                hash TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )", 
            ()
        ).await?;

        // Insert data that will trigger multiple compactions
        let mut expected_hashes = HashMap::new();
        
        for batch in 0..20 {
            // Insert batch of data
            for i in 0..50 {
                let id = batch * 50 + i;
                let data = format!("test_data_{}_batch_{}", i, batch).repeat(100); // Large data
                let hash = format!("hash_{}", id);
                let created_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                
                conn.execute(
                    "INSERT INTO integrity_test (id, data, hash, created_at) VALUES (?, ?, ?, ?)",
                    (id, data.as_bytes(), hash.clone(), created_at)
                ).await?;
                
                expected_hashes.insert(id, hash);
            }
            
            // Force compaction by creating many small transactions
            for _ in 0..10 {
                conn.execute("BEGIN", ()).await?;
                conn.execute("UPDATE integrity_test SET created_at = created_at + 1 WHERE id = ?", (batch * 50,)).await?;
                conn.execute("COMMIT", ()).await?;
            }
            
            // Small delay to allow compaction
            sleep(Duration::from_millis(100)).await;
        }

        // Wait for compactions to complete
        sleep(Duration::from_secs(5)).await;

        // Verify data integrity after compactions
        let mut stmt = conn.prepare("SELECT id, data, hash FROM integrity_test ORDER BY id").await?;
        let mut rows = stmt.query([]).await?;
        
        let mut found_records = 0;
        while let Some(row) = rows.next().await? {
            let id: i64 = row.get(0)?;
            let data: Vec<u8> = row.get(1)?;
            let hash: String = row.get(2)?;
            
            found_records += 1;
            
            // Verify hash matches expected
            if let Some(expected_hash) = expected_hashes.get(&id) {
                if hash != *expected_hash {
                    panic!("Hash corruption detected for record {}: expected {}, got {}", id, expected_hash, hash);
                }
            } else {
                panic!("Unexpected record found: {}", id);
            }
            
            // Verify data integrity
            let expected_data = format!("test_data_{}_batch_{}", id % 50, id / 50).repeat(100);
            if data != expected_data.as_bytes() {
                panic!("Data corruption detected for record {}", id);
            }
        }
        
        if found_records != 1000 {
            panic!("Expected 1000 records, found {}", found_records);
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test schema migration data integrity
/// This test verifies that data remains consistent during schema migrations
/// even when network failures or other issues occur during the migration process.
#[test]
fn schema_migration_data_integrity() {
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
                db_config: crate::config::DbConfig::default(),
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

    sim.client("migration_tester", async move {
        let client = Client::new();
        
        // Create schema database
        client
            .post("http://primary:9090/v1/namespaces/schema/create", json!({"shared_schema": true}))
            .await?;
            
        // Create regular database using the schema
        client
            .post("http://primary:9090/v1/namespaces/testdb/create", json!({"shared_schema_name": "schema"}))
            .await?;

        let schema_db = Database::open_remote_with_connector(
            "http://schema.primary:8080", 
            "", 
            TurmoilConnector
        )?;
        let schema_conn = schema_db.connect()?;

        let test_db = Database::open_remote_with_connector(
            "http://testdb.primary:8080", 
            "", 
            TurmoilConnector
        )?;
        let test_conn = test_db.connect()?;

        // Create initial schema
        schema_conn.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            )", 
            ()
        ).await?;

        // Insert test data
        for i in 0..100 {
            test_conn.execute(
                "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
                (i, format!("User {}", i), format!("user{}@example.com", i))
            ).await?;
        }

        // Perform schema migration while data operations are ongoing
        let migration_handle = tokio::spawn(async move {
            sleep(Duration::from_secs(2)).await;
            
            // Add new column
            schema_conn.execute(
                "ALTER TABLE users ADD COLUMN created_at INTEGER DEFAULT 0", 
                ()
            ).await.unwrap();
            
            sleep(Duration::from_secs(2)).await;
            
            // Add index
            schema_conn.execute(
                "CREATE INDEX idx_users_email ON users(email)", 
                ()
            ).await.unwrap();
        });

        // Concurrent data operations during migration
        let data_ops_handle = tokio::spawn(async move {
            for i in 100..200 {
                match test_conn.execute(
                    "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
                    (i, format!("User {}", i), format!("user{}@example.com", i))
                ).await {
                    Ok(_) => {},
                    Err(e) => {
                        // Some operations might fail during migration, which is acceptable
                        eprintln!("Insert failed during migration: {}", e);
                    }
                }
                
                sleep(Duration::from_millis(50)).await;
            }
        });

        // Wait for operations to complete
        migration_handle.await.unwrap();
        data_ops_handle.await.unwrap();

        // Verify data integrity after migration
        let mut stmt = test_conn.prepare("SELECT id, name, email FROM users ORDER BY id").await?;
        let mut rows = stmt.query([]).await?;
        
        let mut count = 0;
        while let Some(row) = rows.next().await? {
            let id: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            let email: String = row.get(2)?;
            
            let expected_name = format!("User {}", id);
            let expected_email = format!("user{}@example.com", id);
            
            if name != expected_name || email != expected_email {
                panic!("Data corruption after migration: id={}, name={}, email={}", id, name, email);
            }
            
            count += 1;
        }
        
        // Should have at least the original 100 records
        if count < 100 {
            panic!("Data loss detected after migration: expected at least 100 records, found {}", count);
        }

        Ok(())
    });

    sim.run().unwrap();
}