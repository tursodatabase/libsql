//! Advanced corruption scenarios targeting specific edge cases
//! 
//! This module contains tests that target very specific edge cases and race conditions
//! that could lead to data corruption in distributed database systems.

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

/// Test for phantom reads and write skew anomalies under network partitions
/// This test specifically targets isolation level violations that could lead to
/// data corruption in distributed scenarios.
#[test]
fn phantom_reads_and_write_skew_under_partition() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(240))
        .tcp_capacity(512) // Limited bandwidth
        .build();

    let tmp_primary = tempdir().unwrap();
    let tmp_replica1 = tempdir().unwrap();
    let tmp_replica2 = tempdir().unwrap();
    
    let primary_path = tmp_primary.path().to_owned();
    let replica1_path = tmp_replica1.path().to_owned();
    let replica2_path = tmp_replica2.path().to_owned();

    // Setup primary
    init_tracing();
    sim.host("primary", move || {
        let path = primary_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 20,
                    max_log_duration: Some(2.0),
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

    // Setup replica1
    sim.host("replica1", move || {
        let path = replica1_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 20,
                    max_log_duration: Some(2.0),
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

    // Setup replica2
    sim.host("replica2", move || {
        let path = replica2_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 20,
                    max_log_duration: Some(2.0),
                    ..Default::default()
                },
                user_api_config: crate::config::UserApiConfig::default(),
                admin_api_config: Some(crate::config::AdminApiConfig {
                    acceptor: TurmoilAcceptor::bind(([0, 0, 0, 0], 9092)).await?,
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

            server.start_sim(8082).await?;
            Ok(())
        }
    });

    sim.client("isolation_tester", async move {
        let client = Client::new();
        
        client
            .post("http://primary:9090/v1/namespaces/testdb/create", json!({}))
            .await?;

        // Wait for replicas to sync
        sleep(Duration::from_secs(3)).await;

        let primary_db = Database::open_remote_with_connector(
            "http://testdb.primary:8080", 
            "", 
            TurmoilConnector
        )?;
        let primary_conn = primary_db.connect()?;

        // Create test schema for isolation testing
        primary_conn.execute(
            "CREATE TABLE bank_accounts (
                id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL,
                account_type TEXT NOT NULL,
                constraint_check INTEGER NOT NULL DEFAULT 0
            )", 
            ()
        ).await?;

        primary_conn.execute(
            "CREATE TABLE audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER,
                old_balance INTEGER,
                new_balance INTEGER,
                timestamp INTEGER,
                transaction_id TEXT
            )", 
            ()
        ).await?;

        // Insert test accounts
        for i in 0..10 {
            primary_conn.execute(
                "INSERT INTO bank_accounts (id, balance, account_type) VALUES (?, ?, ?)",
                (i, 1000, if i % 2 == 0 { "checking" } else { "savings" })
            ).await?;
        }

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(6));

        let mut handles = vec![];

        // Spawn concurrent workers that perform operations susceptible to write skew
        for worker_id in 0..5 {
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
                    let tx_id = format!("worker_{}_iter_{}", worker_id, iteration);
                    
                    // Start transaction
                    let tx = conn.transaction().await.unwrap();
                    
                    // Read current state (this read should be consistent with writes)
                    let mut stmt = tx.prepare(
                        "SELECT id, balance FROM bank_accounts WHERE account_type = ? ORDER BY id"
                    ).await.unwrap();
                    
                    let account_type = if worker_id % 2 == 0 { "checking" } else { "savings" };
                    let mut rows = stmt.query([account_type]).await.unwrap();
                    
                    let mut accounts = vec![];
                    let mut total_balance = 0i64;
                    
                    while let Some(row) = rows.next().await.unwrap() {
                        let id: i64 = row.get(0).unwrap();
                        let balance: i64 = row.get(1).unwrap();
                        accounts.push((id, balance));
                        total_balance += balance;
                    }
                    
                    // Business rule: total balance for account type should never go below 2000
                    if total_balance >= 2100 && !accounts.is_empty() {
                        // Perform transfer that should maintain invariant
                        let (from_id, from_balance) = accounts[0];
                        let transfer_amount = 100;
                        
                        if from_balance >= transfer_amount {
                            // Log the operation
                            tx.execute(
                                "INSERT INTO audit_log (account_id, old_balance, new_balance, timestamp, transaction_id) 
                                 VALUES (?, ?, ?, ?, ?)",
                                (from_id, from_balance, from_balance - transfer_amount, 
                                 std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64,
                                 tx_id.clone())
                            ).await.unwrap();
                            
                            // Update balance
                            tx.execute(
                                "UPDATE bank_accounts SET balance = balance - ? WHERE id = ?",
                                (transfer_amount, from_id)
                            ).await.unwrap();
                            
                            // Add delay to increase chance of race conditions
                            if iteration % 5 == 0 {
                                sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                    
                    // Commit transaction
                    match timeout(Duration::from_secs(10), tx.commit()).await {
                        Ok(Ok(_)) => {},
                        Ok(Err(e)) => {
                            eprintln!("Transaction failed for worker {}: {}", worker_id, e);
                        }
                        Err(_) => {
                            eprintln!("Transaction timeout for worker {}", worker_id);
                        }
                    }
                    
                    sleep(Duration::from_millis(50)).await;
                }
                
                worker_id
            });
            
            handles.push(handle);
        }

        // Network partition controller
        let partition_handle = tokio::spawn(async move {
            barrier.wait().await;
            
            for cycle in 0..8 {
                sleep(Duration::from_secs(5)).await;
                
                // Create different partition patterns
                match cycle % 4 {
                    0 => {
                        turmoil::partition("primary", "replica1");
                        sleep(Duration::from_secs(3)).await;
                        turmoil::repair("primary", "replica1");
                    }
                    1 => {
                        turmoil::partition("primary", "replica2");
                        sleep(Duration::from_secs(3)).await;
                        turmoil::repair("primary", "replica2");
                    }
                    2 => {
                        turmoil::partition("replica1", "replica2");
                        sleep(Duration::from_secs(3)).await;
                        turmoil::repair("replica1", "replica2");
                    }
                    3 => {
                        // Full partition
                        turmoil::partition("primary", "replica1");
                        turmoil::partition("primary", "replica2");
                        turmoil::partition("replica1", "replica2");
                        sleep(Duration::from_secs(2)).await;
                        turmoil::repair("primary", "replica1");
                        turmoil::repair("primary", "replica2");
                        turmoil::repair("replica1", "replica2");
                    }
                    _ => {}
                }
            }
        });

        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        partition_handle.await.unwrap();

        // Wait for final synchronization
        sleep(Duration::from_secs(5)).await;

        // Verify business invariants
        let mut stmt = primary_conn.prepare(
            "SELECT account_type, SUM(balance) as total_balance FROM bank_accounts GROUP BY account_type"
        ).await?;
        let mut rows = stmt.query([]).await?;
        
        while let Some(row) = rows.next().await? {
            let account_type: String = row.get(0)?;
            let total_balance: i64 = row.get(1)?;
            
            // Each account type started with 5000 (5 accounts * 1000 each)
            // Business rule was to never let total go below 2000
            if total_balance < 2000 {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("INVARIANT VIOLATION: {} accounts have total balance {} < 2000", 
                    account_type, total_balance);
            }
            
            // Also shouldn't have impossible values
            if total_balance < 0 || total_balance > 5000 {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("IMPOSSIBLE BALANCE: {} accounts have total balance {}", 
                    account_type, total_balance);
            }
        }

        // Check audit log consistency
        let mut audit_stmt = primary_conn.prepare(
            "SELECT account_id, old_balance, new_balance FROM audit_log ORDER BY id"
        ).await?;
        let mut audit_rows = audit_stmt.query([]).await?;
        
        while let Some(row) = audit_rows.next().await? {
            let account_id: i64 = row.get(0)?;
            let old_balance: i64 = row.get(1)?;
            let new_balance: i64 = row.get(2)?;
            
            // Verify the change makes sense
            if old_balance - new_balance != 100 {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("AUDIT LOG CORRUPTION: Account {} shows change from {} to {} (should be -100)", 
                    account_id, old_balance, new_balance);
            }
        }

        if corruption_detected.load(Ordering::SeqCst) {
            panic!("Data corruption or invariant violation detected!");
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test for checkpoint corruption during concurrent writes
/// This test targets the checkpoint process which is critical for WAL integrity
#[test]
fn checkpoint_corruption_during_concurrent_writes() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(150))
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
                    max_log_size: 15,
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

    sim.client("checkpoint_tester", async move {
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

        // Create table with checksums for integrity verification
        conn.execute(
            "CREATE TABLE checkpoint_test (
                id INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                checksum TEXT NOT NULL,
                write_order INTEGER NOT NULL
            )", 
            ()
        ).await?;

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let write_counter = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(4));

        let mut handles = vec![];

        // Spawn multiple writers
        for writer_id in 0..3 {
            let barrier = barrier.clone();
            let corruption_detected = corruption_detected.clone();
            let write_counter = write_counter.clone();
            
            let handle = tokio::spawn(async move {
                let db = Database::open_remote_with_connector(
                    "http://testdb.primary:8080", 
                    "", 
                    TurmoilConnector
                ).unwrap();
                let conn = db.connect().unwrap();

                barrier.wait().await;

                for batch in 0..25 {
                    // Write batch of records
                    for i in 0..10 {
                        let write_order = write_counter.fetch_add(1, Ordering::SeqCst);
                        let id = writer_id * 1000 + batch * 10 + i;
                        let data = format!("writer_{}_batch_{}_item_{}_order_{}", writer_id, batch, i, write_order);
                        let checksum = format!("chk_{}", data.len());
                        
                        match conn.execute(
                            "INSERT INTO checkpoint_test (id, data, checksum, write_order) VALUES (?, ?, ?, ?)",
                            (id, data, checksum, write_order as i64)
                        ).await {
                            Ok(_) => {},
                            Err(e) => {
                                eprintln!("Write failed for writer {}: {}", writer_id, e);
                            }
                        }
                    }
                    
                    // Force some transactions to trigger checkpoints
                    if batch % 5 == 0 {
                        for _ in 0..3 {
                            let _ = conn.execute("BEGIN", ()).await;
                            let _ = conn.execute("UPDATE checkpoint_test SET checksum = checksum || '_updated' WHERE id = ?", (writer_id * 1000 + batch * 10,)).await;
                            let _ = conn.execute("COMMIT", ()).await;
                        }
                    }
                    
                    sleep(Duration::from_millis(20)).await;
                }
                
                writer_id
            });
            
            handles.push(handle);
        }

        // Checkpoint controller - forces checkpoints at strategic times
        let checkpoint_handle = tokio::spawn(async move {
            barrier.wait().await;
            
            for _ in 0..15 {
                sleep(Duration::from_secs(3)).await;
                
                // Force checkpoint via admin API (if available) or by creating pressure
                let db = Database::open_remote_with_connector(
                    "http://testdb.primary:8080", 
                    "", 
                    TurmoilConnector
                ).unwrap();
                let conn = db.connect().unwrap();
                
                // Create checkpoint pressure by doing large operations
                for _ in 0..5 {
                    let _ = conn.execute("BEGIN", ()).await;
                    let _ = conn.execute("CREATE TEMP TABLE checkpoint_pressure AS SELECT * FROM checkpoint_test LIMIT 100", ()).await;
                    let _ = conn.execute("DROP TABLE checkpoint_pressure", ()).await;
                    let _ = conn.execute("COMMIT", ()).await;
                }
            }
        });

        // Wait for all operations
        for handle in handles {
            handle.await.unwrap();
        }
        
        checkpoint_handle.await.unwrap();

        // Wait for final checkpoint
        sleep(Duration::from_secs(5)).await;

        // Verify data integrity after checkpoints
        let mut stmt = conn.prepare("SELECT id, data, checksum, write_order FROM checkpoint_test ORDER BY write_order").await?;
        let mut rows = stmt.query([]).await?;
        
        let mut last_write_order = -1i64;
        let mut record_count = 0;
        
        while let Some(row) = rows.next().await? {
            let id: i64 = row.get(0)?;
            let data: String = row.get(1)?;
            let checksum: String = row.get(2)?;
            let write_order: i64 = row.get(3)?;
            
            record_count += 1;
            
            // Verify write order is monotonic (no corruption in ordering)
            if write_order <= last_write_order {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("WRITE ORDER CORRUPTION: Record {} has write_order {} <= previous {}", 
                    id, write_order, last_write_order);
            }
            last_write_order = write_order;
            
            // Verify checksum integrity
            let expected_checksum_base = format!("chk_{}", data.len());
            if !checksum.starts_with(&expected_checksum_base) {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("CHECKSUM CORRUPTION: Record {} has invalid checksum {}", id, checksum);
            }
            
            // Verify data format
            if !data.contains("writer_") || !data.contains("batch_") || !data.contains("item_") {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("DATA CORRUPTION: Record {} has malformed data: {}", id, data);
            }
        }
        
        // Should have written 3 writers * 25 batches * 10 items = 750 records
        if record_count < 700 {
            corruption_detected.store(true, Ordering::SeqCst);
            eprintln!("DATA LOSS: Expected ~750 records, found {}", record_count);
        }

        if corruption_detected.load(Ordering::SeqCst) {
            panic!("Checkpoint corruption detected!");
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test for replication lag consistency issues
/// This test verifies that replicas maintain consistency even under high replication lag
#[test]
fn replication_lag_consistency() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(200))
        .tcp_capacity(256) // Very limited bandwidth to create lag
        .build();

    let tmp_primary = tempdir().unwrap();
    let tmp_replica = tempdir().unwrap();
    let primary_path = tmp_primary.path().to_owned();
    let replica_path = tmp_replica.path().to_owned();

    // Setup primary
    init_tracing();
    sim.host("primary", move || {
        let path = primary_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 50,
                    max_log_duration: Some(5.0),
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

    // Setup replica with intentional delays
    sim.host("replica", move || {
        let path = replica_path.clone();
        async move {
            let server = TestServer {
                path: path.into(),
                db_config: crate::config::DbConfig {
                    max_log_size: 50,
                    max_log_duration: Some(5.0),
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

    sim.client("lag_tester", async move {
        let client = Client::new();
        
        client
            .post("http://primary:9090/v1/namespaces/testdb/create", json!({}))
            .await?;

        sleep(Duration::from_secs(2)).await;

        let primary_db = Database::open_remote_with_connector(
            "http://testdb.primary:8080", 
            "", 
            TurmoilConnector
        )?;
        let primary_conn = primary_db.connect()?;

        // Create sequence table to track operation ordering
        primary_conn.execute(
            "CREATE TABLE operation_sequence (
                seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_type TEXT NOT NULL,
                entity_id INTEGER NOT NULL,
                value INTEGER NOT NULL,
                timestamp INTEGER NOT NULL
            )", 
            ()
        ).await?;

        primary_conn.execute(
            "CREATE TABLE entities (
                id INTEGER PRIMARY KEY,
                value INTEGER NOT NULL,
                last_updated INTEGER NOT NULL
            )", 
            ()
        ).await?;

        // Initialize entities
        for i in 0..20 {
            primary_conn.execute(
                "INSERT INTO entities (id, value, last_updated) VALUES (?, ?, ?)",
                (i, 100, 0)
            ).await?;
        }

        let corruption_detected = Arc::new(AtomicBool::new(false));
        let operation_counter = Arc::new(AtomicU64::new(0));

        // Generate high-frequency operations on primary
        let writer_handle = tokio::spawn(async move {
            for round in 0..100 {
                let op_id = operation_counter.fetch_add(1, Ordering::SeqCst);
                let entity_id = round % 20;
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;

                // Perform operation
                let tx = primary_conn.transaction().await.unwrap();
                
                // Log the operation
                tx.execute(
                    "INSERT INTO operation_sequence (operation_type, entity_id, value, timestamp) VALUES (?, ?, ?, ?)",
                    ("increment", entity_id, 10, timestamp)
                ).await.unwrap();
                
                // Update entity
                tx.execute(
                    "UPDATE entities SET value = value + 10, last_updated = ? WHERE id = ?",
                    (timestamp, entity_id)
                ).await.unwrap();
                
                tx.commit().await.unwrap();
                
                // High frequency updates
                sleep(Duration::from_millis(100)).await;
            }
        });

        // Intermittent network issues to create replication lag
        let network_controller = tokio::spawn(async move {
            for _ in 0..10 {
                sleep(Duration::from_secs(8)).await;
                
                // Create temporary partition
                turmoil::partition("primary", "replica");
                sleep(Duration::from_secs(3)).await;
                turmoil::repair("primary", "replica");
                
                sleep(Duration::from_secs(5)).await;
            }
        });

        writer_handle.await.unwrap();
        network_controller.await.unwrap();

        // Wait for replication to catch up
        sleep(Duration::from_secs(10)).await;

        // Verify consistency between primary and replica
        let replica_db = Database::open_remote_with_connector(
            "http://testdb.replica:8081", 
            "", 
            TurmoilConnector
        )?;
        let replica_conn = replica_db.connect()?;

        // Check operation sequence consistency
        let mut primary_ops = primary_conn.prepare("SELECT seq_id, entity_id, value FROM operation_sequence ORDER BY seq_id").await?;
        let mut primary_rows = primary_ops.query([]).await?;
        
        let mut replica_ops = replica_conn.prepare("SELECT seq_id, entity_id, value FROM operation_sequence ORDER BY seq_id").await?;
        let mut replica_rows = replica_ops.query([]).await?;

        let mut primary_count = 0;
        let mut replica_count = 0;

        // Compare operation sequences
        loop {
            let primary_row = primary_rows.next().await?;
            let replica_row = replica_rows.next().await?;
            
            match (primary_row, replica_row) {
                (Some(p_row), Some(r_row)) => {
                    primary_count += 1;
                    replica_count += 1;
                    
                    let p_seq: i64 = p_row.get(0)?;
                    let p_entity: i64 = p_row.get(1)?;
                    let p_value: i64 = p_row.get(2)?;
                    
                    let r_seq: i64 = r_row.get(0)?;
                    let r_entity: i64 = r_row.get(1)?;
                    let r_value: i64 = r_row.get(2)?;
                    
                    if p_seq != r_seq || p_entity != r_entity || p_value != r_value {
                        corruption_detected.store(true, Ordering::SeqCst);
                        eprintln!("REPLICATION INCONSISTENCY: Operation {} - Primary: ({}, {}, {}), Replica: ({}, {}, {})",
                            p_seq, p_seq, p_entity, p_value, r_seq, r_entity, r_value);
                    }
                }
                (Some(_), None) => {
                    primary_count += 1;
                    corruption_detected.store(true, Ordering::SeqCst);
                    eprintln!("REPLICATION LAG: Primary has more operations than replica");
                    break;
                }
                (None, Some(_)) => {
                    replica_count += 1;
                    corruption_detected.store(true, Ordering::SeqCst);
                    eprintln!("REPLICATION CORRUPTION: Replica has operations not in primary");
                    break;
                }
                (None, None) => break,
            }
        }

        // Check final entity states
        let mut primary_entities = primary_conn.prepare("SELECT id, value FROM entities ORDER BY id").await?;
        let mut primary_entity_rows = primary_entities.query([]).await?;
        
        let mut replica_entities = replica_conn.prepare("SELECT id, value FROM entities ORDER BY id").await?;
        let mut replica_entity_rows = replica_entities.query([]).await?;

        while let (Some(p_row), Some(r_row)) = (primary_entity_rows.next().await?, replica_entity_rows.next().await?) {
            let p_id: i64 = p_row.get(0)?;
            let p_value: i64 = p_row.get(1)?;
            
            let r_id: i64 = r_row.get(0)?;
            let r_value: i64 = r_row.get(1)?;
            
            if p_id != r_id || p_value != r_value {
                corruption_detected.store(true, Ordering::SeqCst);
                eprintln!("ENTITY STATE INCONSISTENCY: Entity {} - Primary: {}, Replica: {}",
                    p_id, p_value, r_value);
            }
        }

        if corruption_detected.load(Ordering::SeqCst) {
            panic!("Replication consistency violation detected!");
        }

        eprintln!("Replication lag test completed successfully. Primary ops: {}, Replica ops: {}",
            primary_count, replica_count);

        Ok(())
    });

    sim.run().unwrap();
}