use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);
static TEST_IMAGE_TAG: OnceLock<String> = OnceLock::new();

fn build_test_image() -> String {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .expect("CARGO_MANIFEST_DIR should have a parent");
    let tag = "libsql-server:test".to_string();

    // Check if image already exists
    let check_output = std::process::Command::new("docker")
        .args(["images", "-q", &tag])
        .output()
        .expect("failed to run docker images");

    if !check_output.stdout.is_empty() {
        return tag;
    }

    let output = std::process::Command::new("docker")
        .arg("build")
        .arg("-t")
        .arg(&tag)
        .arg("-f")
        .arg(repo_root.join("Dockerfile"))
        .arg(repo_root)
        .output()
        .expect("failed to run docker build");

    if !output.status.success() {
        panic!(
            "docker build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    tag
}

fn get_test_image() -> &'static str {
    TEST_IMAGE_TAG.get_or_init(|| build_test_image())
}

async fn docker_host_port(container_name: &str, container_port: u16) -> anyhow::Result<u16> {
    let output = tokio::process::Command::new("docker")
        .args(["port", container_name, &format!("{}", container_port)])
        .output()
        .await?;
    if !output.status.success() {
        anyhow::bail!(
            "docker port failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let line = String::from_utf8_lossy(&output.stdout);
    // Format: "0.0.0.0:49153"
    let port = line
        .trim()
        .split(':')
        .last()
        .ok_or_else(|| anyhow::anyhow!("unexpected docker port output: {}", line))?
        .parse::<u16>()
        .map_err(|e| anyhow::anyhow!("failed to parse port from '{}': {}", line, e))?;
    Ok(port)
}

fn unique_id() -> String {
    use std::time::SystemTime;
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    let counter = ID_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}-{}-{}", pid, counter, ts)
}

pub struct MinioFixture {
    pub container_name: String,
    pub network_name: String,
    api_port: u16,
    console_port: u16,
}

impl MinioFixture {
    pub async fn start() -> anyhow::Result<Self> {
        let uid = unique_id();
        let container_name = format!("minio-test-{}", uid);
        let network_name = format!("sqld-net-{}", uid);

        // Create Docker network
        let net_output = tokio::process::Command::new("docker")
            .args(["network", "create", &network_name])
            .output()
            .await?;
        if !net_output.status.success() {
            anyhow::bail!(
                "Failed to create Docker network: {}",
                String::from_utf8_lossy(&net_output.stderr)
            );
        }

        // Start minio container with random host ports
        let run_output = tokio::process::Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &container_name,
                "--network",
                &network_name,
                "-p",
                ":9000",
                "-p",
                ":9001",
                "-e",
                "MINIO_ROOT_USER=minioadmin",
                "-e",
                "MINIO_ROOT_PASSWORD=minioadmin",
                "quay.io/minio/minio:latest",
                "server",
                "/data",
                "--console-address",
                ":9001",
            ])
            .output()
            .await?;

        if !run_output.status.success() {
            let _ = tokio::process::Command::new("docker")
                .args(["network", "rm", &network_name])
                .output()
                .await;
            anyhow::bail!(
                "Failed to start minio container: {}",
                String::from_utf8_lossy(&run_output.stderr)
            );
        }

        // Discover dynamically assigned host ports
        let api_port = docker_host_port(&container_name, 9000).await?;
        let console_port = docker_host_port(&container_name, 9001).await?;

        // Wait for minio to be ready
        let client = reqwest::Client::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while tokio::time::Instant::now() < deadline {
            match client
                .get(format!("http://127.0.0.1:{}/minio/health/live", api_port))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => break,
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        // Create bucket using mc
        let mc_output = tokio::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                &network_name,
                "quay.io/minio/mc:latest",
                "alias",
                "set",
                "myminio",
                &format!("http://{}:9000", container_name),
                "minioadmin",
                "minioadmin",
            ])
            .output()
            .await?;
        if !mc_output.status.success() {
            tracing::warn!(
                "mc alias set failed: {}",
                String::from_utf8_lossy(&mc_output.stderr)
            );
        }

        let mb_output = tokio::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                &network_name,
                "quay.io/minio/mc:latest",
                "mb",
                "myminio/bottomless",
            ])
            .output()
            .await?;
        if !mb_output.status.success() {
            tracing::warn!(
                "mc mb failed: {}",
                String::from_utf8_lossy(&mb_output.stderr)
            );
        }

        Ok(Self {
            container_name,
            network_name,
            api_port,
            console_port,
        })
    }

    pub fn internal_endpoint(&self) -> String {
        format!("http://{}:9000", self.container_name)
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        let output = tokio::process::Command::new("docker")
            .args(["stop", "-t", "5", &self.container_name])
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!(
                "Failed to stop minio: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    pub async fn restart(&mut self) -> anyhow::Result<()> {
        let output = tokio::process::Command::new("docker")
            .args(["start", &self.container_name])
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!(
                "Failed to restart minio: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        // Re-discover host ports after restart
        self.api_port = docker_host_port(&self.container_name, 9000).await?;
        self.console_port = docker_host_port(&self.container_name, 9001).await?;
        // Wait for minio to be ready
        let client = reqwest::Client::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while tokio::time::Instant::now() < deadline {
            match client
                .get(format!(
                    "http://127.0.0.1:{}/minio/health/live",
                    self.api_port
                ))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        anyhow::bail!("minio did not become ready after restart")
    }

    pub async fn cleanup(&self) -> anyhow::Result<()> {
        let _ = tokio::process::Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .output()
            .await;
        let _ = tokio::process::Command::new("docker")
            .args(["network", "rm", &self.network_name])
            .output()
            .await;
        Ok(())
    }
}

pub struct SqldFixture {
    network_name: String,
    internal_endpoint: String,
    http_port: u16,
    pub container_name: String,
}

impl SqldFixture {
    pub fn new(minio: &MinioFixture) -> Self {
        Self {
            network_name: minio.network_name.clone(),
            internal_endpoint: minio.internal_endpoint(),
            http_port: 0,
            container_name: format!("sqld-test-{}", unique_id()),
        }
    }

    pub async fn start(&mut self, data_dir: &Path) -> anyhow::Result<()> {
        // Remove any existing container
        let _ = tokio::process::Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .output()
            .await;

        let data_dir_str = data_dir.to_str().unwrap();

        let run_output = tokio::process::Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &self.container_name,
                "--network",
                &self.network_name,
                "-p",
                ":8080",
                "-e",
                &format!("LIBSQL_BOTTOMLESS_ENDPOINT={}", self.internal_endpoint),
                "-e",
                "LIBSQL_BOTTOMLESS_BUCKET=bottomless",
                "-e",
                "LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID=minioadmin",
                "-e",
                "LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY=minioadmin",
                "-e",
                "LIBSQL_BOTTOMLESS_AWS_DEFAULT_REGION=us-east-1",
                "-e",
                "SQLD_ENABLE_BOTTOMLESS_REPLICATION=true",
                "-e",
                "SQLD_DB_PATH=/var/lib/sqld",
                "-e",
                "LIBSQL_BOTTOMLESS_S3_READ_TIMEOUT_SECS=5",
                "-e",
                "LIBSQL_BOTTOMLESS_S3_CONNECT_TIMEOUT_SECS=5",
                "-e",
                "LIBSQL_BOTTOMLESS_S3_OPERATION_ATTEMPT_TIMEOUT_SECS=10",
                "-v",
                &format!("{}:/var/lib/sqld", data_dir_str),
                get_test_image(),
            ])
            .output()
            .await?;

        if !run_output.status.success() {
            anyhow::bail!(
                "Failed to start sqld container: {}",
                String::from_utf8_lossy(&run_output.stderr)
            );
        }

        self.http_port = docker_host_port(&self.container_name, 8080).await?;

        Ok(())
    }

    pub async fn kill(&self) -> anyhow::Result<()> {
        let output = tokio::process::Command::new("docker")
            .args(["kill", &self.container_name])
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!(
                "Failed to kill sqld: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        let output = tokio::process::Command::new("docker")
            .args(["stop", "-t", "30", &self.container_name])
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!(
                "Failed to stop sqld: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    pub async fn restart(&mut self) -> anyhow::Result<()> {
        let output = tokio::process::Command::new("docker")
            .args(["start", &self.container_name])
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!(
                "Failed to restart sqld: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        self.http_port = docker_host_port(&self.container_name, 8080).await?;
        Ok(())
    }

    pub async fn wait_for_ready(&self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        let client = reqwest::Client::new();
        while tokio::time::Instant::now() < deadline {
            match client
                .get(format!("http://127.0.0.1:{}/health", self.http_port))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        anyhow::bail!("sqld did not become ready within {:?}", timeout)
    }

    pub async fn wait_for_restore_start(&self) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while tokio::time::Instant::now() < deadline {
            let output = tokio::process::Command::new("docker")
                .args(["logs", &self.container_name])
                .output()
                .await?;
            let logs = String::from_utf8_lossy(&output.stdout);
            if logs.contains("Restoring from generation") || logs.contains("restore") {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        anyhow::bail!("sqld did not start restoring within 30 seconds")
    }

    pub async fn cleanup_data_dir(&self, data_dir: &Path) -> anyhow::Result<()> {
        let data_dir_str = data_dir.to_str().unwrap();
        let output = tokio::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "-v",
                &format!("{}:/data", data_dir_str),
                "alpine",
                "sh",
                "-c",
                "find /data -type f -name '*.db' -delete; find /data -type f -name '*.db-journal' -delete; find /data -type f -name '*.db-wal' -delete; find /data -type f -name '*.db-shm' -delete",
            ])
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!(
                "Failed to cleanup data dir: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    pub fn http_endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }

    pub async fn cleanup(&self) -> anyhow::Result<()> {
        let _ = tokio::process::Command::new("docker")
            .args(["rm", "-f", &self.container_name])
            .output()
            .await;
        Ok(())
    }
}

pub struct TestDatabase {
    endpoint: String,
    client: reqwest::Client,
}

impl TestDatabase {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            client: reqwest::Client::new(),
        }
    }

    pub async fn create_schema(&self) -> anyhow::Result<()> {
        self.execute_sql("DROP TABLE IF EXISTS test_data").await?;
        self.execute_sql("CREATE TABLE test_data (id INTEGER PRIMARY KEY, value TEXT, data BLOB)")
            .await?;
        Ok(())
    }

    pub async fn insert_test_data(&self, count: usize) -> anyhow::Result<()> {
        for i in 0..count {
            let value = format!("test_value_{}", i);
            let data = vec![0u8; 1024];
            let hex_data = hex::encode(&data);
            self.execute_sql(&format!(
                "INSERT INTO test_data (id, value, data) VALUES ({}, '{}', X'{}')",
                i, value, hex_data
            ))
            .await?;
        }
        Ok(())
    }

    pub async fn query_all(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        let resp = self.execute_sql("SELECT * FROM test_data").await?;
        Ok(extract_rows(&resp))
    }

    pub async fn verify_integrity(&self) -> anyhow::Result<()> {
        let resp = self
            .execute_sql("SELECT COUNT(*) AS total FROM test_data")
            .await?;
        let rows = extract_rows(&resp);
        let count = rows
            .first()
            .and_then(|r| r.get("total"))
            .and_then(|c| {
                c.as_i64()
                    .or_else(|| c.as_str().and_then(|s| s.parse::<i64>().ok()))
            })
            .unwrap_or(0);
        if count == 0 {
            anyhow::bail!("Database is empty after restore");
        }
        let resp = self
            .execute_sql("SELECT value FROM test_data WHERE id = 42")
            .await?;
        let rows = extract_rows(&resp);
        let value = rows
            .first()
            .and_then(|r| r.get("value"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if value != "test_value_42" {
            anyhow::bail!(
                "Data integrity check failed: expected 'test_value_42', got '{}'",
                value
            );
        }
        Ok(())
    }

    pub async fn wait_for_replication(&self) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    async fn execute_sql(&self, sql: &str) -> anyhow::Result<serde_json::Value> {
        let body = serde_json::json!({
            "requests": [
                { "type": "execute", "stmt": { "sql": sql } },
                { "type": "close" }
            ]
        });
        let resp = self
            .client
            .post(format!("{}/v2/pipeline", self.endpoint))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("SQL execution failed with status {}: {}", status, text);
        }
        let result: serde_json::Value = resp.json().await?;
        if let Some(results) = result.get("results").and_then(|r| r.as_array()) {
            for res in results {
                if res.get("type") == Some(&serde_json::json!("error")) {
                    let error = res
                        .get("error")
                        .cloned()
                        .unwrap_or(serde_json::json!("unknown error"));
                    anyhow::bail!("SQL execution error: {}", error);
                }
            }
        }
        Ok(result)
    }
}

fn extract_rows(response: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut rows = Vec::new();
    if let Some(results) = response.get("results").and_then(|r| r.as_array()) {
        for result in results {
            if result.get("type") == Some(&serde_json::json!("ok")) {
                if let Some(resp) = result.get("response") {
                    if resp.get("type") == Some(&serde_json::json!("execute")) {
                        if let Some(result_data) = resp.get("result") {
                            if let Some(cols) = result_data.get("cols").and_then(|c| c.as_array()) {
                                if let Some(result_rows) =
                                    result_data.get("rows").and_then(|r| r.as_array())
                                {
                                    for row in result_rows {
                                        let mut obj = serde_json::Map::new();
                                        if let Some(cells) = row.as_array() {
                                            for (i, col) in cols.iter().enumerate() {
                                                let col_name = col
                                                    .get("name")
                                                    .and_then(|n| n.as_str())
                                                    .unwrap_or("unknown");
                                                if let Some(cell) = cells.get(i) {
                                                    let value = if let Some(val_str) =
                                                        cell.get("value").and_then(|v| v.as_str())
                                                    {
                                                        if let Ok(n) = val_str.parse::<i64>() {
                                                            serde_json::json!(n)
                                                        } else {
                                                            serde_json::json!(val_str)
                                                        }
                                                    } else {
                                                        cell.clone()
                                                    };
                                                    obj.insert(col_name.to_string(), value);
                                                }
                                            }
                                        }
                                        rows.push(serde_json::Value::Object(obj));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    rows
}
