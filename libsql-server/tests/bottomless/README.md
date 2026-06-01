# Bottomless Restore Interruption Tests

Integration tests for libsql-server's (sqld) bottomless S3 backup/restore feature. These tests verify that sqld correctly handles interrupted restores from object storage (minio).

## Test Cases

### 1. `basic_restore`
Sanity check - verifies sqld can restore a database from minio after local files are deleted.

### 2. `sqld_interrupted`
Simulates sqld crashing mid-restore (SIGKILL). After restart, sqld must detect the incomplete restore and complete it successfully.

### 3. `minio_interrupted`
Minio (S3) is stopped mid-restore, then restarted. sqld must retry and complete the restore once S3 is available again.

### 4. `network_partition`
The sqld container is disconnected from the Docker network mid-restore (simulating a network partition), then reconnected. sqld must resume and complete the restore without requiring a restart.

## Running Tests

```bash
# Run all bottomless tests
cargo test --test tests bottomless -- --nocapture

# Run individual tests
cargo test --test tests bottomless::basic_restore -- --nocapture
cargo test --test tests bottomless::sqld_interrupted -- --nocapture
cargo test --test tests bottomless::minio_interrupted -- --nocapture
cargo test --test tests bottomless::network_partition -- --nocapture
```

## Architecture

- **MinIO**: Runs in a Docker container with a dedicated Docker network per test
- **sqld**: Runs in a Docker container on the same network, with port mapping for HTTP access
- Each test gets unique container/network names and ports to avoid conflicts when running in parallel
- Data directory is a temp directory mounted into the sqld container

## Prerequisites

- Docker daemon running
- Port range 20000-30000 available on localhost

## Notes

- Tests never skip or ignore. If the Docker environment is unavailable, they fail loudly.
- All temporary containers and networks are cleaned up after each test.
