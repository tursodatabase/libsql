use std::path::Path;

use anyhow::Result;
use sqlite::OpenFlags;
use tokio::net::ToSocketAddrs;

use worker_pool::WorkerPool;

mod job;
mod messages;
mod net;
mod scheduler;
mod server;
mod statements;
mod worker_pool;

pub async fn run_server(db_path: &Path, addr: impl ToSocketAddrs) -> Result<()> {
    let (pool, pool_sender) = WorkerPool::new(0, move || {
        sqlite::Connection::open_with_flags(
            &db_path,
            OpenFlags::new()
                .set_create()
                .set_no_mutex()
                .set_read_write(),
        )
        .unwrap()
    })?;
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    let scheduler = scheduler::Scheduler::new(pool_sender, receiver)?;
    let shandle = tokio::spawn(scheduler.start());
    server::start(addr, sender).await?;
    shandle.await?;
    pool.join().await;

    Ok(())
}
