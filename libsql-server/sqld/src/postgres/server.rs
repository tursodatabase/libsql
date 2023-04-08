use std::net::SocketAddr;
use std::sync::Arc;

use pgwire::api::auth::noop::NoopStartupHandler;
use tokio::net::TcpStream;
use tokio::task::JoinSet;

use crate::database::factory::DbFactory;
use crate::postgres::proto::QueryHandler;

pub async fn run(addr: SocketAddr, factory: Arc<dyn DbFactory>) -> anyhow::Result<()> {
    let mut handles = JoinSet::new();
    let listener = tokio::net::TcpListener::bind(addr).await?;

    loop {
        tokio::select! {
            maybe_stream = listener.accept() => {
                match maybe_stream {
                    Ok((stream, addr)) => {
                        tracing::info!("new posgres connection from {addr}");
                        handle_connection(stream, factory.clone(), &mut handles);
                    },
                    Err(e) => {
                        tracing::error!("posgres connection error: {e}");
                    }
                }
            }
            Some(ret) = handles.join_next() => {
                if let Err(e) | Ok(Err(e)) = ret.map_err(|e| anyhow::anyhow!(e)) {
                    tracing::error!("posgres connection error: {e}");
                }
            },
            else => (),
        }
    }
}

fn handle_connection(
    stream: TcpStream,
    factory: Arc<dyn DbFactory>,
    handles: &mut JoinSet<anyhow::Result<()>>,
) {
    handles.spawn(async move {
        let db = factory.create().await?;
        let handler = Arc::new(QueryHandler::new(db));
        pgwire::tokio::process_socket(
            stream,
            None,
            Arc::new(NoopStartupHandler),
            handler.clone(),
            handler,
        )
        .await?;

        Ok(())
    });
}
