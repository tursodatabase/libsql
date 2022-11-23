use pgwire::api::auth::{noop::NoopStartupHandler, StartupHandler};
use pgwire::error::PgWireError;
use pgwire::messages::PgWireFrontendMessage;
use pgwire::tokio::PgWireMessageServerCodec;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

pub struct PgAuthenticator;

impl PgAuthenticator {
    pub async fn authenticate<T>(
        &self,
        client: &mut Framed<T, PgWireMessageServerCodec>,
        msg: &PgWireFrontendMessage,
    ) -> Result<(), PgWireError>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send,
    {
        NoopStartupHandler.on_startup(client, msg).await?;

        Ok(())
    }
}
