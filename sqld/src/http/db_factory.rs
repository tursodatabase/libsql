use std::sync::Arc;

use axum::extract::FromRequestParts;
use bytes::Bytes;
use hyper::http::request::Parts;
use hyper::HeaderMap;

use crate::connection::MakeConnection;
use crate::database::Database;
use crate::error::Error;
use crate::namespace::MakeNamespace;
use crate::DEFAULT_NAMESPACE_NAME;

use super::AppState;

pub struct MakeConnectionExtractor<D>(pub Arc<dyn MakeConnection<Connection = D>>);

#[async_trait::async_trait]
impl<F> FromRequestParts<AppState<F>>
    for MakeConnectionExtractor<<F::Database as Database>::Connection>
where
    F: MakeNamespace,
{
    type Rejection = Error;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState<F>,
    ) -> Result<Self, Self::Rejection> {
        let ns = namespace_from_headers(&parts.headers, state.allow_default_namespace)?;
        Ok(Self(
            state
                .namespaces
                .with(ns, |ns| ns.db.connection_maker())
                .await?,
        ))
    }
}

pub fn namespace_from_headers(
    headers: &HeaderMap,
    allow_default_namespace: bool,
) -> crate::Result<Bytes> {
    let host = headers
        .get("host")
        .ok_or_else(|| Error::InvalidHost("missing host header".into()))?
        .as_bytes();
    let host_str = std::str::from_utf8(host)
        .map_err(|_| Error::InvalidHost("host header is not valid UTF-8".into()))?;

    match split_namespace(host_str) {
        Ok(ns) => Ok(ns),
        Err(_) if allow_default_namespace => Ok(DEFAULT_NAMESPACE_NAME.into()),
        Err(e) => Err(e),
    }
}

fn split_namespace(host: &str) -> crate::Result<Bytes> {
    let (ns, _) = host.split_once('.').ok_or_else(|| {
        Error::InvalidHost("host header should be in the format <namespace>.<...>".into())
    })?;
    let ns = Bytes::copy_from_slice(ns.as_bytes());
    Ok(ns)
}
