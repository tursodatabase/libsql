use std::sync::Arc;

use axum::extract::{FromRequestParts, Path};
use hyper::http::request::Parts;
use hyper::HeaderMap;

use crate::auth::Authenticated;
use crate::connection::MakeConnection;
use crate::database::Connection;
use crate::error::Error;
use crate::namespace::NamespaceName;

use super::AppState;

pub struct MakeConnectionExtractor(pub Arc<dyn MakeConnection<Connection = Connection>>);

#[async_trait::async_trait]
impl FromRequestParts<AppState> for MakeConnectionExtractor {
    type Rejection = Error;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let auth = Authenticated::from_request_parts(parts, state).await?;
        let ns = namespace_from_headers(
            &parts.headers,
            state.disable_default_namespace,
            state.disable_namespaces,
        )?;
        Ok(Self(
            state
                .namespaces
                .with_authenticated(ns, auth, |ns| ns.db.connection_maker())
                .await?,
        ))
    }
}

pub fn namespace_from_headers(
    headers: &HeaderMap,
    disable_default_namespace: bool,
    disable_namespaces: bool,
) -> crate::Result<NamespaceName> {
    if disable_namespaces {
        return Ok(NamespaceName::default());
    }

    let host = headers
        .get("host")
        .ok_or_else(|| Error::InvalidHost("missing host header".into()))?
        .as_bytes();
    let host_str = std::str::from_utf8(host)
        .map_err(|_| Error::InvalidHost("host header is not valid UTF-8".into()))?;

    match split_namespace(host_str) {
        Ok(ns) => Ok(ns),
        Err(_) if !disable_default_namespace => Ok(NamespaceName::default()),
        Err(e) => Err(e),
    }
}

pub struct MakeConnectionExtractorPath(pub Arc<dyn MakeConnection<Connection = Connection>>);
#[async_trait::async_trait]
impl FromRequestParts<AppState> for MakeConnectionExtractorPath {
    type Rejection = Error;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let auth = Authenticated::from_request_parts(parts, state).await?;
        let Path((ns, _)) = Path::<(NamespaceName, String)>::from_request_parts(parts, state)
            .await
            .map_err(|e| Error::InvalidPath(e.to_string()))?;
        Ok(Self(
            state
                .namespaces
                .with_authenticated(ns, auth, |ns| ns.db.connection_maker())
                .await?,
        ))
    }
}

fn split_namespace(host: &str) -> crate::Result<NamespaceName> {
    let (ns, _) = host.split_once('.').ok_or_else(|| {
        Error::InvalidHost("host header should be in the format <namespace>.<...>".into())
    })?;
    let ns = NamespaceName::from_string(ns.to_owned())?;
    Ok(ns)
}
