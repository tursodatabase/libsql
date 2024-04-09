use std::sync::Arc;

use axum::extract::{FromRequestParts, Path};
use hyper::http::request::Parts;
use hyper::HeaderMap;
use libsql_replication::rpc::replication::NAMESPACE_METADATA_KEY;

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

    let result = 
    if let Some(metadata) = headers.get(NAMESPACE_METADATA_KEY) {
        metadata
            .to_str()
            .map_err(|s| Error::InvalidNamespaceBytes(s))
            .and_then(|ns| NamespaceName::from_string(ns.into()))
    } else {
        headers
            .get("host")
            .ok_or_else(|| Error::InvalidHost("missing host header".into()))
            .and_then(|res| Ok(res.as_bytes()))
            .and_then(|host| {
                std::str::from_utf8(host)
                    .map_err(|_| Error::InvalidHost("host header is not valid UTF-8".into()))
            })
            .and_then(|host_str| split_namespace(host_str))
    };

    if result.is_err() && !disable_default_namespace {
        Ok(NamespaceName::default())
    } else {
        result
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
