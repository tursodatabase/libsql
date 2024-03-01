use anyhow::Context;
use axum::extract::FromRequestParts;

use crate::{
    auth::{parsers::auth_string_to_auth_context, Jwt, Auth},
    connection::RequestContext,
};

use super::{db_factory, AppState};

#[async_trait::async_trait]
impl FromRequestParts<AppState> for RequestContext {
    type Rejection = crate::error::Error;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> std::result::Result<Self, Self::Rejection> {

        // start todo this block is same as the one in mod.rs
        let namespace = db_factory::namespace_from_headers(
            &parts.headers,
            state.disable_default_namespace,
            state.disable_namespaces,
        )?;

        let namespace_jwt_key = state
            .namespaces
            .with(namespace.clone(), |ns| ns.jwt_key())
            .await??;

        let header = parts.headers.get(hyper::header::AUTHORIZATION).context("auth header not found")?;
        let header_str = header.to_str().context("non ASCII auth token")?;
        let context = auth_string_to_auth_context(header_str).context("auth header parsing failed")?;

        let auth = namespace_jwt_key
        .map(Jwt::new)
        .map(Auth::new)
        .unwrap_or_else(|| state.user_auth_strategy.clone())
        .authenticate(context)?;

    // end todo

        Ok(Self::new(
            auth,
            namespace,
            state.namespaces.meta_store().clone(),
        ))
    }
}
