use anyhow::Context;
use axum::extract::FromRequestParts;

use crate::{
    auth::{Auth, Jwt, UserAuthContext},
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

        let context = parts.headers
        .get(hyper::header::AUTHORIZATION).context("auth header not found") // todo this context is swallowed for now, gotta fix that but not with panicking
        .and_then(|h| h.to_str().context("non ascii auth token"))
        .and_then(|t| t.try_into())
        .unwrap_or(UserAuthContext{scheme: None, token: None});


        let authenticated = namespace_jwt_key
        .map(Jwt::new)
        .map(Auth::new)
        .unwrap_or_else(|| state.user_auth_strategy.clone())
        .authenticate(context)?;            

        Ok(Self::new(
            authenticated,
            namespace,
            state.namespaces.meta_store().clone(),
        ))
    }
}
