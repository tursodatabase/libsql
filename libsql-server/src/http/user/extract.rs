use axum::extract::FromRequestParts;

use crate::{
    auth::{Auth, AuthError, Jwt, UserAuthContext},
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
        let namespace = db_factory::namespace_from_headers(
            &parts.headers,
            state.disable_default_namespace,
            state.disable_namespaces,
        )?;
        // todo dupe #auth
        let namespace_jwt_key = state
            .namespaces
            .with(namespace.clone(), |ns| ns.jwt_key())
            .await??;

        let context = parts
            .headers
            .get(hyper::header::AUTHORIZATION)
            .ok_or(AuthError::AuthHeaderNotFound)
            .and_then(|h| h.to_str().map_err(|_| AuthError::AuthHeaderNonAscii))
            .and_then(|t| UserAuthContext::from_auth_str(t));

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
