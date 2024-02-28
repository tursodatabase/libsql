use axum::extract::FromRequestParts;

use crate::{
    auth::{Jwt, UserAuthContext, UserAuthStrategy},
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

        let namespace_jwt_key = state
            .namespaces
            .with(namespace.clone(), |ns| ns.jwt_key())
            .await??;

        let auth_header = parts.headers.get(hyper::header::AUTHORIZATION);

        let auth = match namespace_jwt_key {
            Some(key) => Jwt::new(key).authenticate(UserAuthContext {
                user_credential: auth_header.cloned(),
            })?,
            None => state.user_auth_strategy.authenticate(UserAuthContext {
                user_credential: auth_header.cloned(),
            })?,
        };

        Ok(Self::new(
            auth,
            namespace,
            state.namespaces.meta_store().clone(),
        ))
    }
}
