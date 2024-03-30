use axum::extract::FromRequestParts;

use crate::{
    auth::{Auth, Jwt},
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
            .await
            .and_then(|o|o)?;

        let auth = namespace_jwt_key
            .map(|key|Auth::new(Jwt::new(key)))
            .unwrap_or_else(|| state.user_auth_strategy.clone());

        let context = super::build_context(&parts.headers, &auth.strategy.required_fields());

        auth.authenticate(context)
        .map(|a| Self::new(a, namespace, state.namespaces.meta_store().clone()))
        .map_err(|e|e.into())

    }
}
