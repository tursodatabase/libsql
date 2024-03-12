use std::sync::Arc;

use crate::auth::{constants::GRPC_PROXY_AUTH_HEADER, Authorized};
use crate::namespace::NamespaceName;
use tonic::Status;

use super::authorized::Scope;
use super::Permission;

/// A witness that the user has been authenticated.
#[non_exhaustive]
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub enum Authenticated {
    Anonymous,
    Authorized(Arc<Authorized>),
    Legacy(LegacyAuth),
    FullAccess,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct LegacyAuth {
    pub(crate) namespace: Option<NamespaceName>,
    pub(crate) perm: Permission,
}

impl Authenticated {
    pub fn from_proxy_grpc_request<T>(req: &tonic::Request<T>) -> Result<Self, Status> {
        let auth = match req
            .metadata()
            .get(GRPC_PROXY_AUTH_HEADER)
            .map(|v| v.to_str())
            .transpose()
            .map_err(|_| Status::invalid_argument("missing authorization header"))?
        {
            Some(s) => serde_json::from_str::<Authenticated>(s).unwrap(),
            None => return Err(Status::invalid_argument("x-proxy-authorization not set")),
        };

        Ok(auth)
    }

    pub fn upgrade_grpc_request<T>(&self, req: &mut tonic::Request<T>) {
        let key = tonic::metadata::AsciiMetadataKey::from_static(GRPC_PROXY_AUTH_HEADER);

        let auth = serde_json::to_string(self).unwrap();
        let value = tonic::metadata::AsciiMetadataValue::try_from(auth).unwrap();

        req.metadata_mut().insert(key, value);
    }

    pub fn is_namespace_authorized(&self, namespace: &NamespaceName) -> bool {
        match self {
            Authenticated::Anonymous => false,
            Authenticated::Authorized(auth) => {
                auth.has_right(Scope::Namespace(namespace.clone()), Permission::Read)
            }
            Authenticated::FullAccess => true,
            Authenticated::Legacy(auth) => {
                auth.namespace.is_none() || auth.namespace.iter().any(|ns| ns == namespace)
            }
        }
    }

    pub(crate) fn has_right(
        &self,
        namespace: &NamespaceName,
        perm: Permission,
    ) -> crate::Result<()> {
        match self {
            Authenticated::Anonymous => Err(crate::Error::NotAuthorized(
                "anonymous access not allowed".to_string(),
            )),
            Authenticated::Authorized(a) => {
                if !a.has_right(Scope::Namespace(namespace.clone()), perm) {
                    Err(crate::Error::NotAuthorized(format!(
                                "Current session doesn't not have {perm:?} permission to namespace {namespace}")))
                } else {
                    Ok(())
                }
            }
            Authenticated::FullAccess => Ok(()),
            Authenticated::Legacy(auth) => {
                if self.is_namespace_authorized(namespace) && Permission::has_right(auth.perm, perm)
                {
                    Ok(())
                } else {
                    Err(crate::Error::NotAuthorized(format!(
                                "Current session doesn't not have {perm:?} permission to namespace {namespace}")))
                }
            }
        }
    }

    pub(crate) fn ddl_permitted(&self, namespace: &NamespaceName) -> crate::Result<()> {
        match self {
            Authenticated::Authorized(a) if a.ddl_permitted(namespace) => Ok(()),
            Authenticated::FullAccess => Ok(()),
            _ => Err(crate::Error::NotAuthorized(format!(
                "DDL statements not permitted on namespace {namespace}"
            ))),
        }
    }
}
