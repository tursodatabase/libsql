use crate::auth::{constants::GRPC_PROXY_AUTH_HEADER, Authorized, Permission};
use crate::namespace::NamespaceName;
use libsql_replication::rpc::replication::NAMESPACE_METADATA_KEY;
use tonic::Status;

/// A witness that the user has been authenticated.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Authenticated {
    Anonymous,
    Authorized(Authorized),
}

impl Authenticated {
    pub fn from_proxy_grpc_request<T>(
        req: &tonic::Request<T>,
        disable_namespace: bool,
    ) -> Result<Self, Status> {
        let namespace = if disable_namespace {
            None
        } else {
            req.metadata()
                .get_bin(NAMESPACE_METADATA_KEY)
                .map(|c| c.to_bytes())
                .transpose()
                .map_err(|_| Status::invalid_argument("failed to parse namespace header"))?
                .map(NamespaceName::from_bytes)
                .transpose()
                .map_err(|_| Status::invalid_argument("invalid namespace name"))?
        };

        let auth = match req
            .metadata()
            .get(GRPC_PROXY_AUTH_HEADER)
            .map(|v| v.to_str())
            .transpose()
            .map_err(|_| Status::invalid_argument("missing authorization header"))?
        {
            Some("full_access") => Authenticated::Authorized(Authorized {
                namespace,
                permission: Permission::FullAccess,
            }),
            Some("read_only") => Authenticated::Authorized(Authorized {
                namespace,
                permission: Permission::ReadOnly,
            }),
            Some("anonymous") => Authenticated::Anonymous,
            Some(level) => {
                return Err(Status::permission_denied(format!(
                    "invalid authorization level: {}",
                    level
                )))
            }
            None => return Err(Status::invalid_argument("x-proxy-authorization not set")),
        };

        Ok(auth)
    }

    pub fn upgrade_grpc_request<T>(&self, req: &mut tonic::Request<T>) {
        let key = tonic::metadata::AsciiMetadataKey::from_static(GRPC_PROXY_AUTH_HEADER);

        let auth = match self {
            Authenticated::Anonymous => "anonymous",
            Authenticated::Authorized(Authorized {
                permission: Permission::FullAccess,
                ..
            }) => "full_access",
            Authenticated::Authorized(Authorized {
                permission: Permission::ReadOnly,
                ..
            }) => "read_only",
        };

        let value = tonic::metadata::AsciiMetadataValue::try_from(auth).unwrap();

        req.metadata_mut().insert(key, value);
    }

    pub fn is_namespace_authorized(&self, namespace: &NamespaceName) -> bool {
        match self {
            Authenticated::Anonymous => false,
            Authenticated::Authorized(Authorized {
                namespace: Some(ns),
                ..
            }) => ns == namespace,
            // we threat the absence of a specific namespace has a permission to any namespace
            Authenticated::Authorized(Authorized {
                namespace: None, ..
            }) => true,
        }
    }
}
