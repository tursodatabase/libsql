use std::sync::Arc;

use hashbrown::HashSet;

use crate::namespace::NamespaceName;

use super::{AuthError, Authenticated, Permission};

#[derive(Debug, serde::Deserialize, serde::Serialize, Default)]
pub struct Authorized {
    #[serde(rename = "ro", default)]
    pub read_only: Option<Scopes>,
    #[serde(rename = "rw", default)]
    pub read_write: Option<Scopes>,
    #[serde(rename = "roa", default)]
    pub read_only_attach: Option<Scopes>,
    #[serde(rename = "rwa", default)]
    pub read_write_attach: Option<Scopes>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Scope {
    Namespace(NamespaceName),
}

impl Authorized {
    pub fn has_right(&self, scope: Scope, perm: Permission) -> bool {
        match (perm, scope) {
            (Permission::Read, Scope::Namespace(ref name)) => self.can_read_ns(name),
            (Permission::Write, Scope::Namespace(ref name)) => self.can_write_ns(name),
            (Permission::AttachRead, Scope::Namespace(ref name)) => self.can_attach_ns(name),
        }
    }

    fn is_empty(&self) -> bool {
        self.read_write.is_none()
            && self.read_only.is_none()
            && self.read_only_attach.is_none()
            && self.read_write_attach.is_none()
    }

    pub fn merge_legacy(
        mut self,
        namespace: Option<NamespaceName>,
        perm: Option<Permission>,
    ) -> Result<Authenticated, AuthError> {
        match (namespace, perm) {
            (Some(ns), Some(perm)) => {
                let scope = match perm {
                    Permission::Read => self.read_only.get_or_insert_with(Default::default),
                    Permission::Write => self.read_write.get_or_insert_with(Default::default),
                    Permission::AttachRead => {
                        self.read_only_attach.get_or_insert_with(Default::default)
                    }
                };
                scope
                    .namespaces
                    .get_or_insert_with(Default::default)
                    .insert(ns);
                Ok(Authenticated::Authorized(Arc::new(self)))
            }
            // legacy shit: interpret that as full access to ns
            (Some(ns), None) => {
                self.read_write
                    .get_or_insert_with(Default::default)
                    .namespaces
                    .get_or_insert_with(Default::default)
                    .insert(ns);
                Ok(Authenticated::Authorized(Arc::new(self)))
            }
            (None, None) => {
                // if there are no other claims, no claims is interpreted as full access.
                if self.is_empty() {
                    Ok(Authenticated::FullAccess)
                } else {
                    Ok(Authenticated::Authorized(Arc::new(self)))
                }
            }
            _ => Err(AuthError::JwtInvalid),
        }
    }

    fn can_write_ns(&self, name: &NamespaceName) -> bool {
        if let Some(Scopes {
            namespaces: Some(ref ns),
            ..
        }) = self.read_write
        {
            if ns.contains(name) {
                return true;
            }
        }

        if let Some(Scopes {
            namespaces: Some(ref ns),
            ..
        }) = self.read_write_attach
        {
            if ns.contains(name) {
                return true;
            }
        }

        false
    }

    fn can_read_ns(&self, name: &NamespaceName) -> bool {
        if self.can_write_ns(name) {
            return true;
        }

        if let Some(Scopes {
            namespaces: Some(ref ns),
            ..
        }) = self.read_only
        {
            if ns.contains(name) {
                return true;
            }
        }

        if let Some(Scopes {
            namespaces: Some(ref ns),
            ..
        }) = self.read_only_attach
        {
            if ns.contains(name) {
                return true;
            }
        }

        false
    }

    #[cfg(test)]
    pub fn perms_iter(&self) -> impl Iterator<Item = (Scope, Permission)> + '_ {
        macro_rules! perm_iter {
            ($field:ident, $perm:expr) => {
                self.$field
                    .as_ref()
                    .map(|s| s.iter())
                    .into_iter()
                    .flatten()
                    .zip(std::iter::repeat($perm))
            };
        }

        let ro_iter = perm_iter!(read_only, Permission::Read);
        let rw_iter = perm_iter!(read_write, Permission::Write);
        let ro_attach_iter = perm_iter!(read_only_attach, Permission::AttachRead);
        let rw_attach_iter = perm_iter!(read_write_attach, Permission::AttachRead);

        ro_iter
            .chain(rw_iter)
            .chain(ro_attach_iter)
            .chain(rw_attach_iter)
    }

    fn can_attach_ns(&self, name: &NamespaceName) -> bool {
        if let Some(Scopes {
            namespaces: Some(ref ns),
            ..
        }) = self.read_only_attach
        {
            if ns.contains(name) {
                return true;
            }
        }

        if let Some(Scopes {
            namespaces: Some(ref ns),
            ..
        }) = self.read_write_attach
        {
            if ns.contains(name) {
                return true;
            }
        }

        false
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Default)]
pub struct Scopes {
    #[serde(rename = "ns", default)]
    pub namespaces: Option<HashSet<NamespaceName>>,
}

impl Scopes {
    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = Scope> + '_ {
        self.namespaces
            .as_ref()
            .map(|nss| nss.iter().cloned().map(|ns| Scope::Namespace(ns)))
            .into_iter()
            .flatten()
    }
}
