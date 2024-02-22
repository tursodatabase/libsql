use crate::auth::Permission;
use crate::namespace::NamespaceName;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Authorized {
    pub namespace: Option<NamespaceName>,
    pub permission: Permission,
}
