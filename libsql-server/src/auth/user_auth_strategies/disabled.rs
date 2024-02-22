use super::{UserAuthContext, UserAuthStrategy};
use crate::auth::{AuthError, Authenticated, Authorized, Permission};

pub struct Disabled {}

impl UserAuthStrategy for Disabled {
    fn authenticate(&self, _context: UserAuthContext) -> Result<Authenticated, AuthError> {
        tracing::info!("executing disabled auth");

        Ok(Authenticated::Authorized(Authorized {
            namespace: None,
            permission: Permission::FullAccess,
        }))
    }
}

impl Disabled {
    pub fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use crate::namespace::NamespaceName;

    use super::*;

    #[test]
    fn authenticates() {
        let strategy = Disabled::new();
        let context = UserAuthContext {
            namespace: NamespaceName::default(),
            namespace_credential: None,
            user_credential: None,
        };

        assert_eq!(
            strategy.authenticate(context).unwrap(),
            Authenticated::Authorized(Authorized {
                namespace: None,
                permission: Permission::FullAccess,
            })
        )
    }
}
