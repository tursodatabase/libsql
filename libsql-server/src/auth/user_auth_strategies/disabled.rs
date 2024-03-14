use super::{UserAuthContext, UserAuthStrategy};
use crate::auth::{AuthError, Authenticated};

pub struct Disabled {}

impl UserAuthStrategy for Disabled {
    fn authenticate(
        &self,
        _context: Result<UserAuthContext, AuthError>,
    ) -> Result<Authenticated, AuthError> {
        tracing::trace!("executing disabled auth");
        Ok(Authenticated::FullAccess)
    }
}

impl Disabled {
    pub fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticates() {
        let strategy = Disabled::new();
        let context = Ok(UserAuthContext::empty());

        assert!(matches!(
            strategy.authenticate(context).unwrap(),
            Authenticated::FullAccess
        ))
    }
}
