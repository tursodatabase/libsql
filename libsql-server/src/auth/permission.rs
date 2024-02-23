#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Permission {
    FullAccess,
    ReadOnly,
}
