#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum Permission {
    #[serde(rename = "ro")]
    Read,
    #[serde(rename = "rw")]
    Write,
    #[serde(rename = "roa")]
    AttachRead,
}

impl Permission {
    pub(crate) fn has_right(perm: Self, requested: Self) -> bool {
        match (perm, requested) {
            (Permission::Read, Permission::Read) => true,
            (Permission::AttachRead, Permission::AttachRead) => true,
            (Permission::Write, Permission::Write) => true,
            (Permission::Read, Permission::Write) => false,
            (Permission::Read, Permission::AttachRead) => false,
            (Permission::Write, Permission::Read) => true,
            (Permission::Write, Permission::AttachRead) => false,
            (Permission::AttachRead, Permission::Read) => true,
            (Permission::AttachRead, Permission::Write) => false,
        }
    }
}
