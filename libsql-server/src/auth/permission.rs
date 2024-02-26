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
