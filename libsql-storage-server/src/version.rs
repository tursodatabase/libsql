use clap::builder::{IntoResettable, Str};

#[derive(Default)]
pub struct Version;

impl IntoResettable<Str> for Version {
    fn into_resettable(self) -> clap::builder::Resettable<Str> {
        version().into_resettable()
    }
}

pub fn version() -> String {
    let pkg_version = env!("CARGO_PKG_VERSION");
    let git_sha = env!("VERGEN_GIT_SHA");
    let build_date = env!("VERGEN_BUILD_DATE");
    format!("sqld {} ({} {})", pkg_version, &git_sha[..8], build_date)
}
