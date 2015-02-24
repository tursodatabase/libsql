extern crate "pkg-config" as pkg_config;

fn main() {
    pkg_config::find_library("sqlite3").unwrap();
}
