use cc;
use std::process::Command;

fn main() {
    let target = if let Ok(profile) = std::env::var("PROFILE") {
        match profile.as_str() {
            "debug" => "loadable_dbg",
            "release" => "loadable",
            _ => "loadable",
        }
    } else {
        "loadable"
    };

    Command::new("make")
        .current_dir("../../")
        .arg(target)
        .status()
        .expect("failed to make loadable extension");

    cc::Build::new()
        .file("../../src/sqlite/sqlite3.c")
        .include("../../src/sqlite/")
        .flag("-DSQLITE_CORE")
        .compile("sqlite3");
}
