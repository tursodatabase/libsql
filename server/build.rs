use std::env;
use std::fs;
use std::process::Command;

fn main() {
    let mut pwd = env::current_dir().unwrap();
    pwd.push("../libsql");
    let libsql_dir = fs::canonicalize(pwd.as_path()).unwrap();
    let mut bindings = Command::new("./configure");
    let configure = bindings.current_dir(libsql_dir.as_path()).arg("--with-pic");
    let profile = std::env::var("PROFILE").unwrap();
    if profile.as_str() == "release" {
        configure.arg("--enable-releasemode");
    }
    configure.status().unwrap();
    Command::new("make")
        .current_dir(libsql_dir.as_path())
        .status()
        .unwrap();
    println!("cargo:rustc-link-search=native=libsql/.libs");
    println!("cargo:rustc-link-lib=static=sqlite3");
    println!("cargo:rerun-if-changed=libsql/*");
}
