use std::env;
use std::fs;
use std::process::Command;

use prost_build::Config;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut pwd = env::current_dir().unwrap();
    pwd.push("../libsql");
    let libsql_dir = fs::canonicalize(pwd.as_path()).unwrap();
    let mut bindings = Command::new("./configure");
    let configure = bindings.current_dir(libsql_dir.as_path()).arg("--with-pic");
    let profile = std::env::var("PROFILE").unwrap();
    if profile.as_str() == "release" {
        configure.arg("--enable-releasemode");
    }
    let output = configure.output().unwrap();
    if !output.status.success() {
        println!("{}", std::str::from_utf8(&output.stderr).unwrap());
        panic!("failed to configure");
    }

    if !Command::new("make")
        .current_dir(libsql_dir.as_path())
        .status()
        .unwrap()
        .success()
    {
        panic!("failed to compile");
    }

    let mut config = Config::new();
    config.bytes([".wal_log"]);
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_with_config(
            config,
            &["proto/wal_log.proto", "proto/proxy.proto"],
            &["proto"],
        )?;

    println!("cargo:rerun-if-changed=proto");

    Ok(())
}
