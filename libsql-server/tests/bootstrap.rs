use std::path::PathBuf;
use std::process::Command;

#[test]
fn bootstrap() {
    let iface_files = &["proto/admin_shell.proto"];
    let dirs = &["proto"];

    let out_dir = PathBuf::from(std::env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("generated");

    let config = prost_build::Config::new();

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .build_transport(true)
        .out_dir(&out_dir)
        .compile_with_config(config, iface_files, dirs)
        .unwrap();

    let status = Command::new("git")
        .arg("diff")
        .arg("--exit-code")
        .arg("--")
        .arg(&out_dir)
        .status()
        .unwrap();

    assert!(status.success(), "You should commit the protobuf files");
}
