use std::{path::PathBuf, process::Command};

#[test]
fn bootstrap() {
    let iface_files = &["proto/replication_log.proto", "proto/proxy.proto"];
    let dirs = &["proto"];

    let out_dir = PathBuf::from(std::env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("generated");

    let mut config = prost_build::Config::new();
    config.bytes([".wal_log"]);

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .build_transport(true)
        .out_dir(&out_dir)
        .type_attribute(".proxy", "#[cfg_attr(test, derive(arbitrary::Arbitrary))]")
        .field_attribute(
            ".proxy.Value.data",
            "#[cfg_attr(test, arbitrary(with = crate::test::arbitrary_rpc_value))]",
        )
        .field_attribute(
            ".proxy.ProgramReq.namespace",
            "#[cfg_attr(test, arbitrary(with = crate::test::arbitrary_bytes))]",
        )
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
