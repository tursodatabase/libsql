use prost_build::Config;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protobuf_src::protoc());

    let mut config = Config::new();
    config.bytes([".wal_log"]);
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".proxy", "#[cfg_attr(test, derive(arbitrary::Arbitrary))]")
        .field_attribute(
            ".proxy.Value.data",
            "#[cfg_attr(test, arbitrary(with = crate::test::arbitrary_rpc_value))]",
        )
        .field_attribute(
            ".proxy.ProgramReq.namespace",
            "#[cfg_attr(test, arbitrary(with = crate::test::arbitrary_bytes))]",
        )
        .compile_with_config(
            config,
            &["proto/replication_log.proto", "proto/proxy.proto"],
            &["proto"],
        )?;

    println!("cargo:rerun-if-changed=proto");

    Ok(())
}
