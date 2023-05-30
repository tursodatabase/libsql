use prost_build::Config;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = vergen::Config::default();
    *config.build_mut().kind_mut() = vergen::TimestampKind::All;
    // when building from source, git is not available
    if vergen::vergen(config.clone()).is_err() {
        *config.git_mut().enabled_mut() = false;
        vergen::vergen(config)?;
    }

    let mut config = Config::new();
    config.bytes([".wal_log"]);
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".proxy", "#[cfg_attr(test, derive(arbitrary::Arbitrary))]")
        .field_attribute(".proxy.Value.data", "#[cfg_attr(test, arbitrary(with = crate::database::write_proxy::test::arbitrary_rpc_value))]")
        .compile_with_config(
            config,
            &["proto/replication_log.proto", "proto/proxy.proto"],
            &["proto"],
        )?;

    println!("cargo:rerun-if-changed=proto");

    Ok(())
}
