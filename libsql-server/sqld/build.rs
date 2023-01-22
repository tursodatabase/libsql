use prost_build::Config;

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
