use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("flare_descriptor.bin"))
        .type_attribute(".", "#[derive(serde::Serialize,serde::Deserialize)]")
        .protoc_arg("--experimental_allow_proto3_optional")
        // .bytes(&[".flare.SetRequest", ".flare.ValueResponse"])
        .compile_protos(
            &[
                "proto/flare-common.proto",
                "proto/flare-kv.proto",
                "proto/flare-mgnt.proto",
            ],
            &["proto/"],
        )?;
    // tonic_build::compile_protos("proto/flare-mgnt.proto")?;
    Ok(())
}
