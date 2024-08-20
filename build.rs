fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(&["proto/flare-kv.proto", "proto/flare-mgnt.proto"], &["proto/"])?;
    // tonic_build::compile_protos("proto/flare-mgnt.proto")?;
    Ok(())
}