use crate::flare::{SetRequest, SingleKeyRequest};
use flare::flare_kv_client::FlareKvClient;

pub mod flare {
    tonic::include_proto!("flare"); // The string specified here must match the proto package name
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlareKvClient::connect("http://127.0.0.1:8001").await?;

    let request = tonic::Request::new(SetRequest {
        key: "Tonic".into(),
        value: "test".into(),
        collection: "default".into(),
    });

    let response = client.set(request).await?;
    println!("RESPONSE={:?}", response);

    let request = tonic::Request::new(SingleKeyRequest {
        key: "Tonic".into(),
        collection: "default".into(),
    });

    let response = client.get(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
