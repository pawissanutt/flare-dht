use flare::flare_kv_client::FlareKvClient;
use crate::flare::{GetRequest, SetRequest};

pub mod flare {
    tonic::include_proto!("flare"); // The string specified here must match the proto package name
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlareKvClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(SetRequest {
        key: "Tonic".into(),
        value: "test".into()
    });

    let response = client.set(request).await?;
    println!("RESPONSE={:?}", response);


    let request = tonic::Request::new(GetRequest {
        key: "Tonic".into(),
    });

    let response = client.get(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}