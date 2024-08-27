use crate::flare::CleanRequest;
use flare::flare_kv_client::FlareKvClient;

pub mod flare {
    tonic::include_proto!("flare"); // The string specified here must match the proto package name
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlareKvClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(CleanRequest::default());

    let response = client.clean(request).await?;
    println!("RESPONSE={:?}", response);

    Ok(())
}
