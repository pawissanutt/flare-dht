use flare_dht::proto::flare_kv_client::FlareKvClient;
use flare_dht::proto::CleanRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlareKvClient::connect("http://127.0.0.1:8001").await?;

    let request = tonic::Request::new(CleanRequest::default());

    let response = client.clean(request).await?;
    println!("RESPONSE={:?}", response);

    Ok(())
}
