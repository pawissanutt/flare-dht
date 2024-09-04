use flare_pb::flare_kv_client::FlareKvClient;
use flare_pb::{CreateCollectionRequest, SetRequest, SingleKeyRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlareKvClient::connect("http://127.0.0.1:8001").await?;

    let req = tonic::Request::new(CreateCollectionRequest {
        name: "test".into(),
        shard_count: 1,
        ..Default::default()
    });
    print!("CREATE COLLECTION: {:?}\n", req);
    let resp = client.create_collection(req).await;
    match resp {
        Ok(_) => {
            println!("RESPONSE: {:?}\n", resp)
        }
        Err(_) => {
            println!("RESPONSE: {:?}\n", resp)
        }
    }

    let req = tonic::Request::new(SetRequest {
        key: "foo".into(),
        value: "bar".into(),
        collection: "test".into(),
    });

    print!("SET: {:?}\n", req);
    let resp = client.set(req).await?;
    println!("RESPONSE: {:?}\n", resp);

    let req = tonic::Request::new(SingleKeyRequest {
        key: "foo".into(),
        collection: "test".into(),
    });

    print!("GET: {:?}\n", req);
    let response = client.get(req).await?;
    println!("RESPONSE={:?}\n", response);

    Ok(())
}
