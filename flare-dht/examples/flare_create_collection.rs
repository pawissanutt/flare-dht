use flare_pb::flare_kv_client::FlareKvClient;
use flare_pb::{CreateCollectionRequest, SetRequest, SingleKeyRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let shard_count = std::env::args()
        .nth(1)
        .unwrap_or("1".into())
        .parse::<i32>()
        .unwrap();
    let col_name = std::env::args()
        .nth(2)
        .unwrap_or("default".into())
        .parse::<String>()
        .unwrap();
    let mut client = FlareKvClient::connect("http://127.0.0.1:8001").await?;

    let req = tonic::Request::new(CreateCollectionRequest {
        name: col_name.clone(),
        shard_count: shard_count,
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
        collection: col_name.clone(),
    });

    print!("SET: {:?}\n", req);
    let resp = client.set(req).await?;
    println!("RESPONSE: {:?}\n", resp);

    let req = tonic::Request::new(SingleKeyRequest {
        key: "foo".into(),
        collection: col_name.clone(),
    });

    print!("GET: {:?}\n", req);
    let response = client.get(req).await?;
    println!("RESPONSE={:?}\n", response);

    Ok(())
}
