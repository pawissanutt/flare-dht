use flare_pb::flare_kv_client::FlareKvClient;
use flare_pb::SetRequest;
use rand::Rng;
use std::error::Error;
use std::time::SystemTime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let threads = std::env::args()
        .nth(1)
        .unwrap_or("1".into())
        .parse::<usize>()
        .unwrap();
    let count = std::env::args()
        .nth(2)
        .unwrap_or("100000".into())
        .parse::<u32>()
        .unwrap();
    let size = std::env::args()
        .nth(3)
        .unwrap_or("512".into())
        .parse::<usize>()
        .unwrap();
    let mut joins = Vec::with_capacity(threads);
    let start = SystemTime::now();

    for i in 0..threads {
        let j = tokio::spawn(async move {
            let mut client = FlareKvClient::connect("http://127.0.0.1:8001")
                .await
                .unwrap();
            let value: Vec<u8> = rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(size)
                .map(u8::from)
                .collect();
            for j in 0..count {
                let id = tsid::create_tsid_256().to_string();
                let request = tonic::Request::new(SetRequest {
                    key: id.clone(),
                    value: value.clone(),
                    collection: "default".into(),
                });
                let response = client.set(request).await;
                if let Err(_) = response {
                    print!("error on setting key {id}");
                }

                if (j + 1) % 1000 == 0 {
                    println!("{i}: set kv for {} entries", j + 1)
                }
            }
        });
        joins.push(j);
    }

    for j in joins {
        j.await?;
    }
    let total = count * (threads as u32);
    let time = start.elapsed().unwrap().as_millis();
    println!("time: {time} ms");
    let throughput = (total as f64) / (time as f64) * 1000f64;
    println!("throughput: {throughput} r/s");
    Ok(())
}
