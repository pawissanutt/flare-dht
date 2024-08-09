pub mod flare {
    tonic::include_proto!("flare"); // The string specified here must match the proto package name
}


use crate::flare::flare_kv_client::FlareKvClient;
use crate::flare::SetRequest;



#[tokio::main]
async fn main() {

    let threads = std::env::args().nth(1).unwrap_or("1".into())
        .parse::<usize>().unwrap();
    let mut joins = Vec::with_capacity(threads);
    for i in 0..threads {
        let j = tokio::spawn(async move {
            let mut client = FlareKvClient::connect("http://[::1]:50051")
                .await.unwrap();
            for j in 0..100000 {
                let id = tsid::create_tsid_256().to_string();
                let request = tonic::Request::new(SetRequest {
                    key: id.clone(),
                    value: "0123456789".into(),
                });
                let response = client.set(request).await;
                if let Err(_) = response {
                    print!("error on setting key {id}");
                }
                if (j + 1)  % 100 == 0 {
                    println!("{i}: set kv for {} entries", j + 1)
                }
            }
        });
        joins.push(j);
    }

    for j in joins {
        j.await.expect("err");
    }
}