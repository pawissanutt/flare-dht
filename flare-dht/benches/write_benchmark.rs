use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flare_dht::{shard::ShardEntry, start_server, FlareNode, ServerArgs};
use flare_pb::CreateCollectionRequest;
use rand::Rng;
use std::{sync::Arc, time::Duration};

async fn run(flare: Arc<FlareNode>, size: usize) {
    let key: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    let value: Vec<u8> = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(size)
        .map(u8::from)
        .collect();
    let ve = ShardEntry::from(value);
    let shard = flare.get_shard("benches", &key).await.unwrap();
    shard.set(key, ve).await.unwrap();
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();

    static KB: usize = 1024;
    for size in [512, KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB].iter() {
        let flare_node = runtime.block_on(async {
            let flare_node = start_server(ServerArgs {
                leader: true,
                not_server: true,
                ..Default::default()
            })
            .await
            .unwrap();
            flare_node
                .create_collection(CreateCollectionRequest {
                    name: "benches".into(),
                    shard_count: 16,
                    shard_assignments: vec![],
                })
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            flare_node
        });
        c.bench_with_input(BenchmarkId::new("write", size), &size, |b, &s| {
            b.to_async(&runtime).iter(|| run(flare_node.clone(), *s));
        });
        runtime.block_on(async {
            flare_node.close().await;
        })
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
