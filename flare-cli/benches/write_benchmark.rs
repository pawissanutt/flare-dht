use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flare_cli::start_server;
use flare_dht::{
    proto::CreateCollectionRequest,
    shard::{ByteEntry, HashMapShard, KvShard},
    FlareNode,
};
use rand::Rng;
use std::{sync::Arc, time::Duration};

async fn run(flare: Arc<FlareNode<HashMapShard>>, size: usize) {
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
    let ve = ByteEntry::from(value);
    let shard = flare.get_shard("benches", key.as_bytes()).await.unwrap();
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
            let flare_node = start_server(flare_dht::cli::ServerArgs {
                leader: true,
                not_server: true,
                ..Default::default()
            })
            .await
            .unwrap();
            flare_node
                .metadata_manager
                .create_collection(CreateCollectionRequest {
                    name: "benches".into(),
                    partition_count: 16,
                    shard_assignments: vec![],
                    ..Default::default()
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
