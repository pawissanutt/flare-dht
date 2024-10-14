use anyhow::Ok;
use clap::Parser;
use rlt::{
    cli::BenchCli,
    IterReport, {BenchSuite, IterInfo},
};
use tokio::time::Instant;
use tonic::transport::{Channel, Uri};

use flare_pb::SetRequest;
use flare_pb::{flare_kv_client::FlareKvClient, SingleKeyRequest};
use rand::Rng;

#[derive(Parser, Clone)]
pub struct Opts {
    #[arg(default_value = "http://127.0.0.1:8001")]
    /// Target URL.
    pub url: Uri,
    #[arg(default_value_t = 512)]
    pub size: usize,

    #[arg(short, long, default_value_t = 0)]
    pub get_count: u64,

    /// Embed BenchCli into this Opts.
    #[command(flatten)]
    pub bench_opts: BenchCli,
}

#[derive(Clone)]
struct HttpBench {
    url: Uri,
    value: Vec<u8>,
    get_count: u64,
}

impl HttpBench {
    fn new(url: Uri, size: usize, get_count: u64) -> Self {
        let value: Vec<u8> = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(size)
            .map(u8::from)
            .collect();
        Self {
            url,
            value,
            get_count,
        }
    }
}

#[async_trait::async_trait]
impl BenchSuite for HttpBench {
    type WorkerState = FlareKvClient<Channel>;

    async fn state(&self, _: u32) -> anyhow::Result<Self::WorkerState> {
        let client = FlareKvClient::connect(self.url.clone()).await?;
        Ok(client)
    }

    async fn bench(
        &mut self,
        client: &mut Self::WorkerState,
        _: &IterInfo,
    ) -> anyhow::Result<IterReport> {
        let t = Instant::now();
        let id = tsid::create_tsid_256().to_string();
        let request = tonic::Request::new(SetRequest {
            key: id.clone(),
            value: self.value.clone(),
            collection: "default".into(),
        });
        let resp = client.set(request).await;
        let mut bytes: u64 = 0;
        let mut status = rlt::Status::success(200);
        match resp {
            Result::Ok(_) => {
                bytes = self.value.len() as u64;
            }
            Err(s) => {
                let duration = t.elapsed();
                return Ok(IterReport {
                    duration,
                    status: rlt::Status::error(s.code() as i64),
                    bytes: bytes,
                    items: 1,
                });
            }
        }

        for i in 0..self.get_count {
            let request = tonic::Request::new(SingleKeyRequest {
                key: id.clone(),
                collection: "default".into(),
            });
            let resp = client.get(request).await;
            if let Err(s) = resp {
                status = rlt::Status::success(s.code() as i64);
                let duration = t.elapsed();
                return Ok(IterReport {
                    duration,
                    status: status,
                    bytes: bytes,
                    items: 2 + i as u64,
                });
            }
            bytes += resp.unwrap().into_inner().value.len() as u64;
        }

        let duration = t.elapsed();
        return Ok(IterReport {
            duration,
            status: status,
            bytes: bytes as u64,
            items: 1 + self.get_count,
        });
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts: Opts = Opts::parse();
    let bench = HttpBench::new(opts.url, opts.size, opts.get_count);
    rlt::cli::run(opts.bench_opts, bench).await
}
