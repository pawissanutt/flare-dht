use anyhow::Ok;
use clap::Parser;
use rlt::{
    cli::BenchCli,
    IterReport, {BenchSuite, IterInfo},
};
use tokio::time::Instant;
use tonic::transport::{Channel, Uri};

use flare_pb::flare_kv_client::FlareKvClient;
use flare_pb::SetRequest;
use rand::Rng;

#[derive(Parser, Clone)]
pub struct Opts {
    /// Target URL.
    pub url: Uri,

    pub size: usize,

    /// Embed BenchCli into this Opts.
    #[command(flatten)]
    pub bench_opts: BenchCli,
}

#[derive(Clone)]
struct HttpBench {
    url: Uri,
    value: Vec<u8>,
}

impl HttpBench {
    fn new(url: Uri, size: usize) -> Self {
        let value: Vec<u8> = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(size)
            .map(u8::from)
            .collect();
        Self { url, value }
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
        let duration = t.elapsed();
        match resp {
            Result::Ok(_) => {
                let bytes = self.value.len();
                let status = rlt::Status::success(200);
                return Ok(IterReport {
                    duration,
                    status,
                    bytes: bytes as u64,
                    items: 1,
                });
            }
            Err(s) => {
                return Ok(IterReport {
                    duration,
                    status: rlt::Status::error(s.code() as i64),
                    bytes: 0,
                    items: 1,
                });
            }
        }
        // return Ok(IterReport {
        //     duration,
        //     status: rlt::Status::success(200),
        //     bytes: 1,
        //     items: 1,
        // });
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts: Opts = Opts::parse();
    let bench = HttpBench::new(opts.url, opts.size);
    rlt::cli::run(opts.bench_opts, bench).await
}
