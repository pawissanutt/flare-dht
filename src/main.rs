use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tonic::transport::Server;
use tracing::{debug, info};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use tracing_subscriber::util::SubscriberInitExt;
use flare::flare_kv_server::FlareKv;
use crate::dht::{LocalStore};
use crate::flare::{CleanRequest, CleanResponse, EmptyResponse,  SetRequest, SingleKeyRequest, ValueResponse};
use crate::flare::flare_kv_server::FlareKvServer;

mod dht;
mod discovery;

pub mod flare {
    tonic::include_proto!("flare"); // The string specified here must match the proto package name
}

pub struct FlareKvService {
    store: Arc<RwLock<LocalStore>>,
}

impl FlareKvService {
    fn new(store: Arc<RwLock<LocalStore>>) -> FlareKvService {
        FlareKvService {
            store
        }
    }
}

#[tonic::async_trait]
impl FlareKv for FlareKvService {
    async fn get(&self, request: Request<SingleKeyRequest>) -> Result<Response<ValueResponse>, Status> {
        let store_cloned = self.store.clone();
        let store = store_cloned.read().await;
        let key = request.into_inner().key;
        let shard = store.get_shard(&key);
        debug!("receive get request on '{}'", key);
        match shard.get(&key) {
            None => Err(Status::not_found("not found")),
            Some(value) => Ok(Response::new(ValueResponse {
                key,
                value,
            }))
        }
    }

    async fn delete(&self, request: Request<SingleKeyRequest>) -> Result<Response<EmptyResponse>, Status> {
        let store_cloned = self.store.clone();
        let store = store_cloned.read().await;
        let set_request = request.into_inner();
        let key = set_request.key;
        let shard = store.get_shard(&key);
        debug!("receive delete request on '{}'", key);
        shard.delete(&key);

        Ok(Response::new(EmptyResponse::default()))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<EmptyResponse>, Status> {
        let store_cloned = self.store.clone();
        let store = store_cloned.read().await;
        let set_request = request.into_inner();
        let key = set_request.key;
        let shard = store.get_shard(&key);
        let val = set_request.value;
        debug!("receive set request on '{}'", key);
        shard.set(key, val);

        Ok(Response::new(EmptyResponse::default()))
    }

    async fn clean(&self, _request: Request<CleanRequest>) -> Result<Response<CleanResponse>, Status> {
        let store_cloned = self.store.clone();
        let mut store = store_cloned.write().await;
        let mut count: u64 = 0;
        debug!("receive clean request");
        for s in store.iter_mut_shards() {
            count += s.count() as u64;
            s.clean();
        }
        Ok(Response::new(CleanResponse{
            count,
        }))
    }
}

fn init_log(){
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_thread_ids(true) // Enable thread ID logging
        .finish();
    subscriber.try_init()
        .expect("setting default subscriber failed");

}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_log();
    let addr = "[::1]:50051".parse()?;
    let store = Arc::new(RwLock::new(LocalStore::new()));
    let kv = FlareKvService::new(store);

    info!("start on {addr}");
    Server::builder()
        .add_service(FlareKvServer::new(kv))
        .serve(addr)
        .await?;

    Ok(())
}
