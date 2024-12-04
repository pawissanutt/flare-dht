use crate::cluster::FlareNode;
use crate::shard::KvShard;
use crate::shard::ShardEntry;
use flare_pb::flare_kv_server::FlareKv;
use flare_pb::{
    CreateCollectionRequest, CreateCollectionResponse, EmptyResponse,
    GetTopologyRequest, SetRequest, SingleKeyRequest, TopologyInfo,
    ValueResponse,
};
use std::sync::Arc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct FlareKvService<T>
where
    T: KvShard,
{
    flare_node: Arc<FlareNode<T>>,
}

impl<T> FlareKvService<T>
where
    T: KvShard,
{
    pub fn new(flare_node: Arc<FlareNode<T>>) -> FlareKvService<T> {
        FlareKvService { flare_node }
    }
}

#[tonic::async_trait]
impl<T> FlareKv for FlareKvService<T>
where
    T: KvShard<Key = String, Entry: ShardEntry> + 'static,
{
    async fn get(
        &self,
        request: Request<SingleKeyRequest>,
    ) -> Result<Response<ValueResponse>, Status> {
        let key_request = request.into_inner();
        let shard = self
            .flare_node
            .get_shard(&key_request.collection, key_request.key.as_bytes())
            .await?;
        if let Some(entry) = shard.get(&key_request.key).await? {
            Ok(Response::new(ValueResponse {
                key: key_request.key,
                value: entry.to_vec(),
            }))
        } else {
            Err(Status::not_found("not found data"))
        }
    }

    async fn delete(
        &self,
        request: Request<SingleKeyRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        let key_request = request.into_inner();
        let shard = self
            .flare_node
            .get_shard(&key_request.collection, key_request.key.as_bytes())
            .await?;
        shard.delete(&key_request.key).await?;
        Ok(Response::new(EmptyResponse::default()))
    }

    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        let set_request = request.into_inner();
        let shard = self
            .flare_node
            .get_shard(&set_request.collection, set_request.key.as_bytes())
            .await?;
        shard
            .set(set_request.key, ShardEntry::from_vec(set_request.value))
            .await?;
        Ok(Response::new(EmptyResponse::default()))
    }

    async fn get_topology(
        &self,
        _request: Request<GetTopologyRequest>,
    ) -> Result<Response<TopologyInfo>, Status> {
        todo!()
    }

    type WatchTopologyStream = ReceiverStream<Result<TopologyInfo, Status>>;

    async fn watch_topology(
        &self,
        _request: Request<GetTopologyRequest>,
    ) -> Result<Response<Self::WatchTopologyStream>, Status> {
        todo!()
    }

    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        self.flare_node
            .metadata_manager
            .create_collection(request.into_inner())
            .await
            .map(|r| Response::new(r))
            .map_err(|e| Status::from(e))
    }
}
