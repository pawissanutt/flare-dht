use std::sync::Arc;
use dashmap::DashMap;
use tonic::{Request, Response, Status};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use crate::shard::FlareShard;
use crate::proto::{EmptyResponse, CleanResponse, ValueResponse, SingleKeyRequest, SetRequest, CleanRequest, GetTopologyRequest, CreateCollectionRequest, CreateCollectionResponse, TopologyInfo};
use crate::proto::flare_kv_server::FlareKv;
use crate::types::{RaftRequest, ShardId};
use crate::cluster::{FlareNode, MapDescriptor};

pub struct FlareKvService {
    shards: Arc<DashMap<ShardId, FlareShard>>,
    map_descriptors: Arc<DashMap<String, MapDescriptor>>,
}


impl FlareKvService {
    pub(crate) fn new(node: &FlareNode) -> FlareKvService {
        FlareKvService {
            shards: node.shards.clone(),
            map_descriptors: node.map_descriptors.clone()
        }
    }
}

#[tonic::async_trait]
impl FlareKv for FlareKvService {
    async fn get(&self, request: Request<SingleKeyRequest>) -> Result<Response<ValueResponse>, Status> {
        let map_descriptors = self.map_descriptors.clone();
        let key_request = request.into_inner();
        let descriptor = map_descriptors.get(&key_request.collection)
            .ok_or(Status::not_found("not found store"))?;
        let shards = self.shards.clone();
        let shard = shards.get(&descriptor.shard_ids[0])
            .ok_or(Status::internal("no shard"))?;
        let state = shard.state_machine_store.state_machine.read().await;
        if let Some(val) = state.data.0.get(&key_request.key) {
            Ok(Response::new(ValueResponse {
                key: key_request.key,
                value: val.clone(),
            }))
        } else {
            Err(Status::not_found("not found store"))
        }
    }

    async fn delete(&self, request: Request<SingleKeyRequest>) -> Result<Response<EmptyResponse>, Status> {
        todo!()
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<EmptyResponse>, Status> {
        let map_descriptors = self.map_descriptors.clone();
        let set_request = request.into_inner();
        let descriptor = map_descriptors.get(&set_request.collection)
            .ok_or(Status::not_found("not found store"))?;
        let shards = self.shards.clone();
        let shard = shards.get(&descriptor.shard_ids[0])
            .ok_or(Status::internal("no shard"))?;
        let raft_request = RaftRequest::Set {
            key: set_request.key,
            value: set_request.value,
        };
        shard.raft.client_write(raft_request).await.
            map_err(|_e| Status::internal("raft error"))?;
        Ok(Response::new(EmptyResponse::default()))
    }

    async fn clean(&self, _request: Request<CleanRequest>) -> Result<Response<CleanResponse>, Status> {
        todo!()
    }

    async fn get_topology(&self, request: Request<GetTopologyRequest>) -> Result<Response<TopologyInfo>, Status> {
        todo!()
    }

    type WatchTopologyStream = ReceiverStream<Result<TopologyInfo, Status>>;

    async fn watch_topology(&self, request: Request<GetTopologyRequest>) -> Result<Response<Self::WatchTopologyStream>, Status> {
        todo!()
    }

    async fn create_collection(&self, request: Request<CreateCollectionRequest>) -> Result<Response<CreateCollectionResponse>, Status> {
        todo!()
    }
}