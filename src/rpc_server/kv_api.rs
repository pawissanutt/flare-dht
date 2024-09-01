use crate::cluster::FlareNode;
use crate::metadata::state_machine::{FlareControlRequest, FlareControlResponse};
use crate::metadata::FlareMetadataManager;
use crate::proto::flare_kv_server::FlareKv;
use crate::proto::{
    CleanRequest, CleanResponse, CreateCollectionRequest, CreateCollectionResponse, EmptyResponse,
    GetTopologyRequest, SetRequest, SingleKeyRequest, TopologyInfo, ValueResponse,
};
use crate::shard::hashmap::HashMapShard;
use crate::shard::{ShardId, KvShard};
use dashmap::DashMap;
use std::sync::Arc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct FlareKvService {
    shards: Arc<DashMap<ShardId, HashMapShard>>,
    metadata_manager: Arc<FlareMetadataManager>,
}

impl FlareKvService {
    #[allow(dead_code)]
    pub(crate) fn new(node: Arc<FlareNode>) -> FlareKvService {
        FlareKvService {
            shards: node.shards.clone(),
            metadata_manager: node.metadata_manager.clone(),
        }
    }

    #[inline]
    async fn get_shard_ids(&self, col_name: &str) -> Result<Vec<u64>, Status> {
        let meta = self.metadata_manager.clone();
        meta.get_shard_ids(col_name)
            .await
            .ok_or(Status::not_found("not found store"))
    }
}

#[tonic::async_trait]
impl FlareKv for FlareKvService {
    async fn get(
        &self,
        request: Request<SingleKeyRequest>,
    ) -> Result<Response<ValueResponse>, Status> {
        let key_request = request.into_inner();
        let shard_ids = self.get_shard_ids(&key_request.key).await?;
        let shards = self.shards.clone();
        let shard = shards
            .get(&shard_ids[0])
            .ok_or(Status::internal("no shard"))?;
        shard.get(&key_request.key).await;
        if let Some(val) = shard.get(&key_request.key).await {
            Ok(Response::new(ValueResponse {
                key: key_request.key,
                value: val,
            }))
        } else {
            Err(Status::not_found("not found store"))
        }
    }

    async fn delete(
        &self,
        request: Request<SingleKeyRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        let set_request = request.into_inner();
        let shard_ids = self.get_shard_ids(&set_request.key).await?;
        let shards = self.shards.clone();
        let shard = shards
            .get(&shard_ids[0])
            .ok_or(Status::internal("no shard"))?;
        shard.delete(&set_request.key).await
            .map_err(|_e| Status::internal("raft error"))?;
        Ok(Response::new(EmptyResponse::default()))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<EmptyResponse>, Status> {
        let set_request = request.into_inner();
        let shard_ids = self.get_shard_ids(&set_request.key).await?;
        let shards = self.shards.clone();
        let shard = shards
            .get(&shard_ids[0])
            .ok_or(Status::internal("no shard"))?;
        shard.set(set_request.key, set_request.value).await
            .map_err(|_e| Status::internal("raft error"))?;
        Ok(Response::new(EmptyResponse::default()))
    }

    async fn clean(
        &self,
        _request: Request<CleanRequest>,
    ) -> Result<Response<CleanResponse>, Status> {
        todo!()
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
        let mm = __self.metadata_manager.clone();
        let ccreq = request.into_inner();
        let req = FlareControlRequest::CreateCollection {
            name: ccreq.name,
            shard_count: ccreq.shard_count as u32,
        };
        let resp = mm
            .raft
            .client_write(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if let FlareControlResponse::CollectionCreated { meta } = resp.response() {
            Ok(Response::new(CreateCollectionResponse {
                name: meta.name.clone(),
            }))
        } else {
            Err(Status::already_exists("collection already exist"))
        }
    }
}
