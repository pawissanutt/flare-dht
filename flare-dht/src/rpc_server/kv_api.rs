use crate::cluster::FlareNode;
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

pub struct FlareKvService {
    flare_node: Arc<FlareNode>,
}

impl FlareKvService {
    pub(crate) fn new(flare_node: Arc<FlareNode>) -> FlareKvService {
        FlareKvService { flare_node }
    }
}

#[tonic::async_trait]
impl FlareKv for FlareKvService {
    async fn get(
        &self,
        request: Request<SingleKeyRequest>,
    ) -> Result<Response<ValueResponse>, Status> {
        let key_request = request.into_inner();
        let shard = self
            .flare_node
            .get_shard(&key_request.collection, &key_request.key)
            .await?;
        if let Some(val) = shard.get(&key_request.key).await {
            Ok(Response::new(ValueResponse {
                key: key_request.key,
                value: val.value,
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
            .get_shard(&key_request.collection, &key_request.key)
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
            .get_shard(&set_request.collection, &set_request.key)
            .await?;
        shard
            .set(set_request.key, ShardEntry::from(set_request.value))
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
            .create_collection(request.into_inner())
            .await
            .map(|r| Response::new(r))
            .map_err(|e| Status::from(e))
    }
}
