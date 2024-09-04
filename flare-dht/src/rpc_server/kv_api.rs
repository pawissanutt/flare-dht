use crate::cluster::FlareNode;
use crate::metadata::state_machine::{FlareControlRequest, FlareControlResponse};
use crate::metadata::FlareMetadataManager;
use crate::proto::flare_kv_server::FlareKv;
use crate::proto::{
    CleanRequest, CleanResponse, CreateCollectionRequest, CreateCollectionResponse, EmptyResponse,
    GetTopologyRequest, SetRequest, SingleKeyRequest, TopologyInfo, ValueResponse,
};
use crate::shard::KvShard;
use std::sync::Arc;
use futures::TryFutureExt;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct FlareKvService {
    flare_node: Arc<FlareNode>,
    metadata_manager: Arc<FlareMetadataManager>,
}

impl FlareKvService {
    #[allow(dead_code)]
    pub(crate) fn new(flare_node: Arc<FlareNode>) -> FlareKvService {
        let f = flare_node.clone();
        FlareKvService {
            flare_node,
            metadata_manager: f.metadata_manager.clone(),
        }
    }

    #[inline]
    async fn get_shard_ids(&self, col_name: &str) -> Result<Vec<u64>, Status> {
        self.metadata_manager
            .get_shard_ids(col_name)
            .await
            .ok_or(Status::not_found(format!(
                "not found collection {}",
                col_name
            )))
    }
}



#[tonic::async_trait]
impl FlareKv for FlareKvService {
    async fn get(
        &self,
        request: Request<SingleKeyRequest>,
    ) -> Result<Response<ValueResponse>, Status> {
        let key_request = request.into_inner();
        let shard = self.flare_node.get_shard(&key_request.collection, &key_request.key).await?;
        if let Some(val) = shard.get(&key_request.key).await {
            Ok(Response::new(ValueResponse {
                key: key_request.key,
                value: val,
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
        let shard = self.flare_node.get_shard(&key_request.collection, &key_request.key)
            .await?;
        shard
            .delete(&key_request.key)
            .await?;
        Ok(Response::new(EmptyResponse::default()))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<EmptyResponse>, Status> {
        let set_request = request.into_inner();
        let shard = self.flare_node.get_shard(&set_request.collection, &set_request.key)
            .await?;
        shard
            .set(set_request.key, set_request.value)
            .await?;
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
        let mut ccreq = request.into_inner();
        if ccreq.shard_assignments.len() != ccreq.shard_count as usize {
            let node_id = self.flare_node.node_id;
            ccreq.shard_assignments = vec![node_id].repeat(ccreq.shard_count as usize);
        }
        let req = FlareControlRequest::CreateCollection(ccreq);
        let resp = self
            .metadata_manager
            .raft
            .client_write(req)
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        if let FlareControlResponse::CollectionCreated { meta } = resp.response() {
            self.flare_node.sync_shard().await;
            Ok(Response::new(CreateCollectionResponse {
                name: meta.name.clone(),
            }))
        } else {
            Err(Status::already_exists("collection already exist"))
        }
    }
}
