use crate::cluster::FlareNode;
use crate::proto::flare_raft_server::FlareRaft;
use crate::proto::ByteWrapper;
use crate::util::{server_decode, server_encode};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct FlareRaftService {
    flare_node: Arc<FlareNode>,
}

impl FlareRaftService {
    pub fn new(node: Arc<FlareNode>) -> Self {
        FlareRaftService { flare_node: node }
    }
}

#[tonic::async_trait]
impl FlareRaft for FlareRaftService {
    async fn vote(&self, request: Request<ByteWrapper>) -> Result<Response<ByteWrapper>, Status> {
        let flare = self.flare_node.clone();
        let wrapper = request.into_inner();
        let shard_id = &wrapper.shard_id;
        if *shard_id == 0 {
            let mm = flare.metadata_manager.clone();
            let req = server_decode(&wrapper.data)?;
            let result = mm
                .raft
                .vote(req)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            Ok(Response::new(server_encode(&result)?))
        } else {
            let option = flare.shards.get(&wrapper.shard_id);
            if let Some(shard) = option {
                let req = server_decode(&wrapper.data)?;
                let result = shard
                    .raft
                    .vote(req)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(server_encode(&result)?))
            } else {
                Err(Status::not_found("no shard exist"))
            }
        }
    }

    async fn snapshot(
        &self,
        request: Request<ByteWrapper>,
    ) -> Result<Response<ByteWrapper>, Status> {
        let flare = self.flare_node.clone();
        let wrapper = request.into_inner();
        let shard_id = &wrapper.shard_id;
        if *shard_id == 0 {
            let mm = flare.metadata_manager.clone();
            let req = server_decode(&wrapper.data)?;
            let result = mm
                .raft
                .install_snapshot(req)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            Ok(Response::new(server_encode(&result)?))
        } else {
            let option = flare.shards.get(&wrapper.shard_id);
            if let Some(shard) = option {
                let req = server_decode(&wrapper.data)?;
                let result = shard
                    .raft
                    .install_snapshot(req)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(server_encode(&result)?))
            } else {
                Err(Status::not_found("no shard exist"))
            }
        }
    }

    async fn append(&self, request: Request<ByteWrapper>) -> Result<Response<ByteWrapper>, Status> {
        let flare = self.flare_node.clone();
        let wrapper = request.into_inner();
        let shard_id = &wrapper.shard_id;
        if *shard_id == 0 {
            let mm = flare.metadata_manager.clone();
            let req = server_decode(&wrapper.data)?;
            let result = mm
                .raft
                .append_entries(req)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            Ok(Response::new(server_encode(&result)?))
        } else {
            let option = flare.shards.get(&wrapper.shard_id);
            if let Some(shard) = option {
                let req = server_decode(&wrapper.data)?;
                let result = shard
                    .raft
                    .append_entries(req)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(server_encode(&result)?))
            } else {
                Err(Status::not_found("no shard exist"))
            }
        }
    }
}
