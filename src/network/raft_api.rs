use std::iter::Map;
use std::sync::Arc;
use dashmap::DashMap;
use tonic::{Request, Response, Status};
use crate::cluster::FlareNode;
use crate::shard::FlareShard;
use crate::types::flare::ByteWrapper;
use crate::types::flare::flare_raft_server::FlareRaft;
use crate::types::ShardId;
use crate::util::{encode, decode};

pub struct FlareRaftService {
    shards: Arc<DashMap<ShardId, FlareShard>>
}

impl FlareRaftService {
    pub fn new(node: &FlareNode) -> Self {
        FlareRaftService{shards:node.shards.clone()}
    }
}

#[tonic::async_trait]
impl FlareRaft for FlareRaftService {
    async fn vote(&self, request: Request<ByteWrapper>) -> Result<Response<ByteWrapper>, Status> {
        let map = self.shards.clone();
        let wrapper = request.into_inner();
        let option = map.get(&wrapper.shard_id);
        if let Some(shard) = option {
            let req = decode(&wrapper.data)?;
            let result = shard.raft.vote(req).await
                .map_err(|e| Status::internal(e.to_string()))?;
            Ok(Response::new(encode(&result)?))
        } else {
            Err(Status::not_found("no shard exist"))
        }
    }

    async fn snapshot(&self, request: Request<ByteWrapper>) -> Result<Response<ByteWrapper>, Status> {
        let map = self.shards.clone();
        let wrapper = request.into_inner();
        let option = map.get(&wrapper.shard_id);
        if let Some(shard) = option {
            let req = decode(&wrapper.data)?;
            let result = shard.raft.install_snapshot(req).await
                .map_err(|e| Status::internal(e.to_string()))?;
            Ok(Response::new(encode(&result)?))
        } else {
            Err(Status::not_found("no shard exist"))
        }
    }

    async fn append(&self, request: Request<ByteWrapper>) -> Result<Response<ByteWrapper>, Status> {

        let map = self.shards.clone();
        let wrapper = request.into_inner();
        let option = map.get(&wrapper.shard_id);
        if let Some(shard) = option {
            let req = decode(&wrapper.data)?;
            let result = shard.raft.append_entries(req).await
                .map_err(|e| Status::internal(e.to_string()))?;
            Ok(Response::new(encode(&result)?))
        } else {
            Err(Status::not_found("no shard exist"))
        }
    }
}