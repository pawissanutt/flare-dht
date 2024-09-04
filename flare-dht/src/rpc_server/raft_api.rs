use crate::cluster::FlareNode;
use crate::proto::flare_metadata_raft_server::FlareMetadataRaft;
use crate::proto::ByteWrapper;
use crate::util::{server_decode, server_encode};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct FlareMetaRaftService {
    flare_node: Arc<FlareNode>,
}

impl FlareMetaRaftService {
    pub fn new(node: Arc<FlareNode>) -> Self {
        FlareMetaRaftService { flare_node: node }
    }
}

#[tonic::async_trait]
impl FlareMetadataRaft for FlareMetaRaftService {
    async fn vote(&self, request: Request<ByteWrapper>) -> Result<Response<ByteWrapper>, Status> {
        let flare = self.flare_node.clone();
        let wrapper = request.into_inner();
        let mm = flare.metadata_manager.clone();
        let req = server_decode(&wrapper.data)?;
        let result = mm
            .raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(server_encode(&result)?))
    }

    async fn snapshot(
        &self,
        request: Request<ByteWrapper>,
    ) -> Result<Response<ByteWrapper>, Status> {
        let flare = self.flare_node.clone();
        let wrapper = request.into_inner();
        let mm = flare.metadata_manager.clone();
        let req = server_decode(&wrapper.data)?;
        let result = mm
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(server_encode(&result)?))
    }

    async fn append(&self, request: Request<ByteWrapper>) -> Result<Response<ByteWrapper>, Status> {
        let flare = self.flare_node.clone();
        let wrapper = request.into_inner();
        let mm = flare.metadata_manager.clone();
        let req = server_decode(&wrapper.data)?;
        let result = mm
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(server_encode(&result)?))
    }
}
