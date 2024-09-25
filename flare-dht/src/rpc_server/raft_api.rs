use crate::metadata::FlareMetadataManager;
use crate::util::{server_decode, server_encode};
use flare_pb::flare_metadata_raft_server::FlareMetadataRaft;
use flare_pb::ByteWrapper;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct FlareMetaRaftService {
    mm: Arc<FlareMetadataManager>,
}

impl FlareMetaRaftService {
    pub fn new(mm: Arc<FlareMetadataManager>) -> Self {
        FlareMetaRaftService { mm }
    }
}

#[tonic::async_trait]
impl FlareMetadataRaft for FlareMetaRaftService {
    async fn vote(
        &self,
        request: Request<ByteWrapper>,
    ) -> Result<Response<ByteWrapper>, Status> {
        let wrapper = request.into_inner();
        let req = server_decode(&wrapper.data)?;
        let result = self
            .mm
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
        let wrapper = request.into_inner();
        let req = server_decode(&wrapper.data)?;
        let result = self
            .mm
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(server_encode(&result)?))
    }

    async fn append(
        &self,
        request: Request<ByteWrapper>,
    ) -> Result<Response<ByteWrapper>, Status> {
        let wrapper = request.into_inner();
        let req = server_decode(&wrapper.data)?;
        let result = self
            .mm
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(server_encode(&result)?))
    }
}
