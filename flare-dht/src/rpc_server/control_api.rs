use crate::metadata::MetadataManager;
use flare_pb::flare_control_server::FlareControl;
use flare_pb::{
    ClusterMetadata, ClusterMetadataRequest, ClusterTopologyInfo,
    ClusterTopologyRequest, JoinRequest, JoinResponse, LeaveRequest,
    LeaveResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::info;

pub struct FlareControlService {
    metadata_manager: Arc<dyn MetadataManager>,
}

impl FlareControlService {
    pub fn new(metadata_manager: Arc<dyn MetadataManager>) -> Self {
        Self { metadata_manager }
    }
}

#[tonic::async_trait]
impl FlareControl for FlareControlService {
    async fn join(
        &self,
        request: Request<JoinRequest>,
    ) -> Result<Response<JoinResponse>, Status> {
        let join_request = request.into_inner();
        info!("receive join request {}", &join_request.addr);
        self.metadata_manager
            .other_join(join_request)
            .await
            .map(|r| Response::new(r))
            .map_err(|e| e.into())
    }

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> Result<Response<LeaveResponse>, Status> {
        let leave_req = request.into_inner();
        info!("receive leave request {}", &leave_req.node_id);
        self.metadata_manager.other_leave(leave_req.node_id).await?;
        Ok(Response::new(LeaveResponse::default()))
    }

    async fn get_topology(
        &self,
        _request: Request<ClusterTopologyRequest>,
    ) -> Result<Response<ClusterTopologyInfo>, Status> {
        todo!()
    }

    async fn get_metadata(
        &self,
        _req: Request<ClusterMetadataRequest>,
    ) -> Result<Response<ClusterMetadata>, Status> {
        let meta = __self.metadata_manager.get_metadata().await?;
        Ok(Response::new(meta))
    }
}
