use tonic::{Request, Response, Status};
use crate::cluster::FlareNode;
use crate::types::flare::flare_control_server::FlareControl;
use crate::types::flare::{GetTopologyRequest, JoinRequest, JoinResponse, TopologyResponse};

pub struct FlareControlService {
    node: FlareNode,
}

impl FlareControlService {
    pub fn new(node: FlareNode) -> Self {
        FlareControlService { 
            node
        }
    }
}

#[tonic::async_trait]
impl FlareControl for FlareControlService {
    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        
        todo!()
    }

    async fn get_topology(&self, request: Request<GetTopologyRequest>) -> Result<Response<TopologyResponse>, Status> {
        todo!()
    }
}