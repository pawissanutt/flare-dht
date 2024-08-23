use std::collections::BTreeMap;
use std::sync::{Arc};
use openraft::{BasicNode, ChangeMembers};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use crate::cluster::FlareNode;
use crate::proto::flare_control_server::FlareControl;
use crate::proto::{ClusterTopologyInfo, ClusterTopologyRequest, JoinRequest, JoinResponse};

pub struct FlareControlService {
    flare_node: Arc<Mutex<FlareNode>>,
}

impl FlareControlService {
    pub fn new(flare_node: Arc<Mutex<FlareNode>>) -> Self {
        FlareControlService {
            flare_node
        }
    }
}

#[tonic::async_trait]
impl FlareControl for FlareControlService {
    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let join_request = request.into_inner();
        let flare_node = self.flare_node.lock().await;
        for shard in flare_node.shards.iter() {
            let node_id = join_request.node_id;
            let node = BasicNode { addr: join_request.addr.clone() };
            let mut map = BTreeMap::new();
            map.insert(node_id, node);
            let is_initialized = shard.raft.is_initialized().await.unwrap();
            if is_initialized {
                let change_members = ChangeMembers::AddVoters(map);
                shard.raft.change_membership(change_members, true).await
                .map_err(|e| Status::internal(e.to_string()))?;
            } else {
                map.insert(flare_node.node_id, BasicNode{addr: flare_node.addr.clone()});
                shard.raft.initialize(map).await
                .map_err(|e| Status::internal(e.to_string()))?;
            }   
            
        }
        Ok(Response::new(JoinResponse::default()))
    }

    async fn get_topology(&self, request: Request<ClusterTopologyRequest>) -> Result<Response<ClusterTopologyInfo>, Status> {
        

        todo!()
    }
}