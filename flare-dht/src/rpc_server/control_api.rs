use crate::cluster::FlareNode;
use flare_pb::flare_control_server::FlareControl;
use flare_pb::{
    ClusterMetadata, ClusterMetadataRequest, ClusterTopologyInfo,
    ClusterTopologyRequest, CollectionMetadata, JoinRequest, JoinResponse,
    LeaveRequest, LeaveResponse, ShardMetadata,
};
use openraft::{BasicNode, ChangeMembers};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::info;

pub struct FlareControlService {
    flare_node: Arc<FlareNode>,
}

impl FlareControlService {
    pub fn new(flare_node: Arc<FlareNode>) -> Self {
        FlareControlService { flare_node }
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
        let mm = self.flare_node.metadata_manager.clone();
        if !mm.is_leader().await {
            let mut cc =
                self.flare_node.client_pool.get_control_client().await?;
            return cc.join(join_request).await;
        }

        let mut map = BTreeMap::new();
        let node = BasicNode {
            addr: join_request.addr.clone(),
        };
        let node_id = join_request.node_id;
        map.insert(node_id, node);
        let is_initialized = mm.raft.is_initialized().await.unwrap();
        if is_initialized {
            let change_members = ChangeMembers::AddVoters(map);
            mm.raft
                .change_membership(change_members, false)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        } else {
            map.insert(
                mm.node_id,
                BasicNode {
                    addr: self.flare_node.addr.clone(),
                },
            );
            mm.raft
                .initialize(map)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        Ok(Response::new(JoinResponse::default()))
    }

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> Result<Response<LeaveResponse>, Status> {
        let leave_req = request.into_inner();
        let flare_node = self.flare_node.clone();
        info!("receive join request {}", &leave_req.node_id);
        let metadata_manager = flare_node.metadata_manager.clone();
        let mut nodes = BTreeSet::new();
        nodes.insert(leave_req.node_id);
        if metadata_manager.is_voter(leave_req.node_id).await {
            let change_members = ChangeMembers::RemoveVoters(nodes);
            metadata_manager
                .raft
                .change_membership(change_members, true)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        } else {
            let change_members = ChangeMembers::RemoveNodes(nodes);
            metadata_manager
                .raft
                .change_membership(change_members, true)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
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
        req: Request<ClusterMetadataRequest>,
    ) -> Result<Response<ClusterMetadata>, Status> {
        let flare = self.flare_node.clone();
        let mm = flare.metadata_manager.clone();
        if !mm.is_leader().await {
            let mut cc = flare.client_pool.get_control_client().await?;
            return cc.get_metadata(req.into_inner()).await;
        }

        mm.raft.ensure_linearizable().await.map_err(|e| {
            let err = format!("cannot read metadata: {}", e);
            Status::internal(err)
        })?;
        let state_machine = mm.state_machine.state_machine.read().await;
        let metadata_sm = &state_machine.app_data;
        let collection_sm = &metadata_sm.collections;
        let mut collections = HashMap::with_capacity(collection_sm.len());
        for (name, col) in collection_sm.iter() {
            collections.insert(
                name.into(),
                CollectionMetadata {
                    name: col.name.clone(),
                    shard_ids: col.shard_ids.clone(),
                    replication: col.replication as u32,
                },
            );
        }
        let mut shards = HashMap::with_capacity(metadata_sm.shards.len());
        let ssm = &metadata_sm.shards;
        for (id, shard) in ssm.iter() {
            shards.insert(
                *id,
                ShardMetadata {
                    id: shard.id,
                    collection: shard.collection.clone(),
                    primary: shard.primary,
                    replica: shard.replica.clone(),
                },
            );
        }

        let cm = ClusterMetadata {
            collections,
            shards,
            last_shard_id: metadata_sm.last_shard_id,
        };
        Ok(Response::new(cm))
    }
}
