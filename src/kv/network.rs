use crate::kv::{typ, KvTypeConfig};
use crate::proto::flare_raft_client::FlareRaftClient;
use crate::proto::ByteWrapper;
use crate::raft::FlareRpcError;
use crate::raft::NodeId;
use crate::shard::ShardId;
use crate::util::{client_decode, client_encode};
use bincode::config;
use bincode::config::Configuration;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use tonic::transport::{Channel, Uri};
use tracing::info;

type RpcClient = FlareRaftClient<Channel>;

pub struct Network {
    pub(crate) shard_id: ShardId,
}

impl Network {
    pub fn new(shard_id: ShardId) -> Self {
        Network { shard_id }
    }
}

impl RaftNetworkFactory<KvTypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> NetworkConnection {
        let addr = node.addr.clone();
        let addr = Uri::from_static(Box::leak(addr.into_boxed_str()));
        info!("create grpc client for {}", addr);
        let channel = Channel::builder(addr).connect_lazy();
        let client = FlareRaftClient::new(channel);
        NetworkConnection {
            client,
            // target,
            // target_node: node.clone(),
            shard_id: self.shard_id,
        }
    }
}

pub struct NetworkConnection {
    client: RpcClient,
    // target: NodeId,
    // target_node: BasicNode,
    shard_id: ShardId,
}

impl NetworkConnection {}

impl RaftNetwork<KvTypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<KvTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, typ::RPCError> {
        let data = client_encode(&req)?;
        let req = ByteWrapper {
            data,
            shard_id: self.shard_id,
        };
        let response = self.client.append(req).await.map_err(|e| {
            let err = FlareRpcError::new(e);
            RPCError::Network(NetworkError::new(&err))
        })?;
        let return_data = &response.into_inner().data;
        client_decode(return_data)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<KvTypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, typ::RPCError<InstallSnapshotError>> {
        let data = bincode::serde::encode_to_vec(&req, CONFIGURATION)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let req = ByteWrapper {
            data,
            shard_id: self.shard_id,
        };
        let response = self.client.snapshot(req).await.map_err(|e| {
            let err = FlareRpcError::new(e);
            RPCError::Network(NetworkError::new(&err))
        })?;
        let data = response.into_inner().data;
        let response = bincode::serde::decode_from_slice(&data[..], CONFIGURATION)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        response.0
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, typ::RPCError> {
        let data = client_encode(&req)?;
        let req = ByteWrapper {
            data,
            shard_id: self.shard_id,
        };
        let response = self.client.vote(req).await.map_err(|e| {
            let err = FlareRpcError::new(e);
            RPCError::Network(NetworkError::new(&err))
        })?;
        client_decode(&response.into_inner().data)
    }
}

const CONFIGURATION: Configuration = config::standard();
