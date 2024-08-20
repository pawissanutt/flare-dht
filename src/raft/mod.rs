use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use bincode::config::{self, Configuration};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError};
use openraft::network::RPCOption;
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tonic::{Status};
use tonic::transport::{Channel, Uri};
use crate::types::{typ, NodeId, ShardId, TypeConfig};
use crate::types::flare::ByteWrapper;
use crate::types::flare::flare_raft_client::FlareRaftClient;

type RpcClient = FlareRaftClient<Channel>;

pub struct Network {
    pub(crate) shard_id: ShardId,
}

impl Network {
    pub fn new(shard_id: ShardId) -> Self {
        Network{
            shard_id
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> NetworkConnection {
        let addr = node.addr.clone();
        let addr = Uri::from_static(Box::leak(addr.into_boxed_str()));
        let channel = Channel::builder(addr)
            .connect_lazy();
        let client = FlareRaftClient::new(channel);
        NetworkConnection {
            client,
            target,
            target_node: node.clone(),
            shard_id: self.shard_id
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FlareRpcError {
    code: u32,
}

impl FlareRpcError {
    pub fn new(status: Status) -> Self {
        FlareRpcError {
            code: status.code() as u32
        }
    }
}

impl Display for FlareRpcError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "error with code {}", self.code)
    }
}

impl Error for FlareRpcError {}


pub struct NetworkConnection {
    client: RpcClient,
    target: NodeId,
    target_node: BasicNode,
    shard_id: i64,
}

impl NetworkConnection {}


const CONFIGURATION: Configuration = config::standard();

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, typ::RPCError> {
        let data = encode(&req)?;
        let req = ByteWrapper { data, shard_id: self.shard_id };
        let response = self.client.append(req).await
            .map_err(|e| {
                let err = FlareRpcError::new(e);
                RPCError::Network(NetworkError::new(&err))
            })?;
        decode(&response.into_inner().data)?
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, typ::RPCError<InstallSnapshotError>> {
        let data = bincode::serde::encode_to_vec(&req, CONFIGURATION)
        .map_err(|e| { RPCError::Network(NetworkError::new(&e)) })?;
        let req = ByteWrapper { data, shard_id: self.shard_id };
        let response = self.client.snapshot(req).await
            .map_err(|e| {
                let err = FlareRpcError::new(e);
                RPCError::Network(NetworkError::new(&err))
            })?
            ;
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
        let data = encode(&req)?;
        let req = ByteWrapper { data, shard_id: self.shard_id };
        let response = self.client.vote(req).await
            .map_err(|e| {
                let err = FlareRpcError::new(e);
                RPCError::Network(NetworkError::new(&err))
            })?;
        decode(&response.into_inner().data)?
    }
}



pub fn decode<'a, T: DeserializeOwned>(data: &'a Vec<u8>) -> Result<T, typ::RPCError> {
    let result = bincode::serde::decode_from_slice::<T,Configuration>(&data[..], CONFIGURATION)
        .map_err(|e| { RPCError::Network(NetworkError::new(&e)) })
        .map(|i| i.0);
    result
}

pub fn encode<T: Serialize>(data: &T) -> Result<Vec<u8>, typ::RPCError> {
    let result = bincode::serde::encode_to_vec(&data, CONFIGURATION)
        .map_err(|e| { RPCError::Network(NetworkError::new(&e)) });
    result
}