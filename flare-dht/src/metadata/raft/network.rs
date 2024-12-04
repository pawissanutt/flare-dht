use crate::raft::FlareRpcError;
use crate::raft::{client_decode, client_encode};
use crate::NodeId;
use bincode::config;
use bincode::config::Configuration;
use flare_pb::flare_metadata_raft_client::FlareMetadataRaftClient;
use flare_pb::ByteWrapper;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};

use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use tonic::transport::{Channel, Uri};
use tracing::info;

use super::typ;
use super::MetaTypeConfig;

type RpcClient = FlareMetadataRaftClient<Channel>;

pub struct Network {}

impl Network {
    pub fn new() -> Self {
        Network {}
    }
}

impl RaftNetworkFactory<MetaTypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(
        &mut self,
        _target: NodeId,
        node: &BasicNode,
    ) -> NetworkConnection {
        let addr = node.addr.clone();
        let addr = Uri::from_static(Box::leak(addr.into_boxed_str()));
        info!("create grpc client for {}", addr);
        let channel = Channel::builder(addr).connect_lazy();
        let client = FlareMetadataRaftClient::new(channel);
        NetworkConnection { client }
    }
}

pub struct NetworkConnection {
    client: RpcClient,
}

impl NetworkConnection {}

// fn map_status<E: rancor::StdError>(status: Status) -> typ::RPCError<E> {
//     let err = FlareRpcError::new(status);
//     if
//     RPCError::Network(NetworkError::new(&err))
// }

impl RaftNetwork<MetaTypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, typ::RPCError> {
        let data = client_encode(&req)?;
        let req = ByteWrapper { data };
        let response = self.client.append(req).await.map_err(|e| {
            let err = FlareRpcError::from(e);
            RPCError::Network(NetworkError::new(&err))
        })?;
        let return_data = &response.into_inner().data;
        client_decode(return_data)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        typ::RPCError<InstallSnapshotError>,
    > {
        let data = bincode::serde::encode_to_vec(&req, CONFIGURATION)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let req = ByteWrapper { data };
        let response = self.client.snapshot(req).await.map_err(|e| {
            let err = FlareRpcError::from(e);
            RPCError::Network(NetworkError::new(&err))
        })?;
        let data = response.into_inner().data;
        let response =
            bincode::serde::decode_from_slice(&data[..], CONFIGURATION)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        response.0
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, typ::RPCError> {
        let data = client_encode(&req)?;
        let req = ByteWrapper { data };
        let response = self.client.vote(req).await.map_err(|e| {
            let err = FlareRpcError::from(e);
            RPCError::Network(NetworkError::new(&err))
        })?;
        client_decode(&response.into_inner().data)
    }
}

const CONFIGURATION: Configuration = config::standard();
