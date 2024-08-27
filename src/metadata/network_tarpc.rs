use std::error::Error;

use crate::kv::typ;
use crate::raft::NodeId;
use crate::rpc_server::raft2::FlareTarpcError::{RaftError, ShardNotFound};
use crate::rpc_server::raft2::{FlareTarpcError, RaftRpcClient};
use crate::shard::ShardId;
use openraft::error::{
    Infallible, InstallSnapshotError, NetworkError, RPCError, RemoteError, Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use tarpc::client::{Config, RpcError};
use tarpc::context;
use tarpc::tokio_serde::formats::Bincode;
use tonic::transport::Channel;

use super::MetaTypeConfig;

#[derive(Default)]
pub struct TarpcNetwork {}

impl TarpcNetwork {}

impl RaftNetworkFactory<MetaTypeConfig> for TarpcNetwork {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> NetworkConnection {
        NetworkConnection {
            shard_id: 0,
            client: None,
            node_id: target,
            addr: node.addr.to_owned(),
        }
    }
}

pub struct NetworkConnection {
    addr: String,
    client: Option<RaftRpcClient>,
    node_id: NodeId,
    shard_id: ShardId,
}

impl NetworkConnection {
    async fn new_client<E: Error>(&mut self) -> Result<(), typ::RPCError<E>> {
        let mut transport =
            tarpc::serde_transport::tcp::connect(self.addr.clone(), Bincode::default);
        let conf = transport.config_mut();
        conf.max_frame_length(usize::MAX);
        let t = transport
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        self.client = Some(RaftRpcClient::new(Config::default(), t).spawn());

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum FlareError {
    #[error("Client is unintialized")]
    ClientError,
}

impl RaftNetwork<MetaTypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, typ::RPCError> {
        if let None = &self.client {
            self.new_client().await?;
        }
        if let Some(client) = &self.client {
            let resp = client
                .append_entries(context::current(), req)
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            let out = resp.map_err(|e| match e {
                RaftError(re) => RPCError::RemoteError(RemoteError::new(self.node_id, re)),
                ShardNotFound => RPCError::Network(NetworkError::new(
                    &FlareTarpcError::ShardNotFound::<Infallible>,
                )),
            });
            return out;
        }
        Err(RPCError::Network(NetworkError::new(
            &FlareError::ClientError,
        )))
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, typ::RPCError<InstallSnapshotError>> {
        if let None = &self.client {
            self.new_client().await?;
        }
        if let Some(client) = &self.client {
            let resp = client
                .install_snapshot(context::current(), req)
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            let out = resp.map_err(|e| match e {
                RaftError(re) => RPCError::RemoteError(RemoteError::new(self.node_id, re)),
                ShardNotFound => RPCError::Network(NetworkError::new(
                    &FlareTarpcError::ShardNotFound::<Infallible>,
                )),
            });
            return out;
        }
        Err(RPCError::Network(NetworkError::new(
            &FlareError::ClientError,
        )))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, typ::RPCError> {
        if let None = &self.client {
            self.new_client().await?;
        }
        if let Some(client) = &self.client {
            let resp = client
                .vote(context::current(), req)
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            let out = resp.map_err(|e| match e {
                RaftError(re) => RPCError::RemoteError(RemoteError::new(self.node_id, re)),
                ShardNotFound => RPCError::Network(NetworkError::new(
                    &FlareTarpcError::ShardNotFound::<Infallible>,
                )),
            });
            return out;
        }
        Err(RPCError::Network(NetworkError::new(
            &FlareError::ClientError,
        )))
    }
}
