use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::io::Cursor;


pub type ShardId = i64;

pub mod flare {
    tonic::include_proto!("flare"); // The string specified here must match the proto package name
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftRequest {
     Set { key: String, value: Vec<u8> },
     Delete { key: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RaftResponse {
    pub value: Option<Vec<u8>>,
}

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
);

pub type Raft = openraft::Raft<TypeConfig>;

pub type NodeId = u64;

pub mod typ {
    use crate::types::{NodeId, TypeConfig};
    use openraft::BasicNode;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

