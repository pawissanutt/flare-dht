use state_machine::{FlareKvRequest, FlareKvResponse};
pub mod network;
pub mod state_machine;
pub mod store;
use std::io::Cursor;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub KvTypeConfig:
        D = FlareKvRequest,
        R = FlareKvResponse,
);

pub type Raft = openraft::Raft<KvTypeConfig>;

#[allow(dead_code)]
pub mod typ {
    use crate::shard_raft::KvTypeConfig;
    use crate::raft::NodeId;
    use openraft::BasicNode;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> =
        openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<KvTypeConfig>;
}
