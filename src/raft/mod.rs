use std::fmt;

use openraft::BasicNode;
use tonic::Status;

pub mod log;
pub mod state_machine;

pub type NodeId = u64;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct FlareRpcError {
    code: u32,
    message: String,
}

impl FlareRpcError {
    pub fn new(status: Status) -> Self {
        FlareRpcError {
            code: status.code() as u32,
            message: status.message().into(),
        }
    }
}

impl std::fmt::Display for FlareRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error with code {}: {}", self.code, self.message)
    }
}

impl std::error::Error for FlareRpcError {}

pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
pub type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;
