use openraft::BasicNode;
use tonic::Status;

pub mod log;
pub mod state_machine;

pub type NodeId = u64;

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
pub enum FlareRpcError {
    #[error("Unreachable")]
    Unreachable,
    #[error("Internal `{0}`")]
    Internal(String),
    #[error("Other {0}:{1}")]
    Other(u32, String),
}

impl From<Status> for FlareRpcError {
    fn from(value: Status) -> Self {
        match value.code() {
            _ => FlareRpcError::Other(
                value.code() as u32,
                value.message().into(),
            ),
        }
    }
}

pub type RaftError<E = openraft::error::Infallible> =
    openraft::error::RaftError<NodeId, E>;
pub type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;
