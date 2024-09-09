use std::error::Error;

use tonic::Status;

use crate::{raft::NodeId, shard::ShardId};

#[derive(thiserror::Error, Debug)]
pub enum FlareInternalError {
    #[error("Uri parsing error: {0}")]
    InvalidUrl(#[from] http::uri::InvalidUri),
    #[error("No node with id: {0}")]
    NoSuchNode(NodeId),
    #[error("No leader")]
    NoLeader,
}

#[derive(thiserror::Error, Debug)]
pub enum FlareError {
    #[error("No shard `{0}` on current node")]
    NoShardFound(ShardId),
    #[error("No collection `{0}` in cluster")]
    NoCollection(String),
    #[error("Invalid: {0}")]
    InvalidArgument(String),
    #[error("Invalid: {0}")]
    UnknownError(#[from] Box<dyn Error + Send + Sync + 'static>),
    #[error("RPC Error: {0}")]
    RpcError(#[from] Status),
    #[error("{0}")]
    InternalError(#[from] FlareInternalError),
}

impl From<FlareError> for tonic::Status {
    fn from(value: FlareError) -> Self {
        match value {
            FlareError::InvalidArgument(msg) => Status::invalid_argument(msg),
            FlareError::UnknownError(e) => Status::from_error(e),
            FlareError::InternalError(e) => Status::internal(e.to_string()),
            FlareError::RpcError(e) => e,
            _ => Status::not_found(value.to_string()),
        }
    }
}

impl From<FlareInternalError> for tonic::Status {
    fn from(value: FlareInternalError) -> Self {
        Status::internal(value.to_string())
    }
}
