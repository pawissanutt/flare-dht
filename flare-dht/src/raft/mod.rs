use bincode::config::Configuration;
use openraft::{error::NetworkError, BasicNode};
use serde::{de::DeserializeOwned, Serialize};
use tonic::Status;

use crate::NodeId;

pub mod log;
pub mod state_machine;

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

const CONFIGURATION: Configuration = bincode::config::standard();

pub fn client_decode<T: DeserializeOwned>(
    data: &Vec<u8>,
) -> Result<T, RPCError> {
    let result = bincode::serde::decode_from_slice::<T, Configuration>(
        &data[..],
        CONFIGURATION,
    )
    // let result = serde_json::from_slice(&data[..])
    .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    .map(|i| i.0);
    result
}

pub fn client_encode<T: Serialize>(data: &T) -> Result<Vec<u8>, RPCError> {
    let result = bincode::serde::encode_to_vec(data, CONFIGURATION)
        // let result = serde_json::to_vec(&data)
        .map_err(|e| RPCError::Network(NetworkError::new(&e)));
    result
}
