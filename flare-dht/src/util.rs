use crate::raft::RPCError;
use bincode::config::{self, Configuration};
use flare_pb::ByteWrapper;
use openraft::error::NetworkError;
use serde::{de::DeserializeOwned, Serialize};
use tonic::Status;

const CONFIGURATION: Configuration = config::standard();

pub fn server_decode<T: DeserializeOwned>(wrapper: &Vec<u8>) -> Result<T, Status> {
    let result = bincode::serde::decode_from_slice::<T, Configuration>(&wrapper[..], CONFIGURATION)
        // serde_json::from_slice(&wrapper)
        .map_err(|_e| Status::invalid_argument("decode error"))
        .map(|i| i.0);
    result
}

pub fn server_encode<T: Serialize>(data: &T) -> Result<ByteWrapper, Status> {
    let result = bincode::serde::encode_to_vec(&data, CONFIGURATION)
        // let result = serde_json::to_vec(data)
        .map_err(|e| Status::internal(e.to_string()))
        .map(|v| ByteWrapper { data: v });
    result
}

pub fn client_decode<T: DeserializeOwned>(data: &Vec<u8>) -> Result<T, RPCError> {
    let result = bincode::serde::decode_from_slice::<T, Configuration>(&data[..], CONFIGURATION)
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
