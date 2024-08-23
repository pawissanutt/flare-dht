use bincode::config::{self, Configuration};
use serde::{de::DeserializeOwned, Serialize};
use tonic::Status;
use crate::proto::ByteWrapper;

const CONFIGURATION: Configuration = config::standard();

pub fn decode<T: DeserializeOwned>(wrapper: &Vec<u8>) -> Result<T, Status> {
    let result =
        bincode::serde::decode_from_slice::<T,Configuration>(&wrapper[..], CONFIGURATION)
        // serde_json::from_slice(&wrapper)
        .map_err(|_e| { Status::invalid_argument("decode error") })
        .map(|i| i.0)
        ;
    result
}

pub fn encode<T: Serialize>(data: &T) -> Result<ByteWrapper, Status> {
    let result = bincode::serde::encode_to_vec(&data, CONFIGURATION)
    // let result = serde_json::to_vec(data)
        .map_err(|e| Status::internal(e.to_string()))
        .map(|v| ByteWrapper{shard_id: 0, data: v});
    result
}