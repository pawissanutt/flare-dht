use bincode::config::{self, Configuration};
use serde::{de::DeserializeOwned, Serialize};
use tonic::Status;
use crate::types::flare::ByteWrapper;

const CONFIGURATION: Configuration = config::standard();

pub fn decode<'a, T: DeserializeOwned>(wrapper: &'a Vec<u8>) -> Result<T, Status> {
    let result = bincode::serde::decode_from_slice::<T,Configuration>(&wrapper[..], CONFIGURATION)
        .map_err(|_e| { Status::invalid_argument("decode error") })
        .map(|i| i.0);
    result
}

pub fn encode<T: Serialize>(data: &T) -> Result<ByteWrapper, Status> {
    let result = bincode::serde::encode_to_vec(&data, CONFIGURATION)
        .map_err(|e| Status::internal(e.to_string()))
        .map(|v| ByteWrapper{shard_id: 0, data: v});
    result
}