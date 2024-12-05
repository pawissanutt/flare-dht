use std::marker::PhantomData;

use bincode::error::{DecodeError, EncodeError};
use zenoh::bytes::ZBytes;


pub trait MsgSerde: Clone + Send + Sync {
    type Data: serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + Send
        + Sync;

    const BINCODE_CONFIG: bincode::config::Configuration =
        bincode::config::standard();

    fn to_zbyte(payload: Self::Data) -> Result<ZBytes, EncodeError> {
        let payload =
            bincode::serde::encode_to_vec(payload, Self::BINCODE_CONFIG)?;
        Ok(ZBytes::from(&payload[..]))
    }

    fn from_zbyte(payload: &ZBytes) -> Result<Self::Data, DecodeError> {
        let resp = bincode::serde::decode_from_slice(
            &payload.to_bytes(),
            Self::BINCODE_CONFIG,
        )
        .unwrap()
        .0;
        return Ok(resp);
    }
}

#[derive(Clone)]
pub struct AnyMsgSerde<T> {
    _data: PhantomData<T>,
}

impl<T> MsgSerde for AnyMsgSerde<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
{
    type Data = T;
}
