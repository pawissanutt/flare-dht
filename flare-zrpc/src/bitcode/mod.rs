use std::marker::PhantomData;

use crate::{error::ZrpcServerError, MsgSerde, ZrpcTypeConfig};
use anyerror::AnyError;
use zenoh::bytes::ZBytes;

// pub struct BincodeZrpcType<I, O, E>
// where
//     I: Send + Sync,
//     O: Send + Sync,
//     E: Send + Sync,
// {
//     _i: PhantomData<I>,
//     _o: PhantomData<O>,
//     _e: PhantomData<E>,
// }

// impl<I, O, E> ZrpcTypeConfig for BincodeZrpcType<I, O, E>
// where
//     I: Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
//     O: Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
//     E: Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
// {
//     type In = I;
//     type Out = O;
//     type Err = E;
//     type Wrapper = ZrpcServerError<E>;
//     type InSerde = BincodeMsgSerde<I>;
//     type OutSerde = BincodeMsgSerde<O>;
//     type ErrSerde = BincodeMsgSerde<Self::Wrapper>;

//     fn wrap(output: ZrpcServerError<Self::Err>) -> Self::Wrapper {
//         output
//     }

//     fn unwrap(wrapper: Self::Wrapper) -> ZrpcServerError<Self::Err> {
//         wrapper
//     }
// }

#[derive(Clone)]
pub struct BincodeMsgSerde<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    _data: PhantomData<T>,
}

impl<T> MsgSerde for BincodeMsgSerde<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    type Data = T;

    fn to_zbyte(payload: &Self::Data) -> Result<ZBytes, AnyError> {
        let payload =
            bitcode::serialize(&payload).map_err(|e| AnyError::new(&e))?;
        Ok(ZBytes::from(&payload[..]))
    }

    fn from_zbyte(payload: &ZBytes) -> Result<Self::Data, AnyError> {
        bitcode::deserialize(&payload.to_bytes()).map_err(|e| AnyError::new(&e))
    }
}
