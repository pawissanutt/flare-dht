#[cfg(feature = "bincode")]
pub mod bincode;
pub mod client;
mod error;
mod msg;
mod server;

pub use client::ZrpcClient;
pub use error::ZrpcError;
pub use error::ZrpcServerError;
pub use error::ZrpcSystemError;
pub use msg::MsgSerde;
pub use server::{ZrpcService, ZrpcServiceHander};

#[cfg(test)]
mod test {}

pub trait ZrpcTypeConfig: Send + Sync {
    type In: Send + Sync;
    type Out: Send + Sync;
    type Err: Send + Sync;
    type Wrapper: Send + Sync;
    type InSerde: MsgSerde<Data = Self::In>;
    type OutSerde: MsgSerde<Data = Self::Out>;
    type ErrSerde: MsgSerde<Data = Self::Wrapper>;

    fn wrap(output: ZrpcServerError<Self::Err>) -> Self::Wrapper;
    fn unwrap(wrapper: Self::Wrapper) -> ZrpcServerError<Self::Err>;
}
