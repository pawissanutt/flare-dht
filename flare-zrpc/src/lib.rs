mod client;
mod error;
mod msg;
mod service;

pub use client::ZrpcClient;
pub use error::ZrpcError;
pub use error::ZrpcServerError;
pub use msg::BincodeMsgSerde;
pub use msg::MsgSerde;
pub use service::ZrpcService;
pub use service::ZrpcServiceHander;

pub trait ZrpcTypeConfig: Send + Sync {
    type In: MsgSerde;
    type Out: MsgSerde;
    type Err: MsgSerde<Data = ZrpcServerError<Self::ErrInner>>;
    type ErrInner: Sync + Send;
}

#[cfg(test)]
mod test {}
