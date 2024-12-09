use std::error::Error;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum Infallible {}

#[derive(thiserror::Error, Debug)]
pub enum ZrpcError<E = Infallible> {
    // #[error("reply error: {0}")]
    // ReplyError(#[from] ReplyError),
    #[error("connection error: {0}")]
    ConnectionError(#[from] Box<dyn Error + Send + Sync>),
    #[error("encode error: {0}")]
    EncodeError(#[from] bincode::error::EncodeError),
    #[error("decode error: {0}")]
    DecodeError(#[from] bincode::error::DecodeError),
    #[error("server error: {0}")]
    ServerError(ZrpcServerError<E>),
}

#[derive(
    thiserror::Error, serde::Serialize, serde::Deserialize, Clone, Debug,
)]
pub enum ZrpcServerError<E> {
    #[error("app error: {0}")]
    AppError(E),
    #[error("decode error: {0}")]
    DecodeError(anyerror::AnyError),
    #[error("encode error: {0}")]
    EncodeError(anyerror::AnyError),
}
