use zenoh::{key_expr::KeyExpr, query::ConsolidationMode};

use crate::{msg::MsgSerde, ZrpcError};
use std::marker::PhantomData;

use super::ZrpcTypeConfig;

#[derive(Clone, Debug)]
pub struct ZrpcClientConfig {
    pub service_id: String,
    pub target: zenoh::query::QueryTarget,
}

#[derive(Clone)]
pub struct ZrpcClient<C>
where
    C: ZrpcTypeConfig,
{
    key_expr: KeyExpr<'static>,
    z_session: zenoh::Session,
    config: ZrpcClientConfig,
    _conf: PhantomData<C>,
}

impl<C> ZrpcClient<C>
where
    C: ZrpcTypeConfig,
{
    pub async fn new(service_id: String, z_session: zenoh::Session) -> Self {
        let key_expr = z_session
            .declare_keyexpr(service_id.clone())
            .await
            .expect("Declare key_expr for zenoh");
        Self {
            key_expr,
            z_session,
            config: ZrpcClientConfig {
                service_id,
                target: zenoh::query::QueryTarget::BestMatching,
            },
            _conf: PhantomData,
        }
    }

    pub async fn with_config(
        config: ZrpcClientConfig,
        z_session: zenoh::Session,
    ) -> Self {
        let key_expr = z_session
            .declare_keyexpr(config.service_id.clone())
            .await
            .expect("Declare key_expr for zenoh");
        Self {
            key_expr,
            z_session,
            config,
            _conf: PhantomData,
        }
    }

    pub async fn call(
        &self,
        payload: &C::In,
    ) -> Result<C::Out, ZrpcError<C::Err>> {
        let byte = C::InSerde::to_zbyte(&payload)
            .map_err(|e| ZrpcError::EncodeError(e))?;

        let (tx, rx) = flume::bounded(1);

        let get_result = self
            .z_session
            .get(self.key_expr.clone())
            .payload(byte)
            .target(self.config.target)
            .consolidation(ConsolidationMode::None)
            .congestion_control(zenoh::qos::CongestionControl::Block)
            .with((tx, rx))
            // .callback(move |s| {
            //     let _ = tx.send(s);
            // })
            .await?;
        let reply = get_result.recv_async().await?;
        match reply.result() {
            Ok(sample) => {
                let res = C::OutSerde::from_zbyte(sample.payload())
                    .map_err(|e| ZrpcError::DecodeError(e))?;
                Ok(res)
            }
            Err(err) => {
                let wrapper = C::ErrSerde::from_zbyte(err.payload())
                    .map_err(|e| ZrpcError::DecodeError(e))?;
                let zrpc_server_error = C::unwrap(wrapper);
                let err = match zrpc_server_error {
                    super::ZrpcServerError::AppError(app_err) => {
                        ZrpcError::AppError(app_err)
                    }
                    super::ZrpcServerError::SystemError(zrpc_system_error) => {
                        ZrpcError::ServerSystemError(zrpc_system_error)
                    }
                };
                Err(err)
            }
        }
    }

    pub async fn call_with_key(
        &self,
        key: String,
        payload: &C::In,
    ) -> Result<C::Out, ZrpcError<C::Err>> {
        let byte = C::InSerde::to_zbyte(&payload)
            .map_err(|e| ZrpcError::EncodeError(e))?;

        let chan = flume::bounded(1);

        let get_result = self
            .z_session
            .get(self.key_expr.join(&key).unwrap())
            .payload(byte)
            .target(self.config.target)
            .consolidation(ConsolidationMode::None)
            .congestion_control(zenoh::qos::CongestionControl::Block)
            .with(chan)
            .await?;
        let reply = get_result.recv_async().await?;
        match reply.result() {
            Ok(sample) => {
                let res = C::OutSerde::from_zbyte(sample.payload())
                    .map_err(|e| ZrpcError::DecodeError(e))?;
                Ok(res)
            }
            Err(err) => {
                let wrapper = C::ErrSerde::from_zbyte(err.payload())
                    .map_err(|e| ZrpcError::DecodeError(e))?;
                let zrpc_server_error = C::unwrap(wrapper);
                let err = match zrpc_server_error {
                    super::ZrpcServerError::AppError(app_err) => {
                        ZrpcError::AppError(app_err)
                    }
                    super::ZrpcServerError::SystemError(zrpc_system_error) => {
                        ZrpcError::ServerSystemError(zrpc_system_error)
                    }
                };
                Err(err)
            }
        }
    }
}
