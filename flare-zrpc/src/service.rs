use std::{error::Error, marker::PhantomData, sync::Arc};

use anyerror::AnyError;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use zenoh::query::Query;

use crate::{msg::MsgSerde, ZrpcServerError, ZrpcTypeConfig};

#[async_trait::async_trait]
pub trait ZrpcServiceHander {
    type In;
    type Out;
    type Err;

    async fn handle(&self, req: Self::In) -> Result<Self::Out, Self::Err>;
}

#[derive(Clone)]
pub struct ZrpcService<T, C>
where
    C: ZrpcTypeConfig,
    T: ZrpcServiceHander<
            In = <C::In as MsgSerde>::Data,
            Out = <C::Out as MsgSerde>::Data,
            Err = C::ErrInner,
        > + Send
        + Sync
        + 'static,
{
    service_id: String,
    z_session: zenoh::Session,
    handler: Arc<T>,
    token: tokio_util::sync::CancellationToken,
    _conf: PhantomData<C>,
}

impl<T, C> ZrpcService<T, C>
where
    C: ZrpcTypeConfig,
    T: ZrpcServiceHander<
            In = <C::In as MsgSerde>::Data,
            Out = <C::Out as MsgSerde>::Data,
            Err = C::ErrInner,
        > + Send
        + Sync
        + 'static,
{
    pub fn new(
        service_id: String,
        z_session: zenoh::Session,
        handler: T,
    ) -> Self {
        ZrpcService {
            service_id,
            z_session,
            handler: Arc::new(handler),
            token: CancellationToken::new(),
            _conf: PhantomData,
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let service_id = self.service_id.clone();
        info!("registering rpc on '{}'", service_id);
        let z_session = self.z_session.clone();
        let handler = self.handler.clone();
        let cloned_token = self.token.clone();
        tokio::spawn(async move {
            let queryable = z_session
                .declare_queryable(service_id.clone())
                .await
                .unwrap();
            loop {
                tokio::select! {
                    query_res = queryable.recv_async() => match query_res {
                        Ok(query) => Self::handle(handler.clone(), query).await,
                        Err(err) => {
                            error!("error on queryable '{}': {}", service_id, err,);
                            break;
                        }
                    },
                    _ = cloned_token.cancelled() => {
                        break;
                    }
                }
            }
        });
        Ok(())
    }

    async fn handle(handler: Arc<T>, query: Query) {
        if let Some(payload) = query.payload() {
            match <C::In as MsgSerde>::from_zbyte(payload) {
                Ok(data) => {
                    match handler.handle(data).await {
                        Ok(output) => {
                            Self::write_output(output, query).await;
                        }
                        Err(err) => {
                            Self::write_error(
                                ZrpcServerError::AppError(err),
                                query,
                            )
                            .await;
                        }
                    };
                }
                Err(err) => {
                    let zse = ZrpcServerError::DecodeError::<C::ErrInner>(
                        AnyError::new(&err),
                    );
                    Self::write_error(zse, query).await;
                }
            }
        } else {
            warn!("receive rpc to '{}' without payload", query.key_expr());
        }
    }

    async fn write_output(out: <C::Out as MsgSerde>::Data, query: Query) {
        match <C::Out as MsgSerde>::to_zbyte(out) {
            Ok(byte) => {
                if let Err(e) = query.reply(query.key_expr(), byte).await {
                    warn!("error on replying '{}', {}", query.key_expr(), e);
                }
            }
            Err(err) => {
                let zse = ZrpcServerError::EncodeError::<C::ErrInner>(
                    AnyError::new(&err),
                );
                Self::write_error(zse, query).await;
            }
        }
    }

    async fn write_error(err: <C::Err as MsgSerde>::Data, query: Query) {
        let bytes = C::Err::to_zbyte(err).expect("Encode error message error");
        if let Err(e) = query.reply_err(bytes).await {
            warn!("error on error replying '{}', {}", query.key_expr(), e);
        };
    }

    pub fn close(&mut self) {
        self.token.cancel();
    }
}

#[cfg(test)]
mod test {

    use super::ZrpcServiceHander;

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct TestMsg(u64);
    struct TestHandler;

    #[async_trait::async_trait]
    impl ZrpcServiceHander for TestHandler {
        type In = TestMsg;

        type Out = TestMsg;

        type Err = TestMsg;

        async fn handle(&self, _req: TestMsg) -> Result<TestMsg, TestMsg> {
            Ok(TestMsg(1))
        }
    }
}
