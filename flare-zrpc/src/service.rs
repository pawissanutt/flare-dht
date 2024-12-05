use std::{error::Error, marker::PhantomData, sync::Arc};

use tracing::{error, info, warn};
use zenoh::query::Query;

use crate::{msg::MsgSerde, ZrpcServerError};

#[async_trait::async_trait]
pub trait ZrpcServiceHander {
    type In;
    type Out;
    type Err;

    async fn handle(&self, req: Self::In) -> Result<Self::Out, Self::Err>;
}

#[derive(Clone)]
pub struct ZrpcService<T, I, O, EIN, EOUT>
where
    I: MsgSerde,
    O: MsgSerde,
    EOUT: MsgSerde<Data = ZrpcServerError<EIN>>,
    T: ZrpcServiceHander<In = I::Data, Out = O::Data, Err = EIN>
        + Send
        + Sync
        + 'static,
{
    service_id: String,
    z_session: zenoh::Session,
    handler: Arc<T>,
    _req_serde: PhantomData<I>,
    _resp_serde: PhantomData<O>,
    _err_serde: PhantomData<EOUT>,
}

impl<T, I, O, EIN, EOUT> ZrpcService<T, I, O, EIN, EOUT>
where
    I: MsgSerde,
    O: MsgSerde,
    EIN: Send + Sync,
    EOUT: MsgSerde<Data = ZrpcServerError<EIN>>,
    T: ZrpcServiceHander<In = I::Data, Out = O::Data, Err = EIN>
        + Send
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
            _req_serde: PhantomData,
            _resp_serde: PhantomData,
            _err_serde: PhantomData,
        }
    }

    pub async fn start(self) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let service_id = self.service_id.clone();
        info!("registering rpc on '{}'", service_id);
        let z_session = self.z_session.clone();
        let handler = self.handler.clone();
        tokio::spawn(async move {
            let queryable = z_session
                .declare_queryable(service_id.clone())
                .await
                .unwrap();
            loop {
                match queryable.recv_async().await {
                    Ok(query) => Self::handle(handler.clone(), query).await,
                    Err(err) => {
                        error!("error on queryable '{}': {}", service_id, err,);
                        break;
                    }
                }
            }
        });

        Ok(self)
    }

    async fn handle(handler: Arc<T>, query: Query) {
        if let Some(payload) = query.payload() {
            match I::from_zbyte(payload) {
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
                    let zse =
                        ZrpcServerError::DecodeError::<EIN>(err.to_string());
                    Self::write_error(zse, query).await;
                }
            }
        } else {
            warn!("receive rpc to '{}' without payload", query.key_expr());
        }
    }

    async fn write_output(out: O::Data, query: Query) {
        match O::to_zbyte(out) {
            Ok(byte) => {
                if let Err(e) = query.reply(query.key_expr(), byte).await {
                    warn!("error on replying '{}', {}", query.key_expr(), e);
                }
            }
            Err(err) => {
                let zse = ZrpcServerError::EncodeError::<EIN>(err.to_string());
                Self::write_error(zse, query).await;
            }
        }
    }

    async fn write_error(err: EOUT::Data, query: Query) {
        let bytes = EOUT::to_zbyte(err).expect("Encode error message error");
        if let Err(e) = query.reply_err(bytes).await {
            warn!("error on error replying '{}', {}", query.key_expr(), e);
        };
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
