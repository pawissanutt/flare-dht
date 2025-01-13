use std::{error::Error, marker::PhantomData, sync::Arc};

use anyerror::AnyError;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use zenoh::query::Query;

use crate::msg::MsgSerde;

use crate::{
    error::{ZrpcServerError, ZrpcSystemError},
    ZrpcTypeConfig,
};

#[async_trait::async_trait]
pub trait ZrpcServiceHander<C: ZrpcTypeConfig> {
    async fn handle(&self, req: C::In) -> Result<C::Out, C::Err>;
}

// #[derive(Clone)]
pub struct ZrpcService<T, C>
where
    C: ZrpcTypeConfig,
    T: ZrpcServiceHander<C> + Send + Sync + 'static,
{
    service_id: String,
    z_session: zenoh::Session,
    handler: Arc<T>,
    token: tokio_util::sync::CancellationToken,
    _conf: PhantomData<C>,
}

impl<T, C> Clone for ZrpcService<T, C>
where
    C: ZrpcTypeConfig,
    T: ZrpcServiceHander<C> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            service_id: self.service_id.clone(),
            z_session: self.z_session.clone(),
            handler: self.handler.clone(),
            token: self.token.clone(),
            _conf: self._conf.clone(),
        }
    }
}

impl<T, C> ZrpcService<T, C>
where
    C: ZrpcTypeConfig,
    T: ZrpcServiceHander<C> + Send + Sync + 'static,
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
        debug!("registering rpc on '{}'", service_id);
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
            match C::InSerde::from_zbyte(payload) {
                Ok(data) => Self::run_handler(handler, query, data).await,
                Err(err) => {
                    let zse = ZrpcSystemError::DecodeError(AnyError::new(&err));
                    Self::write_error(ZrpcServerError::SystemError(zse), query)
                        .await;
                }
            }
        } else {
            warn!("receive rpc to '{}' without payload", query.key_expr());
        }
    }

    async fn run_handler(handler: Arc<T>, query: Query, payload: C::In) {
        let result = handler.handle(payload).await;
        match result {
            Ok(ok) => Self::write_output(ok, query).await,
            Err(err) => {
                Self::write_error(ZrpcServerError::AppError(err), query).await
            }
        }
    }

    async fn write_output(out: C::Out, query: Query) {
        match C::OutSerde::to_zbyte(&out) {
            Ok(byte) => {
                let reply_key = query.key_expr();
                if let Err(e) = query.reply(reply_key, byte).await {
                    warn!("error on replying '{}', {}", query.key_expr(), e);
                }
            }
            Err(err) => {
                let zse = ZrpcSystemError::EncodeError(AnyError::new(&err));
                Self::write_error(ZrpcServerError::SystemError(zse), query)
                    .await;
            }
        }
    }

    async fn write_error(err: ZrpcServerError<C::Err>, query: Query) {
        let wrapper = C::wrap(err);
        let bytes = C::ErrSerde::to_zbyte(&wrapper)
            .expect("Encode error message error");
        if let Err(e) = query.reply_err(bytes).await {
            warn!("error on error replying '{}', {}", query.key_expr(), e);
        };
    }

    pub fn close(&self) {
        self.token.cancel();
    }
}
