use crate::{msg::MsgSerde, ZrpcError, ZrpcServerError, ZrpcTypeConfig};
use std::marker::PhantomData;

pub struct ZrpcClient<C>
where
    C: ZrpcTypeConfig,
{
    service_id: String,
    z_session: zenoh::Session,
    _conf: PhantomData<C>,
}

impl<C> ZrpcClient<C>
where
    C: ZrpcTypeConfig,
{
    pub fn new(service_id: String, z_session: zenoh::Session) -> Self {
        Self {
            service_id,
            z_session,
            _conf: PhantomData,
        }
    }

    pub async fn call(
        &self,
        payload: <C::In as MsgSerde>::Data,
    ) -> Result<<C::Out as MsgSerde>::Data, ZrpcError<C::ErrInner>> {
        let get_result = self
            .z_session
            .get(self.service_id.clone())
            .target(zenoh::query::QueryTarget::BestMatching)
            .payload(<C::In as MsgSerde>::to_zbyte(payload)?)
            .await?;
        let reply = get_result.recv_async().await?;
        match reply.result() {
            Ok(sample) => {
                let res = <C::Out as MsgSerde>::from_zbyte(sample.payload())?;
                Ok(res)
            }
            Err(err) => {
                let e = <C::Err as MsgSerde>::from_zbyte(err.payload())?;
                Err(ZrpcError::ServerError(e))
            }
        }
    }
}
