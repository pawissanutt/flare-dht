use zenoh::key_expr::{self, KeyExpr};

use crate::{msg::MsgSerde, ZrpcError, ZrpcTypeConfig};
use std::marker::PhantomData;

pub struct ZrpcClient<C>
where
    C: ZrpcTypeConfig,
{
    key_expr: KeyExpr<'static>,
    z_session: zenoh::Session,
    _conf: PhantomData<C>,
}

impl<C> ZrpcClient<C>
where
    C: ZrpcTypeConfig,
{
    pub async fn new(service_id: String, z_session: zenoh::Session) -> Self {
        let key_expr = z_session
            .declare_keyexpr(service_id)
            .await
            .expect("Declare key_expr for zenoh");
        Self {
            key_expr,
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
            .get(self.key_expr.clone())
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
