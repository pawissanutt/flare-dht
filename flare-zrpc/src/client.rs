use crate::{msg::MsgSerde, ZrpcError, ZrpcServerError};
use std::marker::PhantomData;

pub struct ZrpcClient<I, O, EIN, EOUT>
where
    I: MsgSerde,
    O: MsgSerde,
    EIN: Send + Sync,
    EOUT: MsgSerde<Data = ZrpcServerError<EIN>>,
{
    service_id: String,
    z_session: zenoh::Session,
    _req_serde: PhantomData<I>,
    _resp_serde: PhantomData<O>,
    _err_serde: PhantomData<EOUT>,
}

impl<I, O, EIN, EOUT> ZrpcClient<I, O, EIN, EOUT>
where
    I: MsgSerde,
    O: MsgSerde,
    EIN: Send + Sync,
    EOUT: MsgSerde<Data = ZrpcServerError<EIN>>,
{
    pub fn new(service_id: String, z_session: zenoh::Session) -> Self {
        Self {
            service_id,
            z_session,
            _req_serde: PhantomData,
            _err_serde: PhantomData,
            _resp_serde: PhantomData,
        }
    }

    pub async fn call(
        &self,
        payload: I::Data,
    ) -> Result<O::Data, ZrpcError<EIN>> {
        let get_result = self
            .z_session
            .get(self.service_id.clone())
            .payload(I::to_zbyte(payload)?)
            .await?;
        let reply = get_result.recv_async().await?;
        match reply.result() {
            Ok(sample) => {
                let res = O::from_zbyte(sample.payload())?;
                Ok(res)
            }
            Err(err) => {
                let e = EOUT::from_zbyte(err.payload())?;
                Err(ZrpcError::ServerError(e))
            }
        }
    }
}
