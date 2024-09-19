use std::str::FromStr;

use tonic::transport::Channel;

use crate::error::FlareInternalError;

pub struct GrpcClientManager<T>
where
    T: Send + 'static,
{
    uri: http::Uri,
    new_fn: fn(Channel) -> T,
}

impl<T> GrpcClientManager<T>
where
    T: Send + 'static,
{
    pub fn new(
        addr: &str,
        new_fn: fn(Channel) -> T,
    ) -> Result<Self, FlareInternalError> {
        let uri = http::Uri::from_str(addr)?;
        Ok(Self { uri, new_fn })
    }
}

#[async_trait::async_trait]
impl<T> mobc::Manager for GrpcClientManager<T>
where
    T: Send + 'static,
{
    type Connection = T;

    type Error = FlareInternalError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let ch = Channel::builder(self.uri.clone()).connect().await?;
        let client = (self.new_fn)(ch);
        Ok(client)
    }

    async fn check(
        &self,
        conn: Self::Connection,
    ) -> Result<Self::Connection, Self::Error> {
        Ok(conn)
    }
}
