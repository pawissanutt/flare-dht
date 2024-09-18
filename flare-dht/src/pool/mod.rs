mod conn;

use conn::{ConnFactory, ConnManager};
use flare_pb::{
    flare_control_client::FlareControlClient, flare_kv_client::FlareKvClient,
};
use std::{str::FromStr, sync::Arc};
use tonic::transport::Channel;

use crate::{
    error::FlareInternalError, metadata::FlareMetadataManager, raft::NodeId,
};

pub struct ClientPool {
    pub kv_pool: ConnManager<NodeId, GrpcClientManager<FlareKvClient<Channel>>>,
    pub control_pool:
        ConnManager<NodeId, GrpcClientManager<FlareControlClient<Channel>>>,
}

impl ClientPool {
    pub fn new(mm: Arc<FlareMetadataManager>) -> Self {
        let factory = Arc::new(GrpcConnFactory { mm });
        let kv_pool = ConnManager::new(factory.clone());
        let control_pool = ConnManager::new(factory);
        Self {
            kv_pool,
            control_pool,
        }
    }
}

struct GrpcConnFactory {
    mm: Arc<FlareMetadataManager>,
}

#[async_trait::async_trait]
impl ConnFactory<NodeId, GrpcClientManager<FlareKvClient<Channel>>>
    for GrpcConnFactory
{
    async fn create(
        &self,
        node_id: NodeId,
    ) -> Result<GrpcClientManager<FlareKvClient<Channel>>, FlareInternalError>
    {
        if let Some(addr) = self.mm.get_node_addr(node_id).await {
            GrpcClientManager::new(&addr, FlareKvClient::new)
        } else {
            Err(FlareInternalError::NoSuchNode(node_id))
        }
    }
}

#[async_trait::async_trait]
impl ConnFactory<NodeId, GrpcClientManager<FlareControlClient<Channel>>>
    for GrpcConnFactory
{
    async fn create(
        &self,
        node_id: NodeId,
    ) -> Result<
        GrpcClientManager<FlareControlClient<Channel>>,
        FlareInternalError,
    > {
        if let Some(addr) = self.mm.get_node_addr(node_id).await {
            GrpcClientManager::new(&addr, FlareControlClient::new)
        } else {
            Err(FlareInternalError::NoSuchNode(node_id))
        }
    }
}

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
        let ch = Channel::builder(self.uri.clone()).connect_lazy();
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
