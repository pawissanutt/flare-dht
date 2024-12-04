mod conn;
mod grpc;

use conn::{ConnFactory, ConnManager};
use flare_pb::{
    flare_control_client::FlareControlClient, flare_kv_client::FlareKvClient,
};
use grpc::GrpcClientManager;
use std::sync::Arc;
use tonic::transport::Channel;

use crate::{error::FlareInternalError, NodeId};

pub type ControlPool =
    ConnManager<NodeId, GrpcClientManager<FlareControlClient<Channel>>>;
pub type DataPool =
    ConnManager<NodeId, GrpcClientManager<FlareKvClient<Channel>>>;

pub fn create_control_pool(resolver: Arc<dyn AddrResolver>) -> ControlPool {
    let factory = Arc::new(GrpcConnFactory { resolver });
    return ConnManager::new(factory);
}

pub fn create_data_pool(resolver: Arc<dyn AddrResolver>) -> DataPool {
    let factory = Arc::new(GrpcConnFactory { resolver });
    return ConnManager::new(factory);
}

#[async_trait::async_trait]
pub trait AddrResolver: Send + Sync {
    async fn resolve(&self, node_id: NodeId) -> Option<String>;
}

struct GrpcConnFactory {
    resolver: Arc<dyn AddrResolver>,
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
        if let Some(addr) = self.resolver.resolve(node_id).await {
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
        if let Some(addr) = self.resolver.resolve(node_id).await {
            GrpcClientManager::new(&addr, FlareControlClient::new)
        } else {
            Err(FlareInternalError::NoSuchNode(node_id))
        }
    }
}
