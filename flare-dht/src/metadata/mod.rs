#[cfg(feature = "raft")]
pub mod raft;
use crate::error::FlareError;
use crate::proto::{
    ClusterMetadata, CreateCollectionRequest, CreateCollectionResponse,
    JoinRequest, JoinResponse,
};
use crate::shard::ShardMetadata;
use crate::NodeId;
use std::u32;

#[async_trait::async_trait]
pub trait MetadataManager: Send + Sync {
    async fn initialize(&self) -> Result<(), FlareError>;
    async fn get_shard_ids(&self, col_name: &str) -> Option<Vec<u64>>;
    async fn get_shard_id(&self, col_name: &str, key: &[u8]) -> Option<u64>;
    async fn leave(&self);
    async fn other_leave(&self, node_id: NodeId) -> Result<(), FlareError>;
    async fn other_join(
        &self,
        join_request: JoinRequest,
    ) -> Result<JoinResponse, FlareError>;
    async fn get_metadata(&self) -> Result<ClusterMetadata, FlareError>;

    async fn local_shards(&self) -> Vec<ShardMetadata>;
    async fn create_collection(
        &self,
        mut request: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse, FlareError>;

    fn create_watch(&self) -> tokio::sync::watch::Receiver<u64>;
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Default,
    Clone,
)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct CollectionMetadata {
    pub name: String,
    pub shard_ids: Vec<u64>,
    pub seed: u32,
    pub replication: u8,
}
