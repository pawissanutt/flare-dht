use std::sync::Arc;

use crate::cluster::FlareError;
pub type ShardId = u64;
pub mod hashmap;

#[derive(
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Default, Clone,
)]
#[rkyv(compare(PartialEq), check_bytes, derive(Debug))]
pub struct ShardMetadata {
    pub id: u64,
    pub collection: String,
    pub primary: Option<u64>,
    pub replica: Vec<u64>,
}

#[async_trait::async_trait]
pub trait KvShard: Send + Sync {
    fn shard_id(&self) -> ShardId;
    async fn get(&self, key: &String) -> Option<Vec<u8>>;
    async fn set(&self, key: String, value: Vec<u8>) -> Result<(), FlareError>;
    async fn delete(&self, key: &String) -> Result<(), FlareError>;
}

pub trait ShardFactory: Send + Sync {
    fn create_shard(&self, shard_metadata: ShardMetadata) -> Arc<dyn KvShard>;
}
