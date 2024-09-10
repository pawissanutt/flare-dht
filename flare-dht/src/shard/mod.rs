use std::sync::Arc;

use crate::error::FlareError;
pub type ShardId = u64;
mod hashmap;
// mod remote;

pub use hashmap::HashMapShard;
pub use hashmap::HashMapShardFactory;

#[derive(
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Default, Clone,
)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct ShardMetadata {
    pub id: u64,
    pub collection: String,
    pub primary: Option<u64>,
    pub replica: Vec<u64>,
}

#[async_trait::async_trait]
pub trait KvShard: Send + Sync {
    fn meta(&self) -> &ShardMetadata;
    async fn get(&self, key: &String) -> Option<ShardEntry>;
    async fn set(
        &self,
        key: String,
        value: ShardEntry,
    ) -> Result<(), FlareError>;
    async fn delete(&self, key: &String) -> Result<(), FlareError>;
}

#[derive(Debug, Default, Clone)]
pub struct ShardEntry {
    pub rc: u16,
    pub value: Vec<u8>,
}

impl From<Vec<u8>> for ShardEntry {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        ShardEntry { rc: 1, value: v }
    }
}

pub trait ShardFactory: Send + Sync {
    fn create_shard(&self, shard_metadata: ShardMetadata) -> Arc<dyn KvShard>;
}
