use std::sync::Arc;

use crate::error::FlareError;
pub type ShardId = u64;
mod hashmap;
// mod remote;

use bytes::Bytes;
pub use hashmap::HashMapShard;
pub use hashmap::HashMapShardFactory;
use scc::HashMap;

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
    type Entry: BaseEntry;

    fn meta(&self) -> &ShardMetadata;

    async fn get(
        &self,
        key: &String,
    ) -> Result<Option<Self::Entry>, FlareError>;

    async fn set(
        &self,
        key: String,
        value: Self::Entry,
    ) -> Result<(), FlareError>;

    async fn delete(&self, key: &String) -> Result<(), FlareError>;
}

pub trait BaseEntry: Send + Sync {
    fn to_vec(&self) -> Vec<u8>;
    fn from_vec(v: Vec<u8>) -> Self;
}

#[derive(Debug, Default, Clone)]
pub struct ShardEntry {
    pub rc: u16,
    pub value: Vec<u8>,
    // pub value: Bytes,
}

impl BaseEntry for ShardEntry {
    fn to_vec(&self) -> Vec<u8> {
        self.value.clone()
    }

    fn from_vec(v: Vec<u8>) -> Self {
        ShardEntry { rc: 0, value: v }
    }
}

impl From<Vec<u8>> for ShardEntry {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        ShardEntry { rc: 1, value: v }
    }
}

impl From<&Vec<u8>> for ShardEntry {
    #[inline]
    fn from(v: &Vec<u8>) -> Self {
        ShardEntry {
            rc: 1,
            value: v.clone(),
        }
    }
}

impl From<Bytes> for ShardEntry {
    #[inline]
    fn from(v: Bytes) -> Self {
        ShardEntry {
            rc: 1,
            value: v.to_vec(),
        }
    }
}

impl From<&Bytes> for ShardEntry {
    #[inline]
    fn from(v: &Bytes) -> Self {
        ShardEntry {
            rc: 1,
            value: v.to_vec(),
        }
    }
}

pub trait ShardFactory<T>: Send + Sync
where
    T: KvShard,
{
    fn create_shard(&self, shard_metadata: ShardMetadata) -> Arc<T>;
}

#[allow(dead_code)]
pub struct ShardManager<T>
where
    T: KvShard,
{
    pub shard_factory: Box<dyn ShardFactory<T>>,
    pub shards: HashMap<ShardId, Arc<T>>,
}

impl<T> ShardManager<T>
where
    T: KvShard,
{
    pub fn new(shard_factory: Box<dyn ShardFactory<T>>) -> Self {
        Self {
            shards: HashMap::new(),
            shard_factory,
        }
    }

    #[inline]
    pub fn get_shard(&self, shard_id: ShardId) -> Result<Arc<T>, FlareError> {
        self.shards
            .get(&shard_id)
            .map(|shard| shard.get().clone())
            .ok_or_else(|| FlareError::NoShardFound(shard_id))
    }

    #[inline]
    pub fn create_shard(&self, shard_metadata: ShardMetadata) {
        let shard = self.shard_factory.create_shard(shard_metadata);
        let shard_id = shard.meta().id;
        self.shards.upsert(shard_id, shard);
    }

    #[inline]
    pub fn contains(&self, shard_id: ShardId) -> bool {
        self.shards.contains(&shard_id)
    }

    pub fn sync_shards(&self, shard_meta: &Vec<ShardMetadata>) {
        for s in shard_meta {
            if self.contains(s.id) {
                continue;
            }
            self.create_shard(s.clone());
        }
    }
}
