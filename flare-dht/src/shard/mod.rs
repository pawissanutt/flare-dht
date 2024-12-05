use std::sync::Arc;
mod hashmap;
mod manager;

use crate::error::FlareError;

use bytes::Bytes;
pub use hashmap::HashMapShard;
pub use hashmap::HashMapShardFactory;
pub use manager::ShardManager;

pub type ShardId = u64;

#[derive(
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Default, Clone,
)]
#[rkyv(derive(Debug))]
pub struct ShardMetadata {
    pub id: u64,
    pub collection: String,
    pub partition_id: u16,
    pub primary: Option<u64>,
    pub replica: Vec<u64>,
    pub shard_type: String,
    pub options: hashbrown::HashMap<String, String>,
}

#[async_trait::async_trait]
pub trait KvShard: Send + Sync {
    type Key: Clone;
    type Entry: Send + Sync + Default;

    fn meta(&self) -> &ShardMetadata;

    async fn get(
        &self,
        key: &Self::Key,
    ) -> Result<Option<Self::Entry>, FlareError>;

    async fn modify<F, O>(
        &self,
        key: &Self::Key,
        f: F,
    ) -> Result<O, FlareError>
    where
        F: FnOnce(&mut Self::Entry) -> O + Send;

    async fn set(
        &self,
        key: Self::Key,
        value: Self::Entry,
    ) -> Result<(), FlareError>;

    async fn delete(&self, key: &Self::Key) -> Result<(), FlareError>;
}

pub trait ShardEntry: Send + Sync {
    fn to_vec(&self) -> Vec<u8>;
    fn from_vec(v: Vec<u8>) -> Self;
}

#[derive(Debug, Default, Clone)]
pub struct ByteEntry {
    pub rc: u16,
    pub value: Vec<u8>,
    // pub value: Bytes,
}

impl ShardEntry for ByteEntry {
    fn to_vec(&self) -> Vec<u8> {
        self.value.clone()
    }

    fn from_vec(v: Vec<u8>) -> Self {
        ByteEntry { rc: 0, value: v }
    }
}

impl From<Vec<u8>> for ByteEntry {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        ByteEntry { rc: 1, value: v }
    }
}

impl From<&Vec<u8>> for ByteEntry {
    #[inline]
    fn from(v: &Vec<u8>) -> Self {
        ByteEntry {
            rc: 1,
            value: v.clone(),
        }
    }
}

impl From<Bytes> for ByteEntry {
    #[inline]
    fn from(v: Bytes) -> Self {
        ByteEntry {
            rc: 1,
            value: v.to_vec(),
        }
    }
}

impl From<&Bytes> for ByteEntry {
    #[inline]
    fn from(v: &Bytes) -> Self {
        ByteEntry {
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
