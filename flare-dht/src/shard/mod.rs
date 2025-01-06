use std::sync::Arc;
mod hashmap;
mod manager;

use crate::error::FlareError;

use bytes::Bytes;
pub use hashmap::HashMapShard;
pub use hashmap::HashMapShardFactory;
pub use manager::ShardManager;
use scc::HashMap;

pub type ShardId = u64;

#[derive(
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Default, Clone,
)]
#[rkyv(derive(Debug))]
pub struct ShardMetadata {
    pub id: u64,
    pub collection: String,
    pub partition_id: u16,
    pub owner: Option<u64>,
    pub primary: Option<u64>,
    pub replica: Vec<u64>,
    pub shard_type: String,
    pub options: std::collections::HashMap<String, String>,
}

impl ShardMetadata {
    pub fn into_proto(&self) -> flare_pb::ShardMetadata {
        flare_pb::ShardMetadata {
            id: self.id,
            collection: self.collection.clone(),
            partition_id: self.partition_id as u32,
            owner: self.owner,
            primary: self.primary,
            replica: self.replica.clone(),
            shard_type: self.shard_type.clone(),
            options: self.options.clone(),
        }
    }
}

#[async_trait::async_trait]
pub trait KvShard: Send + Sync {
    type Key: Send + Clone;
    type Entry: Send + Sync + Default;

    fn meta(&self) -> &ShardMetadata;

    async fn initialize(&self) -> Result<(), FlareError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), FlareError> {
        Ok(())
    }

    async fn get(
        &self,
        key: &Self::Key,
    ) -> Result<Option<Self::Entry>, FlareError>;

    // async fn modify<F, O>(
    //     &self,
    //     key: &Self::Key,
    //     f: F,
    // ) -> Result<O, FlareError>
    // where
    //     F: FnOnce(&mut Self::Entry) -> O + Send;

    async fn merge(
        &self,
        key: Self::Key,
        value: Self::Entry,
    ) -> Result<Self::Entry, FlareError> {
        self.set(key.to_owned(), value).await?;
        let item = self.get(&key).await?;
        match item {
            Some(entry) => Ok(entry),
            None => Err(FlareError::InvalidArgument(
                "Merged result is None".to_string(),
            )),
        }
    }

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

pub struct ShardManager2<K, V>
where
    K: Send + Clone,
    V: Send + Sync + Default,
{
    pub shard_factory: Box<dyn ShardFactory2<Key = K, Entry = V>>,
    pub shards: HashMap<ShardId, Arc<dyn KvShard<Key = K, Entry = V>>>,
}

impl<K, V> ShardManager2<K, V>
where
    K: Send + Clone,
    V: Send + Sync + Default,
{
    pub fn new(
        shard_factory: Box<dyn ShardFactory2<Key = K, Entry = V>>,
    ) -> Self {
        Self {
            shards: HashMap::new(),
            shard_factory,
        }
    }

    #[inline]
    pub fn get_shard(
        &self,
        shard_id: ShardId,
    ) -> Result<Arc<dyn KvShard<Key = K, Entry = V>>, FlareError> {
        self.shards
            .get(&shard_id)
            .map(|shard| shard.get().to_owned())
            .ok_or_else(|| FlareError::NoShardFound(shard_id))
    }

    #[inline]
    pub async fn create_shard(&self, shard_metadata: ShardMetadata) {
        let shard = self.shard_factory.create_shard(shard_metadata).await;
        let shard_id = shard.meta().id;
        shard.initialize().await.unwrap();
        self.shards.upsert(shard_id, shard);
    }

    #[inline]
    pub fn contains(&self, shard_id: ShardId) -> bool {
        self.shards.contains(&shard_id)
    }

    pub async fn sync_shards(&self, shard_meta: &Vec<ShardMetadata>) {
        for s in shard_meta {
            if self.contains(s.id) {
                continue;
            }
            self.create_shard(s.to_owned()).await;
        }
    }

    pub async fn remove_shard(&self, shard_id: ShardId) {
        if let Some((_, v)) = self.shards.remove(&shard_id) {
            let _ = v.close().await;
        }
    }
}

pub trait ShardFactory<T>: Send + Sync
where
    T: KvShard,
{
    fn create_shard(&self, shard_metadata: ShardMetadata) -> Arc<T>;
}

#[async_trait::async_trait]
pub trait ShardFactory2: Send + Sync {
    type Key;
    type Entry;
    async fn create_shard(
        &self,
        shard_metadata: ShardMetadata,
    ) -> Arc<dyn KvShard<Key = Self::Key, Entry = Self::Entry>>;
}
