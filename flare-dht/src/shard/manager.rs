use std::sync::Arc;

use scc::HashMap;

use crate::error::FlareError;

use super::{KvShard, ShardFactory, ShardId, ShardMetadata};

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
            .map(|shard| shard.get().to_owned())
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
            self.create_shard(s.to_owned());
        }
    }
}
