use dashmap::DashMap;

use crate::{cluster::FlareError, metadata::state_machine::ShardMetadata};

use super::{KvShard, ShardId};

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct HashMapShard {
    pub shard_metadata: ShardMetadata,
    pub map: DashMap<String, Vec<u8>>,
}

impl HashMapShard {
    pub fn shard_id(&self) -> ShardId {
        self.shard_metadata.id
    }
}

impl KvShard for HashMapShard {
    async fn get(&self, key: &String) -> Option<Vec<u8>> {
        let out = self.map.get(key);
        out.map(|r| r.clone())
    }

    async fn set(&self, key: String, value: Vec<u8>) -> Result<(), FlareError> {
        self.map.insert(key, value);
        Ok(())
    }

    async fn delete(&self, key: &String) -> Result<(), FlareError> {
        self.map.remove(key);
        Ok(())
    }
}
