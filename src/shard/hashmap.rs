use dashmap::DashMap;

use super::{KvShard, ShardId};


#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct HashMapShard {
    pub shard_id: ShardId,
    map: DashMap<String, Vec<u8>>,
}

impl HashMapShard {
    
}

impl KvShard for HashMapShard {
    async fn get(&self, key: &String) -> Option<Vec<u8>> {
        let out = self.map.get(key);
        out.map(|r| r.clone())
    }

    async fn set(&self, key: String, value: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        self.map.insert(key, value);
        Ok(())
    }

    async fn delete(&self, key: &String) -> Result<(), Box<dyn std::error::Error>> {
        self.map.remove(key);
        Ok(())
    }
}