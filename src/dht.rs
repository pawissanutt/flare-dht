use std::hash::Hash;
use std::slice::{Iter, IterMut};
use dashmap::DashMap;
use mur3::Hasher32;
use tracing::warn;


pub struct ShardedStore {
    map: DashMap<String, Vec<u8>>
}

impl ShardedStore {
    pub fn new() -> ShardedStore {
        ShardedStore{
            map: DashMap::new()
        }
    }

    pub fn get(&self, key: &String) -> Option<Vec<u8>> {
        self.map.get(key).map(|i| i.value().clone())
    }

    pub fn set(&self, key: String, value: Vec<u8>) {
        self.map.insert(key, value);
    }

    pub async fn set_async(&self, key: String, value: Vec<u8>) {
        self.map.insert(key, value);
    }

    pub fn delete(&self, key: &String) {
        self.map.remove(key);
    }

    pub fn clean(&mut self) {
        warn!("clean sharded map");
        self.map = DashMap::new()
    }

    pub fn count(&self) -> usize {
        self.map.iter().count()
    }
}

#[derive(Default, Debug)]
struct HashTopology {
    seed: u32,
    total: u32
}

pub struct LocalStore {
    topology: HashTopology,
    shards: Vec<ShardedStore>
}



impl LocalStore {
    pub fn new() -> LocalStore {
        let mut shards = Vec::with_capacity(8);
        for _ in 0..8 {
            shards.push(ShardedStore::new())
        }
        LocalStore{
            topology: HashTopology{seed:0, total: 8},
            shards
        }
    }

    fn hash(&self, key: &String) -> u32 {
        let seed = self.topology.seed;
        let mut hasher = Hasher32::with_seed(seed);
        key.hash(&mut hasher);
        hasher.finish32()
    }

    pub fn get_shard(&self, key: &String) -> &ShardedStore {
        let index = self.hash(key) % self.topology.total;
        let i = usize::try_from(index).unwrap();
        &self.shards[i]
    }

    pub fn iter_shards(&self) -> Iter<'_, ShardedStore> {
        self.shards.iter()
    }

    pub fn iter_mut_shards(&mut self) -> IterMut<'_, ShardedStore> {
        self.shards.iter_mut()
    }
}