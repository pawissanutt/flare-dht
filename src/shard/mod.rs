use std::error::Error;

pub type ShardId = u64;

pub mod hashmap;

pub trait KvShard : Sized {
    fn get(&self, key: &String) -> impl std::future::Future<Output = Option<Vec<u8>>> + Send;
    fn set(&self, key: String, value: Vec<u8>) -> impl std::future::Future<Output = Result<(), Box<dyn Error>>> + Send;
    fn delete(&self, key: &String) -> impl std::future::Future<Output = Result<(), Box<dyn Error>>> + Send;
}
