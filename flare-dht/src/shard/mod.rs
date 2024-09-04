use crate::cluster::FlareError;

pub type ShardId = u64;

pub mod hashmap;

pub trait KvShard {
    fn get(&self, key: &String) -> impl std::future::Future<Output = Option<Vec<u8>>> + Send;
    fn set(
        &self,
        key: String,
        value: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), FlareError>> + Send;
    fn delete(
        &self,
        key: &String,
    ) -> impl std::future::Future<Output = Result<(), FlareError>> + Send;
}
