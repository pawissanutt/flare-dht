use std::sync::Arc;

use openraft::testing::StoreBuilder;
use openraft::testing::Suite;
use openraft::StorageError;

use crate::raft::log::LogStore;
use crate::raft::store::StateMachineStore;
use crate::types::NodeId;
use crate::types::TypeConfig;

struct MemKVStoreBuilder {}

impl StoreBuilder<TypeConfig, LogStore<TypeConfig>, Arc<StateMachineStore>, ()> for MemKVStoreBuilder {
    async fn build(&self) -> Result<((), LogStore<TypeConfig>, Arc<StateMachineStore>), StorageError<NodeId>> {
        Ok(((), LogStore::default(), Arc::default()))
    }
}

#[test]
pub fn test_mem_store() -> Result<(), StorageError<NodeId>> {
    Suite::test_all(MemKVStoreBuilder {})?;
    Ok(())
}
