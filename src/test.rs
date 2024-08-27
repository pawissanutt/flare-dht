use std::sync::Arc;

use openraft::testing::StoreBuilder;
use openraft::testing::Suite;
use openraft::StorageError;

use crate::kv::state_machine::KVAppStateMachine;
use crate::kv::store::StateMachineStore;
use crate::kv::KvTypeConfig;
use crate::raft::log::MemLogStore;
use crate::raft::NodeId;

struct MemKVStoreBuilder {}

impl
    StoreBuilder<
        KvTypeConfig,
        MemLogStore<KvTypeConfig>,
        Arc<StateMachineStore<KVAppStateMachine>>,
        (),
    > for MemKVStoreBuilder
{
    async fn build(
        &self,
    ) -> Result<
        (
            (),
            MemLogStore<KvTypeConfig>,
            Arc<StateMachineStore<KVAppStateMachine>>,
        ),
        StorageError<NodeId>,
    > {
        Ok(((), MemLogStore::default(), Arc::default()))
    }
}

#[test]
pub fn test_mem_store() -> Result<(), StorageError<NodeId>> {
    Suite::test_all(MemKVStoreBuilder {})?;
    Ok(())
}
