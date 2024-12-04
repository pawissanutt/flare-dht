use std::sync::Arc;

use openraft::testing::StoreBuilder;
use openraft::testing::Suite;
use openraft::StorageError;

use crate::raft::log::MemLogStore;
use crate::NodeId;

use super::state_machine::FlareMetadataSM;
use super::store::StateMachineStore;
use super::MetaTypeConfig;

struct MemKVStoreBuilder {}

impl
    StoreBuilder<
        MetaTypeConfig,
        MemLogStore<MetaTypeConfig>,
        Arc<StateMachineStore<FlareMetadataSM>>,
        (),
    > for MemKVStoreBuilder
{
    async fn build(
        &self,
    ) -> Result<
        (
            (),
            MemLogStore<MetaTypeConfig>,
            Arc<StateMachineStore<FlareMetadataSM>>,
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
