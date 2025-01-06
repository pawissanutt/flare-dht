
use openraft::testing::StoreBuilder;
use openraft::testing::Suite;
use openraft::StorageError;

use crate::raft::generic::LocalStateMachineStore;
use crate::raft::log::MemLogStore;
use crate::NodeId;

use super::state_machine::FlareMetadataSM;
use super::MetaTypeConfig;

struct MemKVStoreBuilder {}

impl
    StoreBuilder<
        MetaTypeConfig,
        MemLogStore<MetaTypeConfig>,
        LocalStateMachineStore<FlareMetadataSM, MetaTypeConfig>,
        (),
    > for MemKVStoreBuilder
{
    async fn build(
        &self,
    ) -> Result<
        (
            (),
            MemLogStore<MetaTypeConfig>,
            LocalStateMachineStore<FlareMetadataSM, MetaTypeConfig>,
        ),
        StorageError<NodeId>,
    > {
        Ok((
            (),
            MemLogStore::default(),
            LocalStateMachineStore::default(),
        ))
    }
}

#[test]
pub fn test_mem_store() -> Result<(), StorageError<NodeId>> {
    Suite::test_all(MemKVStoreBuilder {})?;
    Ok(())
}
