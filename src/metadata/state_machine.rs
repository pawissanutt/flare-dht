use std::collections::BTreeMap;
use openraft::{BasicNode, LogId, StoredMembership};
use crate::types::NodeId;

pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,

    /// Application data.
    pub data: FlareMetadata,
}

// #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Default, Clone)]
// #[rkyv(compare(PartialEq),check_bytes,derive(Debug))]
pub struct FlareMetadata{
    pub collections: BTreeMap<String, CollectionMetadata>,
    pub shards: BTreeMap<u64, ShardMetadata>,
}

pub struct CollectionMetadata{
    name: String,
    shard_ids: Vec<u64>,
    replication: u8,
}

pub struct ShardMetadata {
    id: u64,
}


