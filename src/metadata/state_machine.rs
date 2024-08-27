use crate::raft::state_machine::{AppStateMachine, GenericStateMachineData, RaftCommand};
use rancor::Error;
use std::{any::Any, collections::BTreeMap};

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Default, Clone)]
#[rkyv(compare(PartialEq), check_bytes, derive(Debug))]
pub struct FlareMetadataSM {
    pub collections: BTreeMap<String, CollectionMetadata>,
    pub shards: BTreeMap<u64, ShardMetadata>,
    pub last_shard_id: u64,
}

impl AppStateMachine for FlareMetadataSM {
    #[inline]
    fn load(data: &[u8]) -> Result<Self, Error> {
        rkyv::api::high::from_bytes::<Self, Error>(data)
    }

    #[inline]
    fn to_vec(&self) -> Result<Vec<u8>, Error> {
        rkyv::api::high::to_bytes_in::<_, Error>(self, Vec::new())
    }
}

impl FlareMetadataSM {
    fn create_collection(&mut self, name: &str, shard_count: u32) -> FlareControlResponse {
        if self.collections.contains_key(name) {
            return FlareControlResponse::Rejected("collection already exist".to_string());
        }
        // let shards = Vec::with_capacity(shard_count as usize);
        let mut shard_ids = Vec::with_capacity(shard_count as usize);
        for i in (self.last_shard_id + 1)..=(shard_count as u64) {
            let shard_meta = ShardMetadata {
                id: i,
                collection: name.into(),
                ..Default::default()
            };
            shard_ids.push(i);
            self.shards.insert(i, shard_meta);
        }
        self.last_shard_id += shard_count as u64;

        let col_meta = CollectionMetadata {
            name: name.into(),
            shard_ids: shard_ids,
            replication: 1,
        };
        self.collections.insert(name.into(), col_meta.clone());
        FlareControlResponse::CollectionCreated { meta: col_meta }
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Default, Clone)]
#[rkyv(compare(PartialEq), check_bytes, derive(Debug))]
pub struct ShardMetadata {
    pub id: u64,
    pub collection: String,
    pub primary: Option<u64>,
    pub replica: Vec<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum FlareControlRequest {
    CreateCollection { name: String, shard_count: u32 },
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum FlareControlResponse {
    CollectionCreated { meta: CollectionMetadata },
    Rejected(String),
    Empty,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Default,
    Clone,
)]
#[rkyv(compare(PartialEq), check_bytes, derive(Debug))]
pub struct CollectionMetadata {
    pub name: String,
    pub shard_ids: Vec<u64>,
    pub replication: u8,
}

impl<A: crate::raft::state_machine::AppStateMachine> crate::raft::state_machine::RaftCommand<A>
    for FlareControlRequest
{
    type Response = FlareControlResponse;

    fn execute(
        &self,
        state: &mut crate::raft::state_machine::GenericStateMachineData<A>,
    ) -> FlareControlResponse {
        let app_data = &mut state.app_data;
        let value_any = app_data as &mut dyn std::any::Any;
        match value_any.downcast_mut::<FlareMetadataSM>() {
            Some(app_state) => match self {
                FlareControlRequest::CreateCollection { name, shard_count } => {
                    app_state.create_collection(name, *shard_count)
                }
            },
            None => panic!("App state is not KVAppStateMachine"),
        }
    }
}
