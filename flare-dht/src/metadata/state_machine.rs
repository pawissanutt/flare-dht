use crate::raft::state_machine::AppStateMachine;
use flare_pb::CreateCollectionRequest;
use rancor::Error;
use std::collections::BTreeMap;

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
    fn create_collection(&mut self, req: &CreateCollectionRequest) -> FlareControlResponse {
        let name = &req.name;
        let shard_count = req.shard_count;
        // let shard_assignment =
        if self.collections.contains_key(name) {
            return FlareControlResponse::Rejected("collection already exist".to_string());
        }
        // let shards = Vec::with_capacity(shard_count as usize);
        let mut shard_ids = Vec::with_capacity(shard_count as usize);
        for i in 0..shard_count {
            let id = i as u64 + self.last_shard_id + 1;
            let shard_meta = ShardMetadata {
                id: id,
                collection: name.into(),
                primary: Some(req.shard_assignments[i as usize]),
                ..Default::default()
            };
            shard_ids.push(id);
            self.shards.insert(id, shard_meta);
        }
        self.last_shard_id += shard_count as u64;

        let col_meta = CollectionMetadata {
            name: name.into(),
            shard_ids: shard_ids,
            replication: 1,
            ..Default::default()
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
    CreateCollection(CreateCollectionRequest),
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
    pub seed: u32,
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
                FlareControlRequest::CreateCollection(req) => app_state.create_collection(req),
            },
            None => panic!("App state is not KVAppStateMachine"),
        }
        // todo!()
    }
}
