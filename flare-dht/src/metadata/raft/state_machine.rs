use crate::{
    metadata::CollectionMetadata, raft::generic::AppStateMachine,
    shard::ShardMetadata,
};

use anyerror::AnyError;
use flare_pb::CreateCollectionRequest;
use rancor::Error;
use std::collections::BTreeMap;

use super::{FlareControlRequest, FlareControlResponse};

#[derive(
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Default, Clone,
)]
#[rkyv(derive(Debug))]
pub struct FlareMetadataSM {
    pub collections: BTreeMap<String, CollectionMetadata>,
    pub shards: BTreeMap<u64, ShardMetadata>,
    pub last_shard_id: u64,
}

impl AppStateMachine for FlareMetadataSM {
    type Req = FlareControlRequest;
    type Resp = FlareControlResponse;

    // #[inline]
    fn load_snapshot_app(data: &[u8]) -> Result<Self, openraft::AnyError> {
        rkyv::api::high::from_bytes::<Self, Error>(data)
            .map_err(|e| AnyError::new(&e))
    }

    // #[inline]
    fn snapshot_app(&self) -> Result<Vec<u8>, openraft::AnyError> {
        rkyv::api::high::to_bytes_in::<_, Error>(self, Vec::new())
            .map_err(|e| AnyError::new(&e))
    }

    fn apply(&mut self, req: &Self::Req) -> Self::Resp {
        match req {
            FlareControlRequest::CreateCollection(req) => {
                self.create_collection(req)
            }
        }
    }
    fn empty_resp(&self) -> Self::Resp {
        FlareControlResponse::Empty
    }
}

impl FlareMetadataSM {
    fn create_collection(
        &mut self,
        req: &CreateCollectionRequest,
    ) -> FlareControlResponse {
        let name = &req.name;
        let partition_count = req.partition_count;
        // let shard_assignment =
        if self.collections.contains_key(name) {
            return FlareControlResponse::Rejected(
                "collection already exist".to_string(),
            );
        }
        // let shards = Vec::with_capacity(shard_count as usize);
        let mut shard_ids =
            Vec::with_capacity((partition_count * req.replica_count) as usize);
        for i in 0..partition_count {
            let assignment = &req.shard_assignments[i as usize];
            let mut replica_shard_ids = assignment.shard_ids.clone();
            let replica_owner_ids = assignment.replica.clone();
            if replica_shard_ids.is_empty() {
                for _ in 0..req.replica_count {
                    let id = i as u64 + self.last_shard_id + 1;
                    replica_shard_ids.push(id);
                }
            }
            for (i, shard_id) in replica_shard_ids.iter().enumerate() {
                let shard_meta = ShardMetadata {
                    id: *shard_id,
                    collection: name.into(),
                    owner: Some(replica_owner_ids[i % replica_owner_ids.len()]),
                    primary: assignment.primary,
                    replica: assignment.replica.clone(),
                    ..Default::default()
                };
                shard_ids.push(*shard_id);
                self.shards.insert(*shard_id, shard_meta);
            }
        }
        self.last_shard_id += partition_count as u64;

        let col_meta = CollectionMetadata {
            name: name.into(),
            shard_ids: shard_ids,
            replication: req.replica_count as u8,
            ..Default::default()
        };
        self.collections.insert(name.into(), col_meta.clone());
        FlareControlResponse::CollectionCreated { meta: col_meta }
    }
}

// impl<A: crate::raft::state_machine::AppStateMachine>
//     crate::raft::state_machine::RaftCommand<A> for FlareControlRequest
// {
//     type Response = FlareControlResponse;

//     fn execute(
//         &self,
//         state: &mut crate::raft::state_machine::GenericStateMachineData<A>,
//     ) -> FlareControlResponse {
//         let app_data = &mut state.app_data;
//         let value_any = app_data as &mut dyn std::any::Any;
//         match value_any.downcast_mut::<FlareMetadataSM>() {
//             Some(app_state) => match self {
//                 FlareControlRequest::CreateCollection(req) => {
//                     app_state.create_collection(req)
//                 }
//             },
//             None => {
//                 panic!("App state is not KVAppStateMachine")
//             }
//         }
//     }
// }
