pub(crate) mod log;

use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use bincode::{config, Decode, Encode};
use openraft::{BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote};
use openraft::storage::RaftStateMachine;
use serde::{Deserialize, Serialize};
use crate::types::{NodeId, RaftRequest, RaftResponse, TypeConfig};


#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,

    pub data: Vec<u8>,
}


#[derive(Clone)]
struct HashStore(Arc<RwLock<HashMap<u64, String>>>);

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,

    /// Application data.
    pub data: DataWrapper,
}

#[derive(Encode, Decode, Serialize, Deserialize, Debug, Default, Clone)]
pub struct DataWrapper(pub(crate) BTreeMap<String, Vec<u8>>);

#[derive(Debug, Default)]
pub struct StateMachineStore {
    /// The Raft state machine.
    pub state_machine: tokio::sync::RwLock<StateMachineData>,

    /// Used in identifier for snapshot.
    ///
    /// Note that concurrently created snapshots and snapshots created on different nodes
    /// are not guaranteed to have sequential `snapshot_idx` values, but this does not matter for
    /// correctness.
    snapshot_idx: AtomicU64,

    /// The last received snapshot.
    current_snapshot: tokio::sync::RwLock<Option<StoredSnapshot>>,
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Serialize the data of the state machine.
        let state_machine = self.state_machine.read().await;
        let data = bincode::encode_to_vec(&state_machine.data, config::standard())
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        *current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}


impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>> {
        let state_machine = self.state_machine.read().await;
        Ok((state_machine.last_applied_log, state_machine.last_membership.clone()))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<NodeId>>
    where I: IntoIterator<Item = Entry<TypeConfig>> + Send {
        let mut res = Vec::new(); //No `with_capacity`; do not know `len` of iterator

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(RaftResponse { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    RaftRequest::Set { key, value } => {
                        sm.data.0.insert(key.clone(), value.clone());
                        res.push(RaftResponse {
                            value: Some(value.clone()),
                        })
                    },
                    RaftRequest::Delete { key } => {
                        sm.data.0.remove(key);
                        res.push(RaftResponse {
                            value: None
                        })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(RaftResponse { value: None })
                }
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };


        // Update the state machine.
        let updated_state_machine_data =
            bincode::decode_from_slice(&new_snapshot.data[..], config::standard())
                .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;

        let updated_state_machine = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data: updated_state_machine_data.0,
        };
        let mut state_machine = self.state_machine.write().await;
        *state_machine = updated_state_machine;

        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        // Update current snapshot.
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}
