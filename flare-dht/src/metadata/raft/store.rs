use super::MetaTypeConfig;
use crate::metadata::raft::FlareControlResponse;
use crate::raft::state_machine::{AppStateMachine, GenericStateMachineData};
use crate::NodeId;
use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig,
    Snapshot, SnapshotMeta, StorageError, StoredMembership,
};
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,
    pub data: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct StateMachineStore<APP: AppStateMachine> {
    /// The Raft state machine.
    pub state_machine: RwLock<GenericStateMachineData<APP>>,

    /// Used in identifier for snapshot.v
    ///
    /// Note that concurrently created snapshots and snapshots created on different nodes
    /// are not guaranteed to have sequential `snapshot_idx` values, but this does not matter for
    /// correctness.
    snapshot_idx: AtomicU64,

    /// The last received snapshot.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl<APP: AppStateMachine> RaftSnapshotBuilder<MetaTypeConfig>
    for Arc<StateMachineStore<APP>>
{
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<MetaTypeConfig>, StorageError<NodeId>> {
        // Serialize the data of the state machine.
        let state_machine = self.state_machine.read().await;
        let data = state_machine.to_vec()?;

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx =
            self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
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

impl<APP: AppStateMachine> RaftStateMachine<MetaTypeConfig>
    for Arc<StateMachineStore<APP>>
{
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>),
        StorageError<NodeId>,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<FlareControlResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<MetaTypeConfig>> + Send,
    {
        let mut res: Vec<FlareControlResponse> = Vec::new(); //No `with_capacity`; do not know `len` of iterator
        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(FlareControlResponse::Empty),
                EntryPayload::Normal(ref req) => {
                    let resp = sm.apply_command(req);
                    res.push(resp);
                }
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership =
                        StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(FlareControlResponse::Empty)
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
    ) -> Result<
        Box<<MetaTypeConfig as RaftTypeConfig>::SnapshotData>,
        StorageError<NodeId>,
    > {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<MetaTypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        let updated_state_machine =
            GenericStateMachineData::load(meta, &new_snapshot.data[..])?;
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
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MetaTypeConfig>>, StorageError<NodeId>> {
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
