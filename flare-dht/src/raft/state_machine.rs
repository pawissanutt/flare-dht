use openraft::{
    BasicNode, LogId, SnapshotMeta, StorageIOError, StoredMembership,
};
use rancor::Error;
use std::fmt::Debug;

use super::NodeId;

#[derive(Debug, Default, Clone)]
pub struct GenericStateMachineData<APP>
where
    APP: AppStateMachine,
{
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub app_data: APP,
}

impl<APP: AppStateMachine> GenericStateMachineData<APP> {
    pub fn load(
        meta: &SnapshotMeta<NodeId, BasicNode>,
        data: &[u8],
    ) -> Result<Self, StorageIOError<NodeId>> {
        let app = APP::load(data).map_err(|e| {
            StorageIOError::read_snapshot(Some(meta.signature()), &e)
        })?;
        let smd = GenericStateMachineData::<APP> {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            app_data: app,
        };
        Ok(smd)
    }

    pub fn to_vec(&self) -> Result<Vec<u8>, StorageIOError<NodeId>> {
        self.app_data
            .to_vec()
            .map_err(|e| StorageIOError::read_state_machine(&e))
    }

    // #[inline]
    // pub fn apply(&mut self, request: &APP::Request) -> APP::Response {
    //     self.app_data.apply(request)

    // }

    pub fn apply_command<C: RaftCommand<APP>>(
        &mut self,
        req: &C,
    ) -> C::Response {
        req.execute(self)
    }
}

pub trait AppStateMachine:
    Sized + Default + Debug + Send + Sync + 'static
{
    // type Request;
    // type Response;
    fn load(data: &[u8]) -> Result<Self, Error>;
    fn to_vec(&self) -> Result<Vec<u8>, Error>;
    // fn apply(&mut self, req: &Self::Request) -> Self::Response;
}

pub trait RaftCommand<S>
where
    S: AppStateMachine,
{
    type Response;
    fn execute(&self, state: &mut GenericStateMachineData<S>)
        -> Self::Response;
}
