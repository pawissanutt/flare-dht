use rancor::Error;
use serde::{Deserialize, Serialize};

use crate::raft::state_machine::{AppStateMachine, GenericStateMachineData, RaftCommand};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FlareKvRequest {
    Set { key: String, value: Vec<u8> },
    Delete { key: String },
}

impl<A: AppStateMachine> RaftCommand<A> for FlareKvRequest {
    type Response = FlareKvResponse;

    fn execute(&self, state: &mut GenericStateMachineData<A>) -> Self::Response {
        let app_data = &mut state.app_data;
        let value_any = app_data as &mut dyn std::any::Any;
        match value_any.downcast_mut::<KVAppStateMachine>() {
            Some(app_state) => match self {
                FlareKvRequest::Set { key, value } => {
                    app_state.0.insert(key.clone(), value.clone());
                    FlareKvResponse {
                        value: Some(value.clone()),
                    }
                }
                FlareKvRequest::Delete { key } => {
                    app_state.0.remove(key);
                    FlareKvResponse { value: None }
                }
            },
            None => panic!("App state is not KVAppStateMachine"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FlareKvResponse {
    pub value: Option<Vec<u8>>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Default, Debug, Clone)]
// #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone)]
#[rkyv(compare(PartialEq), check_bytes, derive(Debug))]
// #[rkyv(check_bytes,derive(Debug))]
pub struct KVAppStateMachine(
    // #[with(HashMap<String, Vec<u8>>)]
    pub(crate) std::collections::BTreeMap<String, Vec<u8>>, // pub(crate) hashbrown::HashMap<String, Vec<u8>>
);

impl AppStateMachine for KVAppStateMachine {
    #[inline]
    fn load(data: &[u8]) -> Result<Self, Error> {
        rkyv::api::high::from_bytes::<Self, Error>(data)
    }

    #[inline]
    fn to_vec(&self) -> Result<Vec<u8>, Error> {
        rkyv::api::high::to_bytes_in::<_, Error>(self, Vec::new())
    }
}
