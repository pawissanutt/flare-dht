use crate::proto::flare_raft_client::FlareRaftClient;
use openraft::{RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Debug, Display};
use tonic::transport::Channel;


pub mod store;
pub mod log;
pub mod network;
