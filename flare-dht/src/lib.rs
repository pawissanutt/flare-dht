pub use cluster::FlareNode;

pub mod cli;
pub mod cluster;
pub mod error;
pub mod metadata;
pub mod pool;
#[cfg(feature = "raft")]
pub mod raft;
#[cfg(feature = "rpc-server")]
pub mod rpc_server;
pub mod shard;

pub use flare_pb as proto;

pub type NodeId = u64;
