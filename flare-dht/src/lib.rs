pub use cluster::FlareNode;

pub mod cli;
#[cfg(feature = "cluster")]
pub mod cluster;
pub mod error;
pub mod metadata;
pub mod pool;
#[cfg(feature = "raft")]
mod raft;
#[cfg(feature = "cluster")]
pub mod rpc_server;
pub mod shard;
mod util;

pub use flare_pb as proto;

pub type NodeId = u64;
