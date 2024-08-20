use clap::Parser;

mod dht;
mod discovery;
mod network;
mod types;
mod store;
mod raft;
mod shard;
mod util;
mod cluster;


#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct FlareOptions{
    pub addr: String,
    #[arg(short, long)]
    pub leader: bool,
    #[arg(short, long)]
    pub peer_addr: String,
    
}