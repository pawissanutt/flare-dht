use clap::Parser;

mod cluster;
mod kv;
mod metadata;
mod raft;
mod rpc_server;
mod shard;
#[cfg(test)]
mod test;
mod util;

pub mod proto {
    tonic::include_proto!("flare"); // The string specified here must match the proto package name
}

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct FlareOptions {
    pub addr: Option<String>,
    #[arg(short, long, default_value = "8001")]
    pub port: u16,
    #[arg(short, long, default_value = "18001")]
    pub raft_port: u16,
    #[arg(short, long)]
    pub leader: bool,
    #[arg(long)]
    pub peer_addr: Option<String>,
    #[arg(short, long)]
    pub node_id: Option<u64>,
}

impl FlareOptions {
    pub fn get_node_id(&self) -> u64 {
        if let Some(id) = self.node_id {
            id
        } else {
            rand::random()
        }
    }

    pub fn get_peer_addr(&self) -> String {
        if let Some(addr) = &self.addr {
            addr.clone()
        } else {
            // format!("http://127.0.0.1:{}", self.port)
            format!("127.0.0.1:{}", self.raft_port)
        }
    }
}
