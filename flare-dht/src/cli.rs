use http::Uri;

#[derive(clap::Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct FlareCli {
    #[command(subcommand)]
    pub command: FlareCommands,
}

#[derive(clap::Subcommand, Clone, Debug)]
pub enum FlareCommands {
    /// Start as server
    Server(ServerArgs),
    /// Collection operation
    #[clap(aliases = &["col", "c"])]
    Collection {
        #[command(subcommand)]
        opt: CollectionOperation,
    },
}

#[derive(clap::Subcommand, Clone, Debug)]
pub enum CollectionOperation {
    #[clap(aliases = &["c"])]
    Create {
        name: String,
        #[arg(default_value_t = 1)]
        shard_count: u16,
        #[clap(flatten)]
        connection: ConnectionArgs,
    },
}

#[derive(clap::Args, Debug, Clone)]
pub struct ConnectionArgs {
    #[arg(short, long, default_value = "http://127.0.0.1:8001")]
    pub server_url: Uri,
}

#[derive(clap::Args, Debug, Clone, Default)]
pub struct ServerArgs {
    /// advertisement address
    pub addr: Option<String>,
    /// gRPC port
    #[arg(short, long, env = "FLARE_PORT", default_value = "8001")]
    pub port: u16,
    /// if start as Raft leader
    #[arg(short, long)]
    pub leader: bool,
    #[arg(long, default_value = "false")]
    pub not_server: bool,
    /// Address to join the Raft cluster
    #[arg(long)]
    pub peer_addr: Option<String>,
    /// Node ID. Randomized, if none.
    #[arg(short, long)]
    pub node_id: Option<u64>,
}

impl ServerArgs {
    pub fn get_node_id(&self) -> u64 {
        if let Some(id) = self.node_id {
            return id;
        }
        rand::random()
    }

    pub fn get_addr(&self) -> String {
        if let Some(addr) = &self.addr {
            return addr.clone();
        }
        return format!("http://127.0.0.1:{}", self.port);
    }
}
