use flare_dht::cli::{
    CollectionOperation, FlareCli, FlareCommands, ServerArgs,
};
use flare_dht::metadata::raft::FlareMetadataManager;
use flare_dht::metadata::MetadataManager;
use flare_dht::proto::flare_control_server::FlareControlServer;
use flare_dht::proto::flare_kv_client::FlareKvClient;
use flare_dht::proto::flare_kv_server::FlareKvServer;
use flare_dht::proto::CreateCollectionRequest;
use flare_dht::rpc_server::control_api::FlareControlService;
use flare_dht::rpc_server::kv_api::FlareKvService;
use flare_dht::shard::{HashMapShard, HashMapShardFactory, ShardManager};
use flare_dht::FlareNode;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tonic::transport::Server;
use tonic::Request;
use tracing::info;

pub async fn start_server(
    options: ServerArgs,
) -> Result<Arc<FlareNode<HashMapShard>>, Box<dyn Error>> {
    info!("use option {options:?}");

    let node_id = options.get_node_id();
    info!("use node_id: {node_id}");

    let z_session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let prefix = format!("flare/{}/nodes", options.cluster_id);
    let mut metadata_manager = FlareMetadataManager::new(
        node_id,
        options.get_addr(),
        options.clone(),
        z_session.clone(),
        &prefix,
    )
    .await;
    metadata_manager.initialize().await?;
    let metadata_manager: Arc<FlareMetadataManager> =
        Arc::new(metadata_manager);
    let shard_manager =
        Arc::new(ShardManager::new(Box::new(HashMapShardFactory {})));
    let flare_node = FlareNode::new(
        options.get_addr(),
        node_id,
        metadata_manager.clone(),
        shard_manager,
        // metadata_manager.control_pool.clone(),
        // metadata_manager.data_pool.clone(),
    )
    .await;

    let shared_node = Arc::new(flare_node);
    let flare_node = shared_node.clone();
    flare_node.start_watch_stream();
    let flare_kv = FlareKvService::new(shared_node.clone());
    let flare_control = FlareControlService::new(metadata_manager.clone());

    // let socket: SocketAddr = options.addr.parse()?;
    if !options.not_server {
        let socket = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            options.port,
        );
        info!("start on {}", socket);
        let reflection_server_v1a =
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(
                    flare_dht::proto::FILE_DESCRIPTOR_SET,
                )
                .build_v1alpha()
                .unwrap();

        let reflection_server_v1 =
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(
                    flare_dht::proto::FILE_DESCRIPTOR_SET,
                )
                .build_v1()
                .unwrap();

        if let Some(addr) = options.peer_addr {
            let flare_node = shared_node.clone();
            tokio::spawn(async move {
                let node = flare_node.clone();
                node.join(&addr).await.unwrap()
            });
        };

        tokio::spawn(async move {
            Server::builder()
                .add_service(reflection_server_v1a)
                .add_service(reflection_server_v1)
                .add_service(FlareKvServer::new(flare_kv))
                .add_service(FlareControlServer::new(flare_control))
                .serve(socket)
                .await
                .unwrap();
        });
    }

    Ok(shared_node)
}

pub async fn handle_cli(command: FlareCli) -> Result<(), Box<dyn Error>> {
    match command.command {
        FlareCommands::Server(server_args) => handle_server(server_args).await,
        FlareCommands::Collection { opt } => handle_collection(opt).await,
    }
}

async fn handle_server(server_args: ServerArgs) -> Result<(), Box<dyn Error>> {
    let flare_node = start_server(server_args).await?;

    match tokio::signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
            // we also shut down in case of error
        }
    }
    info!("starting a clean up for shutdown");
    flare_node.leave().await;
    info!("done clean up");
    Ok(())
}

async fn handle_collection(
    opt: CollectionOperation,
) -> Result<(), Box<dyn Error>> {
    info!("collection {:?}", opt);
    match opt {
        CollectionOperation::Create {
            name,
            shard_count: partitions,
            connection,
        } => {
            let mut client =
                FlareKvClient::connect(connection.server_url).await?;
            let resp = client
                .create_collection(Request::new(CreateCollectionRequest {
                    partition_count: partitions as i32,
                    name: name,
                    ..Default::default()
                }))
                .await?;
            info!("RESP: {:?}\n", resp);
        }
    }
    Ok(())
}
