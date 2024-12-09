use std::error::Error;

use anyerror::AnyError;
use flare_zrpc::{
    BincodeMsgSerde, ZrpcClient, ZrpcError, ZrpcServerError, ZrpcServiceHander,
    ZrpcTypeConfig,
};
use openraft::{
    error::{
        InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError,
    },
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    Raft, RaftNetwork, RaftNetworkFactory, RaftTypeConfig,
};
use zenoh::Session;

pub struct AppendHandler<T: RaftTypeConfig> {
    raft: Raft<T>,
}

#[async_trait::async_trait]
impl<C: RaftTypeConfig> ZrpcServiceHander for AppendHandler<C> {
    type In = AppendEntriesRequest<C>;
    type Out = AppendEntriesResponse<C::NodeId>;
    type Err = RaftError<C::NodeId>;

    async fn handle(&self, req: Self::In) -> Result<Self::Out, Self::Err> {
        self.raft.append_entries(req).await
    }
}

// struct AppendType;

impl<C: RaftTypeConfig> ZrpcTypeConfig for AppendHandler<C> {
    type In = BincodeMsgSerde<AppendEntriesRequest<C>>;

    type Out = BincodeMsgSerde<AppendEntriesResponse<C::NodeId>>;

    type Err = BincodeMsgSerde<ZrpcServerError<Self::ErrInner>>;

    type ErrInner = RaftError<C::NodeId>;
}

pub struct VoteHandler<T: RaftTypeConfig> {
    raft: Raft<T>,
}

#[async_trait::async_trait]
impl<T: RaftTypeConfig> ZrpcServiceHander for VoteHandler<T> {
    type In = VoteRequest<T::NodeId>;
    type Out = VoteResponse<T::NodeId>;
    type Err = RaftError<T::NodeId>;

    async fn handle(&self, req: Self::In) -> Result<Self::Out, Self::Err> {
        self.raft.vote(req).await
    }
}

impl<C: RaftTypeConfig> ZrpcTypeConfig for VoteHandler<C> {
    type In = BincodeMsgSerde<VoteRequest<C::NodeId>>;

    type Out = BincodeMsgSerde<VoteResponse<C::NodeId>>;

    type Err = BincodeMsgSerde<ZrpcServerError<Self::ErrInner>>;

    type ErrInner = RaftError<C::NodeId>;
}

pub struct InstallSnapshotHandler<T: RaftTypeConfig> {
    raft: Raft<T>,
}

#[async_trait::async_trait]
impl<C: RaftTypeConfig> ZrpcServiceHander for InstallSnapshotHandler<C> {
    type In = InstallSnapshotRequest<C>;
    type Out = InstallSnapshotResponse<C::NodeId>;
    type Err = RaftError<C::NodeId, InstallSnapshotError>;

    async fn handle(&self, req: Self::In) -> Result<Self::Out, Self::Err> {
        self.raft.install_snapshot(req).await
    }
}

impl<C: RaftTypeConfig> ZrpcTypeConfig for InstallSnapshotHandler<C> {
    type In = BincodeMsgSerde<InstallSnapshotRequest<C>>;

    type Out = BincodeMsgSerde<InstallSnapshotResponse<C::NodeId>>;

    type Err = BincodeMsgSerde<ZrpcServerError<Self::ErrInner>>;

    type ErrInner = RaftError<C::NodeId, InstallSnapshotError>;
}

#[allow(type_alias_bounds)]
pub type AppendService<C: RaftTypeConfig> =
    flare_zrpc::ZrpcService<AppendHandler<C>, AppendHandler<C>>;
#[allow(type_alias_bounds)]
pub type VoteService<C: RaftTypeConfig> =
    flare_zrpc::ZrpcService<VoteHandler<C>, VoteHandler<C>>;
#[allow(type_alias_bounds)]
pub type SnapshotService<C: RaftTypeConfig> = flare_zrpc::ZrpcService<
    InstallSnapshotHandler<C>,
    InstallSnapshotHandler<C>,
>;

pub struct RaftZrpcService<C: RaftTypeConfig> {
    append: AppendService<C>,
    vote: VoteService<C>,
    snapshot: SnapshotService<C>,
}

impl<C: RaftTypeConfig> RaftZrpcService<C> {
    pub fn new(
        raft: Raft<C>,
        z_session: Session,
        rpc_prefix: String,
        node_id: C::NodeId,
    ) -> Self {
        let append = AppendService::new(
            format!("{rpc_prefix}/{node_id}/append"),
            z_session.clone(),
            AppendHandler { raft: raft.clone() },
        );
        let vote = VoteService::new(
            format!("{rpc_prefix}/{node_id}/vote"),
            z_session.clone(),
            VoteHandler { raft: raft.clone() },
        );
        let snapshot = SnapshotService::new(
            format!("{rpc_prefix}/{node_id}/snapshot"),
            z_session.clone(),
            InstallSnapshotHandler { raft: raft.clone() },
        );

        Self {
            append,
            vote,
            snapshot,
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error + Sync + Send>> {
        self.append.start().await?;
        self.vote.start().await?;
        self.snapshot.start().await?;

        Ok(())
    }

    pub fn stop(&mut self) {
        self.append.close();
        self.vote.close();
        self.snapshot.close();
    }
}

pub struct Network {
    z_session: Session,
    rpc_prefix: String,
}

impl Network {
    pub fn new(z_session: Session, rpc_prefix: String) -> Self {
        Network {
            z_session,
            rpc_prefix,
        }
    }
}

impl<C: RaftTypeConfig> RaftNetworkFactory<C> for Network {
    type Network = NetworkConnection<C>;

    async fn new_client(
        &mut self,
        target: C::NodeId,
        _node: &C::Node,
    ) -> NetworkConnection<C> {
        NetworkConnection::new(
            self.z_session.clone(),
            self.rpc_prefix.clone(),
            target,
        )
    }
}

#[allow(type_alias_bounds)]
type AppendClient<C: RaftTypeConfig> = ZrpcClient<AppendHandler<C>>;

#[allow(type_alias_bounds)]
type VoteClient<C: RaftTypeConfig> = ZrpcClient<VoteHandler<C>>;

#[allow(type_alias_bounds)]
type InstallSnapshotClient<C: RaftTypeConfig> =
    ZrpcClient<InstallSnapshotHandler<C>>;

pub struct NetworkConnection<C: RaftTypeConfig> {
    target: C::NodeId,
    append_client: AppendClient<C>,
    vote_client: VoteClient<C>,
    snapshot_client: InstallSnapshotClient<C>,
}

impl<C: RaftTypeConfig> NetworkConnection<C> {
    fn new(z_session: Session, rpc_prefix: String, target: C::NodeId) -> Self {
        let append_client = AppendClient::new(
            format!("{rpc_prefix}/{target}/append"),
            z_session.clone(),
        );
        let vote_client = VoteClient::<C>::new(
            format!("{rpc_prefix}/{target}/vote"),
            z_session.clone(),
        );

        let snapshot_client = InstallSnapshotClient::new(
            format!("{rpc_prefix}/{target}/snapshot"),
            z_session.clone(),
        );
        Self {
            target,
            append_client,
            vote_client,
            snapshot_client,
        }
    }

    fn convert<E: Error + 'static>(
        &self,
        error: ZrpcError<E>,
    ) -> RPCError<C::NodeId, C::Node, E> {
        match error {
            ZrpcError::ServerError(zrpc_server_error) => {
                self.convert_server(zrpc_server_error)
            }
            err => RPCError::Network(NetworkError::from(AnyError::new(&err))),
        }
    }

    fn convert_server<E: Error + 'static>(
        &self,
        error: ZrpcServerError<E>,
    ) -> RPCError<C::NodeId, C::Node, E> {
        match error {
            ZrpcServerError::AppError(app_err) => RPCError::RemoteError(
                RemoteError::new(self.target.to_owned(), app_err),
            ),
            err => RPCError::Network(NetworkError::from(AnyError::new(&err))),
        }
    }
}

impl<C: RaftTypeConfig> RaftNetwork<C> for NetworkConnection<C> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<C::NodeId>,
        RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>,
    > {
        let res = self.append_client.call(rpc).await;
        res.map_err(|e| self.convert(e))
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<C::NodeId>,
        RPCError<
            C::NodeId,
            C::Node,
            RaftError<C::NodeId, InstallSnapshotError>,
        >,
    > {
        let res = self.snapshot_client.call(req).await;
        res.map_err(|e| self.convert(e))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<C::NodeId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<C::NodeId>,
        RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>,
    > {
        let res = self.vote_client.call(req).await;
        res.map_err(|e| self.convert(e))
    }
}
