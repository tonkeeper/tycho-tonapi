use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytesize::ByteSize;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_stream::Stream;
use tycho_types::cell::HashBytes;
use tycho_types::models::{BlockIdShort, ShardIdent, StdAddr};

use crate::state::{AppState, StateError};

pub mod proto {
    tonic::include_proto!("indexer");
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GrpcConfig {
    /// gRPC server listen address.
    ///
    /// Default: `0.0.0.0:50051`
    pub listen_addr: SocketAddr,
    /// Max query/answer message size.
    ///
    /// Default: `4 MiB`
    pub max_message_size: ByteSize,
}

impl Default for GrpcConfig {
    #[inline]
    fn default() -> Self {
        Self {
            listen_addr: (Ipv4Addr::UNSPECIFIED, 50051).into(),
            max_message_size: ByteSize::mib(4),
        }
    }
}

pub async fn serve(state: AppState, config: GrpcConfig) -> anyhow::Result<()> {
    use tonic::codec::CompressionEncoding;

    let server_impl = GrpcServer::new(state.clone());

    let service = proto::tycho_indexer_server::TychoIndexerServer::new(server_impl)
        .max_encoding_message_size(config.max_message_size.as_u64() as usize)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Deflate)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Deflate)
        .send_compressed(CompressionEncoding::Zstd);

    let server = tonic::transport::Server::builder()
        .add_service(service)
        .serve(config.listen_addr);

    server.await.map_err(Into::into)
}

// === Service ===

pub struct GrpcServer {
    state: AppState,
}

impl GrpcServer {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl proto::tycho_indexer_server::TychoIndexer for GrpcServer {
    type WatchBlockIdsStream = WatchBlockIdsStream;
    type GetBlockStream = GetBlockStream;

    async fn get_status(
        &self,
        _: tonic::Request<proto::GetStatusRequest>,
    ) -> tonic::Result<tonic::Response<proto::GetStatusResponse>> {
        let status = self.state.get_status();
        Ok(tonic::Response::new(proto::GetStatusResponse {
            mc_state_info: status.mc_state_info.map(Into::into),
            timestamp: status.timestamp,
            zerostate_root_hash: status.zerostate_id.root_hash.as_slice().to_vec(),
            zerostate_file_hash: status.zerostate_id.file_hash.as_slice().to_vec(),
            init_block_seqno: status.init_block_seqno,
            version: crate::version_packed(),
        }))
    }

    async fn watch_block_ids(
        &self,
        request: tonic::Request<proto::WatchBlockIdsRequest>,
    ) -> tonic::Result<tonic::Response<Self::WatchBlockIdsStream>> {
        let res = self
            .state
            .watch_new_blocks(request.get_ref().since_mc_seqno)
            .await?;

        Ok(tonic::Response::new(WatchBlockIdsStream {
            inner: res.data,
            broken: false,
        }))
    }

    async fn get_block(
        &self,
        request: tonic::Request<proto::GetBlockRequest>,
    ) -> tonic::Result<tonic::Response<Self::GetBlockStream>> {
        use self::proto::get_block_request::Query as ProtoQueryBlock;
        use crate::state::QueryBlock;

        let query = match request.get_ref().query.require()? {
            ProtoQueryBlock::BySeqno(q) => QueryBlock::BySeqno(BlockIdShort {
                shard: parse_shard_id(q.workchain, q.shard)?,
                seqno: q.seqno,
            }),
            ProtoQueryBlock::ById(q) => QueryBlock::ById(q.id.require()?.try_into()?),
        };
        let res = self.state.get_block(&query).await?;

        Ok(tonic::Response::new(match res.data {
            Some(stream) => GetBlockStream::Found {
                mc_state_info: res.mc_state_info,
                offset: 0,
                first: true,
                stream: Some(stream),
            },
            None => GetBlockStream::NotFound {
                mc_state_info: res.mc_state_info,
                finished: false,
            },
        }))
    }

    async fn get_shard_account(
        &self,
        request: tonic::Request<proto::GetShardAccountRequest>,
    ) -> tonic::Result<tonic::Response<proto::GetShardAccountResponse>> {
        use self::proto::get_shard_account_request::AtBlock as ProtoAtBlock;
        use self::proto::get_shard_account_response::Account as ProtoAccount;
        use crate::state::AtBlock;

        let request = request.get_ref();

        let address = parse_address(request.workchain, &request.address)?;
        let at_block = match &request.at_block {
            None | Some(ProtoAtBlock::Latest(_)) => AtBlock::Latest,
            Some(ProtoAtBlock::BySeqno(q)) => AtBlock::BySeqno(BlockIdShort {
                shard: parse_shard_id(q.workchain, q.shard)?,
                seqno: q.seqno,
            }),
            Some(ProtoAtBlock::ById(q)) => AtBlock::ById(q.id.require()?.try_into()?),
        };

        let res = self
            .state
            .get_account_state(&address, request.with_proof, &at_block)
            .await?;

        let msg = match res.data {
            Some(accessed) => ProtoAccount::Accessed(proto::ShardAccount {
                mc_state_info: Some(res.mc_state_info.into()),
                account_state: accessed.account_state,
                proof: accessed.proof,
                last_transaction: (accessed.last_trans_lt != 0).then(|| proto::TransactionId {
                    lt: accessed.last_trans_lt,
                    hash: accessed.last_trans_hash.as_array().to_vec(),
                }),
            }),
            None => ProtoAccount::BlockNotFound(proto::BlockNotFound {
                mc_state_info: Some(res.mc_state_info.into()),
            }),
        };

        Ok(tonic::Response::new(proto::GetShardAccountResponse {
            account: Some(msg),
        }))
    }

    async fn get_library_cell(
        &self,
        request: tonic::Request<proto::GetLibraryCellRequest>,
    ) -> tonic::Result<tonic::Response<proto::GetLibraryCellResponse>> {
        use proto::get_library_cell_response::Library;

        let lib_hash = parse_hash_ref(&request.get_ref().hash)?;
        let res = self.state.get_library_cell(lib_hash)?;
        Ok(tonic::Response::new(proto::GetLibraryCellResponse {
            library: Some(match res.data {
                Some(cell) => Library::Found(proto::LibraryCellFound {
                    cell,
                    mc_state_info: Some(res.mc_state_info.into()),
                }),
                None => Library::NotFound(proto::LibraryCellNotFound {
                    mc_state_info: Some(res.mc_state_info.into()),
                }),
            }),
        }))
    }

    async fn get_library_cells(
        &self,
        request: tonic::Request<proto::GetLibraryCellsRequest>,
    ) -> tonic::Result<tonic::Response<proto::GetLibraryCellsResponse>> {
        let hashes = request.get_ref().hashes.as_slice();
        let max_batch_len = self.state.config().libs_max_batch_len;
        if hashes.len() > max_batch_len {
            return Err(tonic::Status::invalid_argument(format!(
                "too many libraries requested, at most {max_batch_len} is allowed"
            )));
        }

        let mut hashes = hashes
            .iter()
            .map(|item| parse_hash_ref(item))
            .collect::<Result<Vec<_>, _>>()?;
        hashes.sort_unstable();
        hashes.dedup();

        let res = self.state.get_library_cells(hashes.iter().copied())?;
        Ok(tonic::Response::new(proto::GetLibraryCellsResponse {
            mc_state_info: Some(res.mc_state_info.into()),
            entries: res
                .data
                .into_iter()
                .map(|item| proto::LibraryCellsBatchEntry {
                    hash: item.hash.as_array().to_vec(),
                    cell: item.cell,
                })
                .collect(),
        }))
    }

    async fn send_message(
        &self,
        request: tonic::Request<proto::SendMessageRequest>,
    ) -> tonic::Result<tonic::Response<proto::SendMessageResponse>> {
        let delivered_to = self
            .state
            .send_message(request.into_inner().message)
            .await?;

        Ok(tonic::Response::new(proto::SendMessageResponse {
            delivered_to: delivered_to as u32,
        }))
    }
}

// === Streams ===

pub struct WatchBlockIdsStream {
    inner: crate::state::NewBlocksStream,
    broken: bool,
}

impl tokio_stream::Stream for WatchBlockIdsStream {
    type Item = tonic::Result<proto::WatchBlockIdsEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use self::proto::watch_block_ids_event::Event;
        use crate::state::NewBlocksStreamItem;

        if self.broken {
            return Poll::Ready(None);
        }

        let Some(res) = ready!(self.inner.poll_next_unpin(cx)) else {
            return Poll::Ready(None);
        };

        let item = match res {
            Ok(item) => item,
            Err(e) => {
                self.broken = true;
                return Poll::Ready(Some(Err(e.into())));
            }
        };

        let event = match item {
            NewBlocksStreamItem::NewMcBlock(item) => {
                Event::NewMcBlock(proto::NewMasterchainBlock {
                    mc_state_info: Some(item.mc_state_info.into()),
                    mc_block_id: Some(item.mc_block_id.into()),
                    shard_description: item
                        .shard_description
                        .iter()
                        .map(|item| proto::ShardDescription {
                            latest_block_id: Some(item.block_id.into()),
                            end_lt: item.end_lt,
                            utime: item.gen_utime,
                            reg_mc_seqno: item.reg_mc_seqno,
                        })
                        .collect(),
                    shard_block_ids: item.shard_block_ids.iter().map(Into::into).collect(),
                })
            }
            NewBlocksStreamItem::RangeSkipped(item) => {
                Event::RangeSkipped(proto::BlocksRangeSkipped {
                    mc_state_info: Some(item.mc_state_info.into()),
                    from: item.from,
                    to: item.to,
                })
            }
        };

        Poll::Ready(Some(Ok(proto::WatchBlockIdsEvent { event: Some(event) })))
    }
}

pub enum GetBlockStream {
    NotFound {
        mc_state_info: crate::state::McStateInfo,
        finished: bool,
    },
    Found {
        mc_state_info: crate::state::McStateInfo,
        offset: u64,
        first: bool,
        stream: Option<Box<crate::state::BlockDataStream>>,
    },
}

impl Stream for GetBlockStream {
    type Item = tonic::Result<proto::GetBlockResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use proto::get_block_response::Msg;

        match &mut *self {
            // Send "NotFound" once.
            GetBlockStream::NotFound {
                mc_state_info,
                finished,
            } => Poll::Ready(if *finished {
                None
            } else {
                *finished = true;
                Some(Ok(proto::GetBlockResponse {
                    msg: Some(Msg::NotFound(proto::BlockNotFound {
                        mc_state_info: Some((*mc_state_info).into()),
                    })),
                }))
            }),
            // Send "Found" once and all remaining "Chunk"'s afterwards.
            GetBlockStream::Found {
                mc_state_info,
                offset,
                first,
                stream,
            } => {
                let field = stream;
                let Some(stream) = field.as_mut() else {
                    return Poll::Ready(None);
                };

                match Pin::new(stream.as_mut()).poll_next(cx) {
                    Poll::Ready(Some(Ok(data))) => {
                        let next_offset = offset.saturating_add(data.len() as u64);
                        let chunk = proto::BlockChunk {
                            offset: *offset,
                            data,
                        };
                        *offset = next_offset;

                        let total_size = stream.total_size();
                        let max_chunk_size = stream.chunk_size();
                        if next_offset >= total_size {
                            *field = None;
                        }

                        let msg = if std::mem::take(first) {
                            Msg::Found(proto::BlockFound {
                                mc_state_info: Some((*mc_state_info).into()),
                                total_size,
                                max_chunk_size,
                                first_chunk: Some(chunk),
                            })
                        } else {
                            Msg::Chunk(chunk)
                        };

                        Poll::Ready(Some(Ok(proto::GetBlockResponse { msg: Some(msg) })))
                    }
                    Poll::Ready(Some(Err(e))) => {
                        *field = None;
                        Poll::Ready(Some(Err(tonic::Status::internal(format!(
                            "data stream failed: {e}"
                        )))))
                    }
                    Poll::Ready(None) => {
                        *field = None;
                        Poll::Ready(if *first {
                            Some(Err(tonic::Status::internal(
                                "unexpected end of data stream",
                            )))
                        } else {
                            None
                        })
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

// === Protobuf helpers ===

trait RequireField {
    type Field;

    fn require(&self) -> tonic::Result<&Self::Field>;
}

impl<T> RequireField for Option<T> {
    type Field = T;

    #[inline]
    fn require(&self) -> tonic::Result<&Self::Field> {
        match self {
            Some(field) => Ok(field),
            None => Err(tonic::Status::invalid_argument(
                "required field is missing".to_owned(),
            )),
        }
    }
}

fn parse_hash_ref(value: &[u8]) -> tonic::Result<&HashBytes> {
    match value.try_into() {
        Ok::<&[u8; 32], _>(value) => Ok(HashBytes::wrap(value)),
        Err(_) => Err(tonic::Status::invalid_argument("invalid hash length").to_owned()),
    }
}

fn parse_shard_id(workchain: i32, shard: u64) -> tonic::Result<ShardIdent> {
    ShardIdent::new(workchain, shard)
        .ok_or_else(|| tonic::Status::invalid_argument("invalid shard id"))
}

fn parse_address(workchain: i32, account: &[u8]) -> tonic::Result<StdAddr> {
    let Ok::<i8, _>(workchain) = workchain.try_into() else {
        return Err(tonic::Status::invalid_argument(
            "workchain is out of std range",
        ));
    };
    Ok(StdAddr::new(workchain, *parse_hash_ref(account)?))
}

impl From<StateError> for tonic::Status {
    fn from(value: StateError) -> Self {
        match value {
            StateError::NotReady => tonic::Status::unavailable("service is not ready"),
            StateError::Internal(error) => tonic::Status::internal(error.to_string()),
            StateError::Cancelled => tonic::Status::cancelled("operation cancelled"),
            StateError::InvalidData(error) => tonic::Status::invalid_argument(error.to_string()),
        }
    }
}

impl From<crate::state::McStateInfo> for proto::McStateInfo {
    #[inline]
    fn from(value: crate::state::McStateInfo) -> Self {
        Self {
            mc_seqno: value.mc_seqno,
            lt: value.lt,
            utime: value.utime,
        }
    }
}

impl From<tycho_types::models::BlockId> for proto::BlockId {
    #[inline]
    fn from(value: tycho_types::models::BlockId) -> Self {
        Self::from(&value)
    }
}

impl From<&tycho_types::models::BlockId> for proto::BlockId {
    fn from(value: &tycho_types::models::BlockId) -> Self {
        Self {
            workchain: value.shard.workchain(),
            shard: value.shard.prefix(),
            seqno: value.seqno,
            root_hash: value.root_hash.as_slice().to_vec(),
            file_hash: value.file_hash.as_slice().to_vec(),
        }
    }
}

impl TryFrom<&proto::BlockId> for tycho_types::models::BlockId {
    type Error = tonic::Status;

    fn try_from(value: &proto::BlockId) -> Result<Self, Self::Error> {
        let shard = parse_shard_id(value.workchain, value.shard)?;
        Ok(Self {
            shard,
            seqno: value.seqno,
            root_hash: *parse_hash_ref(&value.root_hash)?,
            file_hash: *parse_hash_ref(&value.file_hash)?,
        })
    }
}
