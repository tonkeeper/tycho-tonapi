use std::borrow::Borrow;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, anyhow};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use bytesize::ByteSize;
use futures_util::future::BoxFuture;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::AbortHandle;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;
use tycho_block_util::message::validate_external_message;
use tycho_block_util::state::ShardStateStuff;
use tycho_core::block_strider::{BlockSubscriber, BlockSubscriberContext, StateSubscriber};
use tycho_core::blockchain_rpc::BlockchainRpcClient;
use tycho_core::global_config::ZerostateId;
use tycho_core::storage::{BlocksGcConfig, BlocksGcType, CoreStorage};
use tycho_storage::kv::NamedTables;
use tycho_types::merkle::MerkleProofBuilder;
use tycho_types::models::{
    Block, BlockId, BlockIdShort, DepthBalanceInfo, LibDescr, McBlockExtra, ShardAccount,
    ShardIdent, ShardStateUnsplit, StdAddr,
};
use tycho_types::prelude::*;
use tycho_util::mem::Reclaimer;
use tycho_util::{FastHashMap, FastHashSet, FastHasherState, serde_helpers};
use weedb::{OwnedSnapshot, rocksdb};

use crate::db::{TonApiDb, TonApiTables, tables};

const MIN_GC_SEQNO_OFFSET: u32 = 200;
const FALLBACK_GC_SEQNO_OFFSET: u32 = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppStateConfig {
    /// How many library cells to cache.
    ///
    /// Default: `100`
    pub libs_cache_capacity: u64,

    /// Maximum number of items allowed to request in batch.
    ///
    /// Default: `16`
    pub libs_max_batch_len: usize,

    /// Block data is divided into chunks of this size.
    ///
    /// Default: `1 MB`
    pub block_data_chunk_size: ByteSize,

    /// Max parallel block downloads.
    ///
    /// Default: `1000`
    pub max_concurrent_downloads: usize,

    /// Max parallel subscriptions for new block ids.
    ///
    /// Default: `10000`
    pub max_subscriptions: usize,

    /// How many events are buffered for slow readers before skipping the oldest one.
    ///
    /// Default: `50`
    pub events_buffer_size: usize,

    /// Keep at most this amount of masterchain states.
    ///
    /// Default: `3`
    pub states_tail_len: NonZeroUsize,

    /// Interval for known blocks GC.
    ///
    /// Default: `1 min`
    #[serde(with = "serde_helpers::humantime")]
    pub gc_interval: Duration,
}

impl Default for AppStateConfig {
    fn default() -> Self {
        Self {
            libs_cache_capacity: 100,
            libs_max_batch_len: 16,
            block_data_chunk_size: ByteSize::mib(1),
            max_concurrent_downloads: 1000,
            max_subscriptions: 10000,
            events_buffer_size: 50,
            states_tail_len: NonZeroUsize::new(3).unwrap(),
            gc_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct AppState {
    inner: Arc<Inner>,
}

impl AppState {
    pub fn new(
        client: BlockchainRpcClient,
        storage: CoreStorage,
        zerostate_id: ZerostateId,
        config: AppStateConfig,
        blocks_gc: Option<BlocksGcConfig>,
    ) -> Result<Self> {
        let gc_seqno_offset = 'gc_offset: {
            if let Some(gc) = blocks_gc
                && let BlocksGcType::BeforeSafeDistance { safe_distance, .. } = gc.ty
            {
                break 'gc_offset std::cmp::max(safe_distance, MIN_GC_SEQNO_OFFSET);
            }

            FALLBACK_GC_SEQNO_OFFSET
        };

        let db = storage.context().open_preconfigured(TonApiTables::NAME)?;

        let (new_mc_block_events, _) = watch::channel(None);

        tracing::info!(
            gc_seqno_offset,
            block_data_chunk_size = %config.block_data_chunk_size,
            "app state created"
        );

        Ok(Self {
            inner: Arc::new(Inner {
                is_ready: AtomicBool::new(false),
                client,
                storage,
                libs_cache: moka::sync::Cache::builder()
                    .max_capacity(config.libs_cache_capacity)
                    .build_with_hasher(Default::default()),
                latest_states: Default::default(),
                recent_states: RwLock::new(RecentStates {
                    states_tail_len: config.states_tail_len,
                    mc_seqno_start: 0,
                    mc_states: Default::default(),
                    shard_states: Default::default(),
                }),
                new_states: Default::default(),
                block_data_chunk_size: config.block_data_chunk_size.as_u64(),
                download_block_semaphore: Arc::new(Semaphore::new(config.max_concurrent_downloads)),
                subscriptions_semaphore: Arc::new(Semaphore::new(config.max_subscriptions)),
                db,
                db_snapshot: Default::default(),
                latest_mc_block_event: new_mc_block_events,
                zerostate_id,
                init_block_seqno: AtomicU32::new(u32::MAX),
                gc_seqno_offset,
                gc_interval: config.gc_interval,
                gc_task_handle: Default::default(),
                config,
            }),
        })
    }

    pub async fn init(&self, latest_block_id: &BlockId) -> Result<()> {
        let this = self.inner.as_ref();

        // Preload latest states
        let ref_by_mc_seqno = latest_block_id.seqno;

        let mc_state = this
            .storage
            .shard_state_storage()
            .load_state(ref_by_mc_seqno, latest_block_id)
            .await
            .and_then(CachedState::for_masterchain)
            .map(Arc::new)?;

        let mut shard_states = FastHashMap::default();
        for entry in mc_state.state.shards()?.latest_blocks() {
            let block_id = entry?;
            let state = this
                .storage
                .shard_state_storage()
                .load_state(ref_by_mc_seqno, &block_id)
                .await?;

            shard_states.insert(
                block_id.shard,
                CachedState::for_shard(state, mc_state.mc_state_info).map(Arc::new)?,
            );
        }

        this.recent_states
            .write()
            .push(mc_state.clone(), shard_states.values().cloned().collect());

        this.latest_states.store(Some(Arc::new(LatestStates {
            mc_state,
            shard_states,
        })));

        // Preload init block seqno
        let init_block_id = this
            .storage
            .node_state()
            .load_init_mc_block_id()
            .context("core storage left uninitialized")?;

        this.init_block_seqno
            .store(init_block_id.seqno, Ordering::Release);

        // TODO: Preload partially processed block ids from the previous restart

        // Spawn known blocks gc
        let gc_task = tokio::spawn({
            let span = tracing::info_span!("known_blocks_gc");

            let gc_interval = this.gc_interval;
            let this = Arc::downgrade(&self.inner);
            async move {
                tracing::info!("started");
                scopeguard::defer!(tracing::info!("finished"));

                let mut interval = tokio::time::interval(gc_interval);

                let mut progress = 0;
                loop {
                    interval.tick().await;

                    let Some(this) = this.upgrade() else {
                        return;
                    };

                    let latest_mc_seqno = this
                        .latest_states
                        .load()
                        .as_ref()
                        .map(|latest| latest.mc_state.mc_state_info.mc_seqno)
                        .unwrap_or_default();

                    let Some(remove_until) = latest_mc_seqno.checked_sub(this.gc_seqno_offset)
                    else {
                        continue;
                    };

                    if remove_until > progress {
                        match this.remove_known_blocks(remove_until).await {
                            Ok(_) => progress = remove_until,
                            Err(e) => tracing::error!("failed to remove known blocks: {e}"),
                        }
                    }
                }
            }
            .instrument(span)
        });
        *this.gc_task_handle.lock().unwrap() = Some(gc_task.abort_handle());

        // Done
        let was_ready = this.is_ready.swap(true, Ordering::Release);
        assert!(!was_ready);

        tracing::info!("app state is ready");
        Ok(())
    }

    pub fn config(&self) -> &AppStateConfig {
        &self.inner.config
    }

    pub fn get_status(&self) -> AppStatus {
        let this = self.inner.as_ref();
        AppStatus {
            mc_state_info: if self.is_ready() {
                this.latest_states
                    .load()
                    .as_ref()
                    .map(|x| x.mc_state.mc_state_info)
            } else {
                None
            },
            timestamp: tycho_util::time::now_millis(),
            zerostate_id: this.zerostate_id,
            init_block_seqno: this.init_block_seqno.load(Ordering::Acquire),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }

    pub async fn watch_new_blocks(&self, since_mc_seqno: u32) -> StateResult<NewBlocksStream> {
        const BUFFER_EVENTS: usize = 2;

        let this = self.inner.as_ref();
        let mc_state_info = this.load_mc_state_info()?;

        let permit = this
            .subscriptions_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StateError::Internal(anyhow!("subscription semaphore dropped")))?;

        let mut latest_event = this.latest_mc_block_event.subscribe();

        let (tx, rx) = mpsc::channel(BUFFER_EVENTS);
        let this = Arc::downgrade(&self.inner);
        tokio::task::spawn(async move {
            scopeguard::defer!(tracing::debug!("watch stream dropped"));

            // Capture permit.
            let _permit = permit;

            let mut at_seqno = since_mc_seqno;
            let mut can_use_db = true;
            let mut pending_event = None;

            'outer: loop {
                let event = 'event: {
                    if let Some(event) = pending_event.take() {
                        break 'event Ok(NewBlocksStreamItem::NewMcBlock(event));
                    }

                    let already_searched = can_use_db;
                    if can_use_db {
                        let Some(this) = this.upgrade() else {
                            return;
                        };

                        let loaded = (|| {
                            // Search the exact event first.
                            let snapshot = match this.get_old_mc_block_event(at_seqno, None)? {
                                OldEvent::Found(event) => {
                                    at_seqno = event.mc_block_id.seqno.saturating_add(1);
                                    return Ok(Some(NewBlocksStreamItem::NewMcBlock(event)));
                                }
                                OldEvent::NotFound(snapshot) => snapshot,
                            };

                            // Find the next event if any.
                            if let Some(next_seqno) =
                                this.get_closest_next_event(at_seqno, &snapshot)?
                                && let OldEvent::Found(event) =
                                    this.get_old_mc_block_event(next_seqno, Some(snapshot))?
                            {
                                let prev_seqno = at_seqno;
                                let event_seqno = event.mc_block_id.seqno;
                                debug_assert_eq!(event_seqno, next_seqno);

                                // Some events were skipped.
                                at_seqno = event_seqno.saturating_add(1);
                                pending_event = Some(event);

                                return Ok(Some(NewBlocksStreamItem::RangeSkipped(
                                    SkippedBlocksRange {
                                        mc_state_info: this.load_mc_state_info()?,
                                        from: prev_seqno,
                                        to: event_seqno.saturating_sub(1),
                                    },
                                )));
                            }

                            Ok::<_, StateError>(None)
                        })();

                        if let Some(event) = loaded.transpose() {
                            break 'event event;
                        }

                        // The requested seqno is somewhere in the future or the db is empty.
                        can_use_db = false;
                        tokio::task::yield_now().await;
                    }

                    if let Some(event) = latest_event.borrow_and_update().clone() {
                        let event_seqno = event.mc_block_id.seqno;
                        match at_seqno.cmp(&event_seqno) {
                            // Too old event was requested
                            std::cmp::Ordering::Less if already_searched => {
                                let prev_seqno = at_seqno;

                                // Some events were skipped.
                                at_seqno = event_seqno.saturating_add(1);
                                let mc_state_info = event.mc_state_info;
                                pending_event = Some(event);

                                break 'event Ok(NewBlocksStreamItem::RangeSkipped(
                                    SkippedBlocksRange {
                                        mc_state_info,
                                        from: prev_seqno,
                                        to: event_seqno.saturating_sub(1),
                                    },
                                ));
                            }
                            // Try to search event in DB
                            std::cmp::Ordering::Less => {
                                can_use_db = true;
                                continue 'outer;
                            }
                            // The exact event was found
                            std::cmp::Ordering::Equal => {
                                at_seqno = event_seqno.saturating_add(1);
                                break 'event Ok(NewBlocksStreamItem::NewMcBlock(event));
                            }
                            // The requested event is in the future, need to wait
                            std::cmp::Ordering::Greater => {}
                        }
                    }

                    // Wait until the next event
                    match latest_event.changed().await {
                        Ok(()) => continue 'outer,
                        Err(_) => return,
                    }
                };

                if tx.send(event).await.is_err() {
                    return;
                }
            }
        });

        Ok(WithMcStateInfo::new(mc_state_info, ReceiverStream::new(rx)))
    }

    pub async fn get_block(&self, query: &QueryBlock) -> StateResult<Option<Box<BlockDataStream>>> {
        let this = self.inner.as_ref();
        let mc_state_info = this.load_mc_state_info()?;

        let handles = this.storage.block_handle_storage();
        let blocks = this.storage.block_storage();

        let stream = 'stream: {
            let block_id = match query {
                QueryBlock::BySeqno(short_id) => {
                    let Some(block_id) = self
                        .find_block_id_by_seqno(short_id)
                        .map_err(StateError::Internal)?
                    else {
                        break 'stream None;
                    };
                    block_id
                }
                QueryBlock::ById(block_id) => *block_id,
            };

            let handle = match handles.load_handle(&block_id) {
                Some(handle) if handle.has_data() => handle,
                // Early exit of no data found.
                _ => break 'stream None,
            };

            let permit = this
                .download_block_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| StateError::Internal(anyhow!("blocks semaphore dropped")))?;

            // Check once more if block was removed during waiting for a permit.
            if !handle.has_data() {
                break 'stream None;
            }

            // TODO: Stream block data from disk.
            let data = blocks
                .load_block_data_decompressed(&handle)
                .await
                .map_err(StateError::Internal)?;

            Some(Box::new(BlockDataStream {
                total_size: data.len() as u64,
                chunk_size: this.block_data_chunk_size,
                block_id,
                data,
                offset: 0,
                _permit: permit,
            }))
        };

        Ok(WithMcStateInfo::new(mc_state_info, stream))
    }

    pub async fn get_account_state(
        &self,
        address: &StdAddr,
        with_proof: bool,
        at_block: &AtBlock,
    ) -> StateResult<Option<AccessedShardAccount>> {
        let this = self.inner.as_ref();

        let resolve = |cached: Arc<CachedState>| {
            let cached_block_id = cached.state.block_id();
            let address_workchain = address.workchain;

            if cached_block_id.shard.workchain() == address_workchain as i32 {
                if cached_block_id.shard.contains_address(address) {
                    Ok(Some(cached))
                } else {
                    Err(StateError::Internal(anyhow!(
                        "requested account id is not contained in the shard of the reference block"
                    )))
                }
            } else if cached_block_id.is_masterchain() {
                if let Some(shards) = cached
                    .state
                    .shards()
                    .map_err(StateError::Internal)?
                    .get_workchain_shards(address_workchain as i32)?
                {
                    for entry in shards.latest_blocks() {
                        let block_id = entry?;
                        if !block_id.shard.contains_account(&address.address) {
                            continue;
                        }

                        return Ok(this.recent_states.read().get(&block_id.as_short_id()));
                    }
                }

                Err(StateError::Internal(anyhow!(
                    "requested account id is not contained in any shard of the reference mc block"
                )))
            } else {
                Err(StateError::Internal(anyhow!(
                    "reference block must belong to the masterchain"
                )))
            }
        };

        // Find a cached state at the specified block.
        // TODO: Load state from disk if not cached?
        let mc_state_info;
        let Some(cached) = ({
            match at_block {
                AtBlock::Latest => {
                    // NOTE: Keep the scope of `latest` as small as possible.
                    let latest = this.latest_states.load();
                    let Some(latest) = latest.as_ref() else {
                        return Err(StateError::NotReady);
                    };

                    mc_state_info = latest.mc_state.mc_state_info;
                    latest.find_state_for_address(address)
                }
                AtBlock::BySeqno(short_id) => {
                    mc_state_info = this.load_mc_state_info()?;
                    if let Some(cached) = this.recent_states.read().get(short_id) {
                        resolve(cached)?
                    } else {
                        None
                    }
                }
                AtBlock::ById(block_id) => {
                    mc_state_info = this.load_mc_state_info()?;
                    if let Some(cached) = this.recent_states.read().get(&block_id.as_short_id())
                        && cached.state.block_id() == block_id
                    {
                        resolve(cached)?
                    } else {
                        None
                    }
                }
            }
        }) else {
            return Ok(WithMcStateInfo::new(mc_state_info, None));
        };

        // TODO: Add semaphore

        let mc_state_info = cached.mc_state_info;

        let (account_state, prev_lt, prev_hash, proof) = if with_proof {
            let address = address.clone();
            let db = self.inner.db.clone();
            let span = tracing::Span::current();
            tokio::task::spawn_blocking(move || {
                let _span = span.enter();

                let block_id = cached.state.block_id();
                debug_assert!(block_id.shard.workchain() == address.workchain as i32);

                let usage_tree = UsageTree::new(UsageTreeMode::OnLoad);

                // Get account and prepare its proof
                let mut prev_lt = 0;
                let mut prev_hash = HashBytes::ZERO;
                let account_proof;
                let account = {
                    let state_root = usage_tree.track(cached.state.root_cell());
                    let state = state_root.parse::<ShardStateUnsplit>()?;
                    let (accounts, _) = state.accounts.load()?.into_parts();

                    let account = accounts.get(address.address)?.map(|(_, account)| {
                        prev_lt = account.last_trans_lt;
                        prev_hash = account.last_trans_hash;
                        account.account.into_inner()
                    });

                    account_proof = {
                        let state_root = cached.state.root_cell().as_ref();
                        let proof = MerkleProofBuilder::new(state_root, usage_tree)
                            .prune_big_cells(true)
                            .build()?;
                        CellBuilder::build_from(proof)?
                    };

                    // NOTE: We need to untrack the cell just in case if `usage_tree` is still alive.
                    account.map(|cell| Boc::encode(Cell::untrack(cell)))
                };

                // Prepare state root proof
                let mut key = [0; tables::KnownBlocks::KEY_LEN];
                key[0] = address.workchain as u8;
                key[1..9].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
                key[9..13].copy_from_slice(&block_id.seqno.to_be_bytes());

                let Some(known_block) = db
                    .known_blocks
                    .get(key)
                    .map_err(|e| StateError::Internal(e.into()))?
                else {
                    return Err(StateError::Internal(anyhow::anyhow!(
                        "state root proof not found for block {block_id}"
                    )));
                };

                let state_root_proof = {
                    const OFFSET: usize = tables::KnownBlocks::STATE_PROOF_OFFSET;

                    let known_block = known_block.as_ref();
                    let state_proof_len =
                        u32::from_le_bytes(known_block[OFFSET..OFFSET + 4].try_into().unwrap());

                    Boc::decode(&known_block[OFFSET + 4..OFFSET + 4 + state_proof_len as usize])
                        .map_err(|e| {
                            StateError::Internal(anyhow!(
                                "failed to deserialize state roof proof: {e:?}"
                            ))
                        })?
                };
                drop(known_block);

                // Combine proofs into one cell
                let proof = {
                    let mut boc = tycho_types::boc::ser::BocHeader::<FastHasherState>::default();
                    boc.add_root(state_root_proof.as_ref());
                    boc.add_root(account_proof.as_ref());

                    let mut buffer = Vec::new();
                    boc.encode(&mut buffer);
                    buffer
                };

                Ok::<_, StateError>((account, prev_lt, prev_hash, Some(proof)))
            })
            .await
            .map_err(|_| StateError::Cancelled)??
        } else {
            // Simple case where we just access the account cell.

            let mut prev_lt = 0;
            let mut prev_hash = HashBytes::ZERO;

            let account = cached
                .accounts
                .get(address.address)?
                .map(|(_, account)| {
                    prev_lt = account.last_trans_lt;
                    prev_hash = account.last_trans_hash;
                    account.account.into_inner()
                })
                .map(Boc::encode);

            (account, prev_lt, prev_hash, None)
        };

        Ok(WithMcStateInfo::new(
            mc_state_info,
            Some(AccessedShardAccount {
                account_state: account_state.map(Bytes::from),
                proof: proof.map(Bytes::from),
                last_trans_lt: prev_lt,
                last_trans_hash: prev_hash,
            }),
        ))
    }

    pub fn get_library_cell(&self, hash: &HashBytes) -> StateResult<Option<Bytes>> {
        let state = 'state: {
            if let Some(latest) = self.inner.latest_states.load().as_ref() {
                break 'state latest.mc_state.clone();
            }
            return Err(StateError::NotReady);
        };

        let mut lib = self.inner.libs_cache.get(hash);
        if lib.is_none() {
            lib = state
                .libraries
                .get(hash)?
                .map(|x| Bytes::from(Boc::encode(x.lib)));

            if let Some(lib) = &lib {
                self.inner.libs_cache.insert(*hash, lib.clone());
            }
        }

        Ok(WithMcStateInfo::new(state.mc_state_info, lib))
    }

    // TODO: Make this method async and serialize libraries in a rayon task.
    pub fn get_library_cells<I, T>(&self, hashes: I) -> StateResult<Vec<LibrariesBatchEntry>>
    where
        I: ExactSizeIterator<Item = T>,
        T: Borrow<HashBytes>,
    {
        let state = 'state: {
            if let Some(latest) = self.inner.latest_states.load().as_ref() {
                break 'state latest.mc_state.clone();
            }
            return Err(StateError::NotReady);
        };

        let mut result = Vec::with_capacity(hashes.len());

        for item in hashes {
            let hash = item.borrow();
            let mut lib = self.inner.libs_cache.get(hash);
            if lib.is_none() {
                lib = state
                    .libraries
                    .get(hash)?
                    .map(|x| Bytes::from(Boc::encode(x.lib)));

                if let Some(lib) = &lib {
                    self.inner.libs_cache.insert(*hash, lib.clone());
                }
            }

            if let Some(cell) = lib {
                result.push(LibrariesBatchEntry { hash: *hash, cell });
            }
        }

        Ok(WithMcStateInfo::new(state.mc_state_info, result))
    }

    pub async fn send_message(&self, message: Bytes) -> Result<usize, StateError> {
        if let Err(e) = validate_external_message(&message).await {
            return Err(StateError::InvalidData(e.into()));
        }

        Ok(self.inner.client.broadcast_external_message(&message).await)
    }

    // ===

    fn find_block_id_by_seqno(&self, short_id: &BlockIdShort) -> Result<Option<BlockId>> {
        let this = self.inner.as_ref();

        let Ok::<i8, _>(workchain) = short_id.shard.workchain().try_into() else {
            return Ok(None);
        };

        let mut key = [0; tables::KnownBlocks::KEY_LEN];
        key[0] = workchain as u8;
        key[1..9].copy_from_slice(&short_id.shard.prefix().to_be_bytes());
        key[9..13].copy_from_slice(&short_id.seqno.to_be_bytes());

        let Some(value) = this.db.known_blocks.get(key)? else {
            return Ok(None);
        };
        let value = value.as_ref();

        Ok(Some(BlockId {
            shard: short_id.shard,
            seqno: short_id.seqno,
            root_hash: HashBytes::from_slice(&value[0..32]),
            file_hash: HashBytes::from_slice(&value[32..64]),
        }))
    }
}

impl BlockSubscriber for AppState {
    type Prepared = tokio::task::JoinHandle<Result<()>>;
    type PrepareBlockFut<'a> = futures_util::future::Ready<Result<Self::Prepared>>;
    type HandleBlockFut<'a> = BoxFuture<'a, Result<()>>;

    fn prepare_block<'a>(&'a self, cx: &'a BlockSubscriberContext) -> Self::PrepareBlockFut<'a> {
        let block_id = *cx.block.id();
        let mc_seqno = cx.mc_block_id.seqno;

        // TODO: Fill from either archive data or load from disk. However, it may not be
        //       even needed, because we can read it from the compressed data.
        let raw_data_size = 0u64;

        let block = cx.block.clone();
        let db = self.inner.db.clone();
        let span = tracing::Span::current();
        let handle = tokio::task::spawn_blocking(move || {
            // Skip unusual workchains
            let Ok::<i8, _>(workchain) = block_id.shard.workchain().try_into() else {
                return Ok(());
            };

            let _span = span.enter();

            let info = block.load_info()?;

            let shard_hashes = block_id
                .is_masterchain()
                .then(|| {
                    let custom = block.load_custom()?;
                    BriefShardDescription::load_from_custom(custom)
                })
                .transpose()?;

            let mut batch = rocksdb::WriteBatch::default();

            // Prepare common key
            let mut key = [0; tables::BlocksByMcSeqno::KEY_LEN];
            key[0..4].copy_from_slice(&mc_seqno.to_be_bytes());
            key[4] = workchain as u8;
            key[5..13].copy_from_slice(&block_id.shard.prefix().to_be_bytes());
            key[13..17].copy_from_slice(&block_id.seqno.to_be_bytes());

            let block_proof = make_state_root_proof(block.root_cell())
                .map(Boc::encode)
                .context("failed to build state root proof")?;
            let Ok::<u32, _>(block_proof_len) = block_proof.len().try_into() else {
                anyhow::bail!("state root proof is too big");
            };

            // Reserve value buffer
            let mut value = Vec::<u8>::with_capacity(256);

            // Fill known_blocks entry
            value.extend_from_slice(block_id.root_hash.as_array());
            value.extend_from_slice(block_id.file_hash.as_array());
            value.extend_from_slice(&mc_seqno.to_le_bytes());
            value.extend_from_slice(&raw_data_size.to_le_bytes());
            value.extend_from_slice(&block_proof_len.to_le_bytes());
            value.extend_from_slice(&block_proof);

            batch.put_cf(&db.known_blocks.cf(), &key[4..], &value);

            // Fill blocks_by_mc_seqno entry
            value.clear();
            value.extend_from_slice(block_id.root_hash.as_array());
            value.extend_from_slice(block_id.file_hash.as_array());
            value.extend_from_slice(&info.end_lt.to_le_bytes());
            value.extend_from_slice(&info.gen_utime.to_le_bytes());
            if let Some(shard_hashes) = &shard_hashes {
                value.extend_from_slice(&(shard_hashes.len() as u32).to_le_bytes());
                for item in shard_hashes {
                    value.push(item.block_id.shard.workchain() as i8 as u8);
                    value.extend_from_slice(&item.block_id.shard.prefix().to_le_bytes());
                    value.extend_from_slice(&item.block_id.seqno.to_le_bytes());
                    value.extend_from_slice(item.block_id.root_hash.as_array());
                    value.extend_from_slice(item.block_id.file_hash.as_array());
                    value.extend_from_slice(&item.end_lt.to_le_bytes());
                    value.extend_from_slice(&item.gen_utime.to_le_bytes());
                    value.extend_from_slice(&item.reg_mc_seqno.to_le_bytes());
                }
            }

            batch.put_cf(&db.blocks_by_mc_seqno.cf(), key.as_slice(), value);

            // Write batch
            db.rocksdb()
                .write_opt(batch, db.known_blocks.write_config())?;

            // Done
            Ok::<_, anyhow::Error>(())
        });

        futures_util::future::ready(Ok(handle))
    }

    fn handle_block<'a>(
        &'a self,
        _cx: &'a BlockSubscriberContext,
        prepared: Self::Prepared,
    ) -> Self::HandleBlockFut<'a> {
        Box::pin(async move {
            prepared.await?.context("failed to store block metadata")?;

            Ok(())
        })
    }
}

impl StateSubscriber for AppState {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(
        &'a self,
        cx: &'a tycho_core::block_strider::StateSubscriberContext,
    ) -> Self::HandleStateFut<'a> {
        Box::pin(async move {
            let this = self.inner.as_ref();

            let Some(prev_latest_states) = this.latest_states.load_full() else {
                anyhow::bail!("app state was not properly initialized");
            };

            let block_id = cx.block.id();

            // Reload storage cell from disk
            let state = this
                .storage
                .shard_state_storage()
                .load_state(cx.mc_block_id.seqno, block_id)
                .await?;

            if !cx.block.id().is_masterchain() {
                let mut new = self.inner.new_states.lock().unwrap();
                if new.ref_by_mc_seqno != cx.mc_block_id.seqno {
                    new.ref_by_mc_seqno = cx.mc_block_id.seqno;
                    new.shard_states.clear();
                }
                new.shard_states.push(state);
                return Ok(());
            }

            // Update DB snapshot only after masterchain block has beed processed.
            this.db_snapshot
                .store(Some(Arc::new(this.db.owned_snapshot())));

            // TODO: Somehow reuse data from `prepare_block`
            let shard_description = cx
                .block
                .load_custom()
                .and_then(BriefShardDescription::load_from_custom)?;

            // Prepare cached mc state
            let mc_state = CachedState::for_masterchain(state).map(Arc::new)?;

            // Send new block event.
            let mut new_shard_states = {
                let mut new = this.new_states.lock().unwrap();
                if new.ref_by_mc_seqno == block_id.seqno {
                    std::mem::take(&mut new.shard_states)
                } else {
                    new.shard_states.clear();
                    Vec::new()
                }
            };

            // FIXME: Should not sort like this, but its ok for now since there is one base shard.
            new_shard_states.sort_unstable_by(|a, b| a.block_id().cmp(b.block_id()));

            let mut shard_block_ids = Vec::with_capacity(new_shard_states.len());
            let new_shard_states = new_shard_states
                .into_iter()
                .map(|state| {
                    shard_block_ids.push(*state.block_id());
                    CachedState::for_shard(state, mc_state.mc_state_info).map(Arc::new)
                })
                .collect::<Result<Vec<_>>>()?;

            this.recent_states
                .write()
                .push(mc_state.clone(), new_shard_states.clone());

            // Update the latest states
            this.latest_states.store(Some(Arc::new(LatestStates {
                mc_state: mc_state.clone(),
                // FIXME: Assumes that there is only one base shard for now.
                shard_states: match new_shard_states.last() {
                    None => prev_latest_states.shard_states.clone(),
                    Some(shard) => {
                        FastHashMap::from_iter([(shard.state.block_id().shard, shard.clone())])
                    }
                },
            })));

            // Send event
            this.latest_mc_block_event
                .send_replace(Some(Arc::new(NewMasterchainBlock {
                    mc_state_info: mc_state.mc_state_info,
                    mc_block_id: *block_id,
                    shard_description,
                    shard_block_ids,
                })));

            // Done
            Ok(())
        })
    }
}

struct Inner {
    is_ready: AtomicBool,
    config: AppStateConfig,
    client: BlockchainRpcClient,
    storage: CoreStorage,
    libs_cache: moka::sync::Cache<HashBytes, Bytes, FastHasherState>,
    latest_states: ArcSwapOption<LatestStates>,
    recent_states: RwLock<RecentStates>,
    new_states: Mutex<NewStates>,
    block_data_chunk_size: u64,
    download_block_semaphore: Arc<Semaphore>,
    subscriptions_semaphore: Arc<Semaphore>,
    db: TonApiDb,
    db_snapshot: ArcSwapOption<OwnedSnapshot>,
    latest_mc_block_event: watch::Sender<Option<Arc<NewMasterchainBlock>>>,
    zerostate_id: ZerostateId,
    init_block_seqno: AtomicU32,
    gc_seqno_offset: u32,
    gc_interval: Duration,
    gc_task_handle: Mutex<Option<AbortHandle>>,
}

impl Inner {
    fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::Acquire)
    }

    fn load_mc_state_info(&self) -> Result<McStateInfo, StateError> {
        let latest_mc_state = if self.is_ready() {
            self.latest_states
                .load()
                .as_ref()
                .map(|x| x.mc_state.mc_state_info)
        } else {
            None
        };

        latest_mc_state.ok_or(StateError::NotReady)
    }

    async fn remove_known_blocks(&self, until_mc_seqno: u32) -> Result<bool> {
        tracing::info!(until_mc_seqno, "started known blocks gc");
        let started_at = Instant::now();

        let db = self.db.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();

            // Find shard blocks edge.
            let mut key = [0; tables::BlocksByMcSeqno::KEY_LEN];
            key[0..4].copy_from_slice(&until_mc_seqno.to_be_bytes());
            key[4] = ShardIdent::MASTERCHAIN.workchain() as u8;
            key[5..13].copy_from_slice(&ShardIdent::MASTERCHAIN.prefix().to_be_bytes());
            key[13..17].copy_from_slice(&until_mc_seqno.to_be_bytes());
            let Some(value) = db.blocks_by_mc_seqno.get(key)? else {
                tracing::info!("target block not found, skipping");
                return Ok(false);
            };

            tracing::info!(
                short_id = %format_args!("{}:{until_mc_seqno}", ShardIdent::MASTERCHAIN),
                "found target masterchain block"
            );

            let mut batch = rocksdb::WriteBatch::default();

            // Delete a range from `blocks_by_mc_seqno` table.
            key[4..].fill(0xff);
            batch.delete_range_cf(
                &db.blocks_by_mc_seqno.cf(),
                [0; tables::BlocksByMcSeqno::KEY_LEN].as_slice(),
                key.as_slice(),
            );

            // Remove known mc blocks.
            let mut range_from = [0u8; tables::KnownBlocks::KEY_LEN];
            // NOTE: copy workchain+shard from key because it already contains a mc shard.
            range_from[0..9].copy_from_slice(&key[4..13]);
            let mut range_to = range_from;
            // NOTE: range_to has +1 block to make the range inclusive.
            range_to[9..13].copy_from_slice(&until_mc_seqno.saturating_add(1).to_be_bytes());

            batch.delete_range_cf(
                &db.known_blocks.cf(),
                range_from.as_slice(),
                range_to.as_slice(),
            );

            // Remove all referenced shard blocks up to this mc block.
            {
                const OFFSET: usize = tables::BlocksByMcSeqno::SHARD_HASHES_OFFSET;

                let mut value = value.as_ref();

                let shard_count = u32::from_le_bytes(value[OFFSET..OFFSET + 4].try_into().unwrap());
                value = &value[OFFSET + 4..];
                for _ in 0..shard_count {
                    let workchain = value[0] as i8;
                    let shard = u64::from_le_bytes(value[1..9].try_into().unwrap());
                    let seqno = u32::from_le_bytes(value[9..13].try_into().unwrap());

                    tracing::info!(
                        short_id = %format_args!("{workchain}:{shard:016x}:{seqno}"),
                        "found target shard block"
                    );

                    // Update workchain+shard for shard blocks (seqno for range_from is still 0).
                    range_from[0] = workchain as u8;
                    range_from[1..9].copy_from_slice(&shard.to_be_bytes());

                    // Copy workchain+short for range end and set the seqno
                    range_to[0..9].copy_from_slice(&range_from[0..9]);
                    // NOTE: range_to has +1 block to make the range inclusive.
                    range_to[9..13].copy_from_slice(&seqno.saturating_add(1).to_be_bytes());

                    batch.delete_range_cf(
                        &db.known_blocks.cf(),
                        range_from.as_slice(),
                        range_to.as_slice(),
                    );

                    // Move to the next shard description
                    value = &value[tables::BlocksByMcSeqno::SHARD_HASHES_ITEM_LEN..];
                }
            }

            db.rocksdb().write(batch)?;

            tracing::info!(
                elapsed = %humantime::format_duration(started_at.elapsed()),
                "known blocks gc finished"
            );
            Ok(true)
        })
        .await?
    }

    fn get_closest_next_event(
        &self,
        mc_seqno: u32,
        snapshot: &OwnedSnapshot,
    ) -> Result<Option<u32>, StateError> {
        let mut key = [0; tables::BlocksByMcSeqno::KEY_LEN];
        key[0..4].copy_from_slice(&mc_seqno.to_be_bytes());

        let mut readopts = self.db.blocks_by_mc_seqno.new_read_config();
        readopts.set_snapshot(snapshot);
        readopts.set_iterate_lower_bound(key.as_slice());

        let mut iter = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&self.db.blocks_by_mc_seqno.cf(), readopts);
        iter.seek_to_first();

        match iter.key() {
            Some(key) => {
                let seqno = u32::from_be_bytes(key[0..4].try_into().unwrap());
                Ok(Some(seqno))
            }
            None => match iter.status() {
                Ok(()) => Ok(None),
                Err(e) => Err(StateError::Internal(e.into())),
            },
        }
    }

    fn get_old_mc_block_event(
        &self,
        mc_seqno: u32,
        snapshot: Option<Arc<OwnedSnapshot>>,
    ) -> Result<OldEvent, StateError> {
        let mc_state_info = self.load_mc_state_info()?;
        let Some(snapshot) = snapshot.or_else(|| self.db_snapshot.load_full()) else {
            return Err(StateError::NotReady);
        };

        let mut key = [0; tables::BlocksByMcSeqno::KEY_LEN];
        key[0..4].copy_from_slice(&mc_seqno.to_be_bytes());

        let mut readopts = self.db.blocks_by_mc_seqno.new_read_config();
        readopts.set_snapshot(&snapshot);
        readopts.set_iterate_lower_bound(key.as_slice());
        key[0..4].copy_from_slice(&mc_seqno.saturating_add(1).to_be_bytes());
        readopts.set_iterate_upper_bound(key.as_slice());

        let mut iter = self
            .db
            .rocksdb()
            .raw_iterator_cf_opt(&self.db.blocks_by_mc_seqno.cf(), readopts);
        iter.seek_to_first();

        let mut shard_block_ids = Vec::new();
        let mut shard_description = Vec::new();

        'items: loop {
            let (key, mut value) = match iter.item() {
                Some(item) => item,
                None => match iter.status() {
                    // No items found.
                    Ok(()) if shard_block_ids.is_empty() => break 'items,
                    // We would have exited on a masterchain block if there was any.
                    Ok(()) => {
                        return Err(StateError::Internal(anyhow!(
                            "inconsistent `blocks_by_mc_seqno` state for block {}:{mc_seqno}",
                            ShardIdent::MASTERCHAIN
                        )));
                    }
                    Err(e) => return Err(StateError::Internal(e.into())),
                },
            };

            let shard = ShardIdent::new(
                key[4] as i8 as i32,
                u64::from_be_bytes(key[5..13].try_into().unwrap()),
            )
            .expect("db must store a valid shard");

            let block_id = BlockId {
                shard,
                seqno: u32::from_be_bytes(key[13..17].try_into().unwrap()),
                root_hash: HashBytes::from_slice(&value[0..32]),
                file_hash: HashBytes::from_slice(&value[32..64]),
            };

            if block_id.is_masterchain() {
                debug_assert_eq!(
                    block_id.seqno, mc_seqno,
                    "masterchain block must be referenced by itself",
                );

                const OFFSET: usize = tables::BlocksByMcSeqno::SHARD_HASHES_OFFSET;

                let shard_count = u32::from_le_bytes(value[OFFSET..OFFSET + 4].try_into().unwrap());
                value = &value[OFFSET + 4..];
                for _ in 0..shard_count {
                    let shard = ShardIdent::new(
                        value[0] as i8 as i32,
                        u64::from_le_bytes(value[1..9].try_into().unwrap()),
                    )
                    .expect("db must store a valid shard");
                    let seqno = u32::from_le_bytes(value[9..13].try_into().unwrap());

                    shard_description.push(BriefShardDescription {
                        block_id: BlockId {
                            shard,
                            seqno,
                            root_hash: HashBytes::from_slice(&value[13..45]),
                            file_hash: HashBytes::from_slice(&value[45..77]),
                        },
                        end_lt: u64::from_le_bytes(value[77..85].try_into().unwrap()),
                        gen_utime: u32::from_le_bytes(value[85..89].try_into().unwrap()),
                        reg_mc_seqno: u32::from_le_bytes(value[89..93].try_into().unwrap()),
                    });

                    // Move to the next shard description
                    value = &value[tables::BlocksByMcSeqno::SHARD_HASHES_ITEM_LEN..];
                }

                return Ok(OldEvent::Found(Arc::new(NewMasterchainBlock {
                    mc_state_info,
                    mc_block_id: block_id,
                    shard_description,
                    shard_block_ids,
                })));
            } else {
                shard_block_ids.push(block_id);
            }

            iter.next();
        }

        Ok(OldEvent::NotFound(snapshot))
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(handle) = self.gc_task_handle.get_mut().unwrap().take() {
            handle.abort();
        }
    }
}

enum OldEvent {
    Found(Arc<NewMasterchainBlock>),
    NotFound(Arc<OwnedSnapshot>),
}

struct LatestStates {
    mc_state: Arc<CachedState>,
    shard_states: FastHashMap<ShardIdent, Arc<CachedState>>,
}

impl LatestStates {
    fn find_state_for_address(&self, address: &StdAddr) -> Option<Arc<CachedState>> {
        if address.is_masterchain() {
            Some(self.mc_state.clone())
        } else {
            for (shard, state) in &self.shard_states {
                if shard.contains_address(address) {
                    return Some(state.clone());
                }
            }
            None
        }
    }
}

struct RecentStates {
    states_tail_len: NonZeroUsize,
    mc_seqno_start: u32,
    mc_states: VecDeque<Arc<CachedState>>,
    shard_states: FastHashMap<BlockIdShort, Arc<CachedState>>,
}

impl RecentStates {
    fn push(&mut self, mc_state: Arc<CachedState>, shard_states: Vec<Arc<CachedState>>) {
        if self.mc_states.is_empty() {
            assert_eq!(self.mc_seqno_start, 0);
            self.mc_seqno_start = mc_state.state.block_id().seqno;
            self.mc_states.push_back(mc_state);
        } else {
            self.mc_states.push_back(mc_state);
            if self.mc_states.len() > self.states_tail_len.get() {
                let oldest = self.mc_states.pop_front().expect("mc state not empty");
                assert_eq!(oldest.state.block_id().seqno, self.mc_seqno_start);
                self.mc_seqno_start += 1;
                Reclaimer::instance().drop(oldest);
            }
        }

        let mut shard_updated = FastHashSet::default();
        for shard in shard_states {
            shard_updated.insert(shard.state.block_id().shard);
            self.shard_states
                .insert(shard.state.block_id().as_short_id(), shard);
        }

        self.shard_states.retain(|short_id, cached| {
            if !shard_updated.contains(&short_id.shard) {
                // Retain shards that were not updated
                return true;
            }

            // Keep shard states referenced by the current mc states tail
            cached.mc_state_info.mc_seqno >= self.mc_seqno_start
        });
    }

    fn get(&self, short_id: &BlockIdShort) -> Option<Arc<CachedState>> {
        if short_id.is_masterchain() {
            short_id
                .seqno
                .checked_sub(self.mc_seqno_start)
                .and_then(|idx| self.mc_states.get(idx as usize))
        } else {
            self.shard_states.get(short_id)
        }
        .cloned()
    }
}

#[derive(Default)]
struct NewStates {
    ref_by_mc_seqno: u32,
    shard_states: Vec<ShardStateStuff>,
}

#[derive(Clone)]
struct CachedState {
    state: ShardStateStuff,
    libraries: Dict<HashBytes, LibDescr>,
    accounts: ShardAccountsDict,
    mc_state_info: McStateInfo,
}

impl CachedState {
    fn for_masterchain(state: ShardStateStuff) -> Result<Self> {
        let libraries;
        let accounts;
        let mc_state_info;
        {
            let state = state.as_ref();
            libraries = state.libraries.clone();
            accounts = state.load_accounts()?.into_parts().0;
            mc_state_info = McStateInfo {
                mc_seqno: state.seqno,
                lt: state.gen_lt,
                utime: state.gen_utime,
            };
        }

        Ok(Self {
            state,
            libraries,
            accounts,
            mc_state_info,
        })
    }

    fn for_shard(state: ShardStateStuff, mc_state_info: McStateInfo) -> Result<Self> {
        let (accounts, _) = state.as_ref().load_accounts()?.into_parts();
        Ok(Self {
            state,
            libraries: Dict::new(),
            accounts,
            mc_state_info,
        })
    }
}

type ShardAccountsDict = Dict<HashBytes, (DepthBalanceInfo, ShardAccount)>;

pub struct WithMcStateInfo<T> {
    pub mc_state_info: McStateInfo,
    pub data: T,
}

impl<T> WithMcStateInfo<T> {
    #[inline]
    fn new(mc_state_info: McStateInfo, data: T) -> Self {
        Self {
            mc_state_info,
            data,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppStatus {
    pub mc_state_info: Option<McStateInfo>,
    pub timestamp: u64,
    pub zerostate_id: ZerostateId,
    pub init_block_seqno: u32,
}

pub struct AccessedShardAccount {
    pub account_state: Option<Bytes>,
    pub proof: Option<Bytes>,
    pub last_trans_lt: u64,
    pub last_trans_hash: HashBytes,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct McStateInfo {
    pub mc_seqno: u32,
    pub lt: u64,
    pub utime: u32,
}

#[derive(Debug, Clone)]
pub struct LibrariesBatchEntry {
    pub hash: HashBytes,
    pub cell: Bytes,
}

#[derive(Debug)]
pub struct NewMasterchainBlock {
    pub mc_state_info: McStateInfo,
    pub mc_block_id: BlockId,
    pub shard_description: Vec<BriefShardDescription>,
    pub shard_block_ids: Vec<BlockId>,
}

#[derive(Debug)]
pub struct BriefShardDescription {
    pub block_id: BlockId,
    pub end_lt: u64,
    pub gen_utime: u32,
    pub reg_mc_seqno: u32,
}

impl BriefShardDescription {
    fn load_from_custom(custom: &McBlockExtra) -> Result<Vec<Self>, tycho_types::error::Error> {
        let mut result = Vec::new();
        for entry in custom.shards.iter() {
            let (shard, descr) = entry?;
            if i8::try_from(shard.workchain()).is_err() {
                continue;
            }

            result.push(BriefShardDescription {
                block_id: BlockId {
                    shard,
                    seqno: descr.seqno,
                    root_hash: descr.root_hash,
                    file_hash: descr.file_hash,
                },
                end_lt: descr.end_lt,
                gen_utime: descr.gen_utime,
                reg_mc_seqno: descr.reg_mc_seqno,
            })
        }
        Ok(result)
    }
}

#[derive(Debug)]
pub struct SkippedBlocksRange {
    pub mc_state_info: McStateInfo,
    pub from: u32,
    pub to: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum AtBlock {
    Latest,
    BySeqno(BlockIdShort),
    ById(BlockId),
}

#[derive(Debug, Clone, Copy)]
pub enum QueryBlock {
    BySeqno(BlockIdShort),
    ById(BlockId),
}

pub enum NewBlocksStreamItem {
    NewMcBlock(Arc<NewMasterchainBlock>),
    RangeSkipped(SkippedBlocksRange),
}

pub type NewBlocksStream = ReceiverStream<Result<NewBlocksStreamItem, StateError>>;

pub struct BlockDataStream {
    total_size: u64,
    chunk_size: u64,
    block_id: BlockId,
    // TODO: Stream data from disk
    data: Bytes,
    offset: u64,
    _permit: OwnedSemaphorePermit,
}

impl BlockDataStream {
    #[inline]
    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    #[inline]
    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    #[inline]
    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }
}

impl Stream for BlockDataStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self.as_mut();
        let data_len = this.data.len() as u64;
        if this.offset >= data_len {
            return Poll::Ready(None);
        }

        let next_offset = std::cmp::min(this.offset.saturating_add(this.chunk_size), data_len);
        let data = this.data.slice(this.offset as usize..next_offset as usize);
        this.offset = next_offset;
        Poll::Ready(Some(Ok(data)))
    }
}

fn make_state_root_proof(block_root: &Cell) -> Result<Cell, tycho_types::error::Error> {
    let usage_tree = UsageTree::new(UsageTreeMode::OnLoad);

    let block = usage_tree.track(block_root).parse::<Block>()?;
    let block_info = block.load_info()?;
    block_info.prev_ref.touch_recursive();
    let _state_update = block.state_update.load()?;

    let proof = MerkleProofBuilder::new(block_root.as_ref(), usage_tree)
        .prune_big_cells(true)
        .build()?;

    CellBuilder::build_from(proof)
}

pub type StateResult<T> = Result<WithMcStateInfo<T>, StateError>;

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("state is not ready")]
    NotReady,
    #[error("internal error: {0}")]
    Internal(#[source] anyhow::Error),
    #[error("cancelled")]
    Cancelled,
    #[error("invalid data: {0}")]
    InvalidData(anyhow::Error),
}

impl From<tycho_types::error::Error> for StateError {
    #[inline]
    fn from(value: tycho_types::error::Error) -> Self {
        Self::Internal(value.into())
    }
}
