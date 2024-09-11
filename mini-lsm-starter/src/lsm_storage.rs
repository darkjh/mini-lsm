#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    // for mutating lsm structure
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        println!("Closing mini-lsm ...");
        self.inner.sync_dir()?;
        self.flush_notifier.send(()).ok();
        self.compaction_notifier.send(()).ok();

        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync_dir()?;
            return Ok(());
        }

        // freeze current memtable, if not empty
        if {
            let snapshot = self.inner.state.read();
            !snapshot.memtable.is_empty()
        } {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }

        // flush all memtables to sstables, if any
        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        println!("Opening mini-lsm storage ...");
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let block_cache = Arc::new(BlockCache::new(1024));

        let mut next_sst_id = 1usize;
        let manifest_path = path.join("MANIFEST");
        let manifest = if manifest_path.exists() {
            let (manifest, records) = Manifest::recover(&manifest_path)?;
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) =
                            compaction_controller.apply_compaction_result(&state, &task, &output);
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                    ManifestRecord::NewMemtable(_) => {}
                }
            }

            let mut sst_count = 0;
            // load ssts
            for sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, sst_ids)| sst_ids))
            {
                let sst_id = *sst_id;
                let sst = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, sst_id))
                        .with_context(|| format!("failed to open SST: {}", sst_id))?,
                )?;
                state.sstables.insert(sst_id, Arc::new(sst));
                sst_count += 1;
            }
            println!("{} SSTs opened", sst_count);

            next_sst_id += 1;
            state.memtable = Arc::new(MemTable::create(next_sst_id));
            manifest
        } else {
            Manifest::create(&manifest_path)?
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        println!("Storage loaded");
        storage.dump_structure();

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = { self.state.read().clone() };

        match snapshot.memtable.get(key) {
            Some(bytes) if bytes.is_empty() => Ok(None),
            Some(bytes) => Ok(Some(bytes)),
            None => {
                for memtable in &snapshot.imm_memtables {
                    if let Some(bytes) = memtable.get(key) {
                        return if bytes.is_empty() {
                            // deleted
                            Ok(None)
                        } else {
                            Ok(Some(bytes))
                        };
                    }
                }

                // no result in memtables, lookup in sstables by using a scan
                let sstable_iter = {
                    let l0_iters: Result<Vec<Box<SsTableIterator>>> = snapshot
                        .l0_sstables
                        .iter()
                        .filter(|idx| {
                            let table = snapshot.sstables.get(idx).unwrap().as_ref();
                            match &table.bloom {
                                Some(b) => b.may_contain(farmhash::fingerprint32(&key)),
                                None => true,
                            }
                        })
                        .map(|idx| {
                            let iter = SsTableIterator::create_and_seek_to_key(
                                snapshot.sstables.get(idx).unwrap().clone(),
                                KeySlice::from_slice(key),
                            )?;
                            Ok(Box::new(iter))
                        })
                        .collect();

                    let mut level_iters = vec![];
                    for (_, sst_ids) in &snapshot.levels {
                        let iter = {
                            let tables: Vec<Arc<SsTable>> = sst_ids
                                .iter()
                                .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                                .filter(|sst| match &sst.bloom {
                                    Some(b) => b.may_contain(farmhash::fingerprint32(&key)),
                                    None => true,
                                })
                                .collect();
                            SstConcatIterator::create_and_seek_to_key(
                                tables,
                                KeySlice::from_slice(key),
                            )
                        }?;
                        level_iters.push(Box::new(iter));
                    }

                    TwoMergeIterator::create(
                        MergeIterator::create(l0_iters?),
                        MergeIterator::create(level_iters),
                    )?
                };
                if sstable_iter.is_valid() {
                    if sstable_iter.key() == KeySlice::from_slice(key) {
                        return if sstable_iter.value().is_empty() {
                            // deleted
                            Ok(None)
                        } else {
                            Ok(Some(Bytes::copy_from_slice(sstable_iter.value())))
                        };
                    }
                }
                Ok(None)
            }
        }
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        {
            let guard = self.state.read();
            guard.memtable.put(_key, _value)?;
        }

        if self.is_size_limit_reached() {
            let lock = self.state_lock.lock();
            if self.is_size_limit_reached() {
                self.force_freeze_memtable(&lock)?;
            }
            drop(lock);
        }
        Ok(())
    }

    fn is_size_limit_reached(&self) -> bool {
        self.state.read().memtable.approximate_size() >= self.options.target_sst_size
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        {
            let guard = self.state.read();
            guard.memtable.put(_key, &[])?;
        }

        if self.is_size_limit_reached() {
            let lock = self.state_lock.lock();
            if self.is_size_limit_reached() {
                self.force_freeze_memtable(&lock)?;
            }
            drop(lock);
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // TODO should probably create the new memtable outside of the state lock
        let new_memtable = Arc::new(MemTable::create(self.next_sst_id()));
        {
            let mut guard = self.state.write();
            let mut state_snapshot = guard.as_ref().clone();
            println!("Freezing memtable id={:?}", state_snapshot.memtable.id());
            state_snapshot
                .imm_memtables
                .insert(0, state_snapshot.memtable);
            state_snapshot.memtable = new_memtable;

            *guard = Arc::new(state_snapshot);
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // exclusive access to lsm structure
        let _lock = self.state_lock.lock();

        let last_memtable = { self.state.read().imm_memtables.last().unwrap().clone() };
        let last_memtable_id = last_memtable.id();

        let mut builder = SsTableBuilder::new(self.options.block_size);
        last_memtable.flush(&mut builder)?;

        let sstable = builder.build(
            last_memtable_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(last_memtable_id),
        )?;
        let sstable_size = sstable.table_size();

        {
            let mut guard = self.state.write();

            let mut state_snapshot = guard.as_ref().clone();
            state_snapshot
                .sstables
                .insert(last_memtable_id, Arc::new(sstable));

            // insert the new sstable to the lsm structure, depending on the compaction algorithm used
            if self.compaction_controller.flush_to_l0() {
                state_snapshot.l0_sstables.insert(0, last_memtable_id);
            } else {
                let new_tier = (last_memtable_id, vec![last_memtable_id]);
                state_snapshot.levels.insert(0, new_tier);
            }

            // finally remove the flushed memtable
            if state_snapshot.imm_memtables.last().unwrap().id() == last_memtable_id {
                let _ = state_snapshot.imm_memtables.pop();
            } else {
                panic!("The memtable to flush is not the last one");
            }

            *guard = Arc::new(state_snapshot);

            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&_lock, ManifestRecord::Flush(last_memtable_id))?;
            self.sync_dir()?
        }
        println!(
            "Flushed memtable id={:?} size={:?}",
            last_memtable_id, sstable_size
        );
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            self.state.read().clone()
            // drop the read lock as the iterator creation can take some time
            // it's effective a pointer to a snapshot of lsm state
            // the state itself can be modified but the snapshot should be stable
        };
        let mut iters = vec![Box::new(snapshot.memtable.scan(lower, upper))];
        iters.extend(
            snapshot
                .imm_memtables
                .iter()
                .map(|x| Box::new(x.scan(lower, upper))),
        );
        let memtable_iter = MergeIterator::create(iters);

        let sstable_iter = {
            let l0_iters: Result<Vec<Box<SsTableIterator>>> = snapshot
                .l0_sstables
                .iter()
                .filter(|idx| {
                    let table = snapshot.sstables.get(idx).unwrap().clone();
                    // skip sstable if there's no intersection
                    match upper {
                        Bound::Included(ub) => {
                            table.first_key().as_key_slice() <= KeySlice::from_slice(ub)
                        }
                        Bound::Excluded(ub) => {
                            table.first_key().as_key_slice() < KeySlice::from_slice(ub)
                        }
                        Bound::Unbounded => true,
                    }
                })
                .map(|idx| {
                    let table = snapshot.sstables.get(idx).unwrap().clone();
                    let iter = match lower {
                        Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                        Bound::Included(lb) => SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(lb),
                        )?,
                        Bound::Excluded(lb) => {
                            let mut iter = SsTableIterator::create_and_seek_to_key(
                                table,
                                KeySlice::from_slice(lb),
                            )?;

                            // exclude the excluded bound
                            if iter.is_valid() && iter.key().raw_ref() == lb {
                                iter.next()?
                            }
                            iter
                        }
                    };

                    Ok(Box::new(iter))
                })
                .collect();

            let mut level_iters = vec![];
            for (_, sst_ids) in &snapshot.levels {
                let iter = {
                    let filtered_ssts: Vec<Arc<SsTable>> = sst_ids
                        .iter()
                        .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                        .filter(|sst| match upper {
                            Bound::Included(ub) => {
                                sst.first_key().as_key_slice() <= KeySlice::from_slice(ub)
                            }
                            Bound::Excluded(ub) => {
                                sst.first_key().as_key_slice() < KeySlice::from_slice(ub)
                            }
                            Bound::Unbounded => true,
                        })
                        .collect();

                    match lower {
                        Bound::Unbounded => {
                            SstConcatIterator::create_and_seek_to_first(filtered_ssts)?
                        }
                        Bound::Included(lb) => SstConcatIterator::create_and_seek_to_key(
                            filtered_ssts,
                            KeySlice::from_slice(lb),
                        )?,
                        Bound::Excluded(lb) => {
                            let mut iter = SstConcatIterator::create_and_seek_to_key(
                                filtered_ssts,
                                KeySlice::from_slice(lb),
                            )?;
                            // exclude the excluded bound
                            if iter.is_valid() && iter.key().raw_ref() == lb {
                                iter.next()?
                            }
                            iter
                        }
                    }
                };

                level_iters.push(Box::new(iter));
            }

            TwoMergeIterator::create(
                MergeIterator::create(l0_iters?),
                MergeIterator::create(level_iters),
            )?
        };

        LsmIterator::new(
            TwoMergeIterator::create(memtable_iter, sstable_iter)?,
            map_bound(upper),
        )
        .map(FusedIterator::new)
    }
}
