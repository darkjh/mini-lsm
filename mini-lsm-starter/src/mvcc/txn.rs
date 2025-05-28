use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::WriteBatchRecord;
use crate::mem_table::map_bound;
use crate::mvcc::CommittedTxnData;
use crate::{
    iterators::StorageIterator,
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;
use std::sync::atomic::Ordering;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.error_if_committed()?;

        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }

        match self.local_storage.get(key) {
            Some(entry) => {
                if entry.value().is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(entry.value().clone()))
                }
            }
            None => self.inner.get_with_ts(key, self.read_ts),
        }
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.error_if_committed()?;

        let map = self.local_storage.clone();
        let txn_local_iter = {
            let mut iter = TxnLocalIteratorBuilder {
                map,
                iter_builder: |m: &Arc<SkipMap<Bytes, Bytes>>| {
                    m.range((map_bound(lower), map_bound(upper)))
                },
                item: (Bytes::new(), Bytes::new()),
            }
            .build();
            let first_entry =
                iter.with_iter_mut(|iter| TxnLocalIterator::item_from_entry(iter.next()));
            iter.with_mut(|fields| *fields.item = first_entry);
            iter
        };

        let iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;

        let merged_iter = TwoMergeIterator::create(txn_local_iter, iter)?;

        TxnIterator::create(self.clone(), merged_iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.error_if_committed().unwrap();

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            let (write_set, _) = &mut *guard;
            write_set.insert(farmhash::hash32(key));
        }
    }

    pub fn delete(&self, key: &[u8]) {
        self.error_if_committed().unwrap();

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            let (write_set, _) = &mut *guard;
            write_set.insert(farmhash::hash32(key));
        }
    }

    pub fn commit(&self) -> Result<()> {
        self.error_if_committed()?;

        let _lock = self.inner.mvcc().commit_lock.lock();

        if let Some(key_hashes) = &self.key_hashes {
            let guard = key_hashes.lock();
            let (write_set, read_set) = &*guard;

            if write_set.is_empty() {
                // read only transaction
                let commit_ts = self.do_commit()?;
                self.inner.mvcc().update_commit_ts(commit_ts);
            } else {
                // write transaction, need to check serializability
                let mut committed_txns = self.inner.mvcc().committed_txns.lock();

                for (_, committed_txn) in committed_txns.range(self.read_ts + 1..) {
                    if !read_set.is_disjoint(&committed_txn.key_hashes) {
                        bail!("serializability check failed!");
                    }
                }

                // serializability check passed
                let commit_ts = self.do_commit()?;

                // update committed transaction tracking
                self.inner.mvcc().update_commit_ts(commit_ts);
                committed_txns.insert(
                    commit_ts,
                    CommittedTxnData {
                        key_hashes: write_set.clone(),
                        read_ts: self.read_ts,
                        commit_ts,
                    },
                );
            }
        } else {
            // without serializability check
            let commit_ts = self.do_commit()?;
            self.inner.mvcc().update_commit_ts(commit_ts);
        }

        // gc, only keeps committed transactions with ts >= watermark
        // otherwise they cannot be seen by any new transaction
        let mut committed_txns = self.inner.mvcc().committed_txns.lock();
        let watermark = self.inner.mvcc().watermark();
        committed_txns.retain(|&ts, _| ts >= watermark);

        self.committed.store(true, Ordering::Release);
        Ok(())
    }

    fn do_commit(&self) -> Result<u64> {
        let mut batch = Vec::with_capacity(self.local_storage.len());
        for entry in self.local_storage.iter() {
            if entry.value().is_empty() {
                batch.push(WriteBatchRecord::Del(entry.key().clone()));
            } else {
                batch.push(WriteBatchRecord::Put(
                    entry.key().clone(),
                    entry.value().clone(),
                ));
            }
        }
        self.inner.do_write_batch(&batch)
    }

    fn error_if_committed(&self) -> Result<()> {
        if self.committed.load(Ordering::Acquire) {
            bail!("cannot operate on committed txn!");
        }
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn item_from_entry(entry: Option<Entry<Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry.map_or_else(
            || (Bytes::new(), Bytes::new()),
            |entry| (entry.key().clone(), entry.value().clone()),
        )
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        let entry = self.borrow_item();
        entry.0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| {
            *fields.item = TxnLocalIterator::item_from_entry(fields.iter.next());
            Ok(())
        })
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = TxnIterator { txn, iter };
        iter.skip_deletes()?;
        Ok(iter)
    }

    fn skip_deletes(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.track_read(self.iter.key());
            self.iter.next()?;
        }
        Ok(())
    }

    fn track_read(&self, key: &[u8]) {
        if let Some(key_hashes) = &self.txn.key_hashes {
            let mut guard = key_hashes.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.iter.is_valid() {
            self.track_read(self.iter.key());
        }
        self.iter.next()?;
        self.skip_deletes()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
