use anyhow::{bail, Result};
use bytes::Bytes;
use std::ops::Bound;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    MergeIterator<MemTableIterator>,
    TwoMergeIterator<MergeIterator<SsTableIterator>, MergeIterator<SstConcatIterator>>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<Bytes>) -> Result<Self> {
        let mut lsm_iter = Self { inner: iter, upper };

        // In case that first values are empty
        while lsm_iter.is_valid() && lsm_iter.value().is_empty() {
            match lsm_iter.next() {
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(lsm_iter)
    }

    fn is_out_of_bound(&self, key: &[u8]) -> bool {
        match self.upper.as_ref() {
            Bound::Unbounded => false,
            Bound::Included(up) => key > up,
            Bound::Excluded(up) => key >= up,
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && !self.is_out_of_bound(self.inner.key().key_ref())
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        loop {
            match self.inner.next() {
                Ok(_) => {
                    if !self.inner.is_valid() || !self.inner.value().is_empty() {
                        return Ok(());
                    }
                }
                e @ Err(_) => return e,
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.errored || !self.iter.is_valid() {
            panic!("underlying iterator error")
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.errored || !self.iter.is_valid() {
            panic!("underlying iterator error")
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.errored {
            bail!("underlying iterator has errored");
        }

        if self.iter.is_valid() {
            if let e @ Err(_) = self.iter.next() {
                self.errored = true;
                return e;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
