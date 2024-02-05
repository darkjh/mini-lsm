use anyhow::{bail, Result};

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner) -> Result<Self> {
        let mut lsm_iter = Self { inner: iter };

        // In case that first values are empty
        while lsm_iter.is_valid() && lsm_iter.value().is_empty() {
            match lsm_iter.next() {
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(lsm_iter)
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().into_inner()
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
}
