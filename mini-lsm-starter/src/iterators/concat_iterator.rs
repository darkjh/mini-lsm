use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_idx: 0,
                sstables,
            });
        }

        let iter = SsTableIterator::create_and_seek_to_first(sstables[0].clone())?;
        Ok(SstConcatIterator {
            current: Some(iter),
            next_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_idx: 0,
                sstables,
            });
        }
        // find the first sstable that could potentially contain the key
        let idx: usize = sstables
            .partition_point(|table| table.first_key().as_key_slice() <= key)
            .saturating_sub(1);
        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_idx: sstables.len(),
                sstables,
            });
        }

        let iter = SsTableIterator::create_and_seek_to_key(sstables[idx].clone(), key)?;
        Ok(SstConcatIterator {
            current: Some(iter),
            next_idx: idx + 1,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().unwrap().is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;

        // if after the next() call, the current iterator is not valid anymore
        // we need to make sure we move to the next iterator that is valid
        while self.next_idx < self.sstables.len() && !self.current.as_ref().unwrap().is_valid() {
            self.current = Some(SsTableIterator::create_and_seek_to_first(
                self.sstables[self.next_idx].clone(),
            )?);
            self.next_idx += 1;
        }

        if self.next_idx > self.sstables.len() {
            self.current = None;
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
