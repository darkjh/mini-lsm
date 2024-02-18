use std::sync::Arc;

use anyhow::{bail, Result};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        if table.num_of_blocks() == 0 {
            bail!("Empty sstable")
        }

        let blk_iter = BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?);

        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        if self.table.num_of_blocks() == 0 {
            bail!("Empty sstable")
        }

        self.blk_idx = 0;
        self.blk_iter = BlockIterator::create_and_seek_to_first(self.table.read_block_cached(0)?);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        if table.num_of_blocks() == 0 {
            bail!("Empty sstable")
        }

        let block_idx = table.find_block_idx(key);

        let blk_iter = if block_idx >= table.num_of_blocks() {
            BlockIterator::empty()
        } else {
            let block = table.read_block_cached(block_idx)?;
            BlockIterator::create_and_seek_to_key(block, key)
        };

        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx: block_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let block_idx = self.table.find_block_idx(key);

        if block_idx >= self.table.num_of_blocks() {
            self.blk_iter = BlockIterator::empty();
        } else {
            let block = self.table.read_block_cached(block_idx)?;

            self.blk_idx = block_idx;
            self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();

        if !self.blk_iter.is_valid() {
            if self.blk_idx + 1 < self.table.num_of_blocks() {
                // switch to next block
                self.blk_idx += 1;
                let next_blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx)?,
                );
                let invalid_iter = std::mem::replace(&mut self.blk_iter, next_blk_iter);
                drop(invalid_iter);
            }
        }
        Ok(())
    }
}
