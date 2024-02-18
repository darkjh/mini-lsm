use bytes::Buf;
use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    pub(crate) fn empty() -> BlockIterator {
        BlockIterator::create_and_seek_to_first(Arc::new(Block {
            data: Vec::new(),
            offsets: Vec::new(),
        }))
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            return;
        }
        let offset = self.block.offsets[idx];
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    fn seek_to_offset(&mut self, offset: u16) {
        let mut entry = &self.block.data[offset as usize..];

        let key_len = entry.get_u16() as usize;
        self.key = KeyVec::from_vec(entry[..key_len].to_vec());

        entry.advance(key_len);

        let value_len = entry.get_u16() as usize;
        self.value_range.0 = offset as usize + 4 + key_len;
        self.value_range.1 = self.value_range.0 + value_len;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let pos = self.block.offsets.binary_search_by(|v| {
            let mut entry = &self.block.data[*v as usize..];
            let key_len = entry.get_u16();
            let entry_key = &entry[..key_len as usize];
            entry_key.cmp(key.into_inner())
        });

        match pos {
            Ok(pos) => self.seek_to_idx(pos),
            Err(pos) if pos < self.block.offsets.len() => self.seek_to_idx(pos),
            _ => {
                self.key.clear();
            }
        }
    }
}
