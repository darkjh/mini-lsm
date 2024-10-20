use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let current_size = self.data.len() + self.offsets.len() * 2;

        // key_len + value_len + offset_len(2 bytes) + sizes * 2 (2 bytes each)
        let size = key.raw_len() + value.len() + 6;
        if current_size + size > self.block_size && !self.data.is_empty() {
            return false;
        }

        // handle offsets
        self.offsets.push(self.data.len() as u16);

        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);

            self.data.put_u16(key.key_len() as u16);
            self.data.put_u16(0u16);
            self.data.put(key.key_ref());
            self.data.put_u64(key.ts());
        } else {
            // key prefix encoding
            // first value would have full overlap and 0 rest_key_len
            let overlap = self.compute_key_overlap(key);
            let rest_key_len = key.key_len() - overlap;

            self.data.put_u16(overlap as u16);
            self.data.put_u16(rest_key_len as u16);
            self.data.put(&key.key_ref()[overlap..]);
            self.data.put_u64(key.ts());
        }

        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        true
    }

    fn compute_key_overlap(&self, key: KeySlice) -> usize {
        let mut i = 0;
        while i < self.first_key.key_len() && i < key.key_len() {
            if self.first_key.key_ref()[i] != key.key_ref()[i] {
                break;
            }
            i += 1;
        }
        i
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
