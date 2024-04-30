use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;
use bytes::Bytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    // bloom filter for sst filtering
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.extend(key.into_inner());
        }

        if !self.builder.add(key, value) {
            // current block is full
            self.finish_current_block();
            // it's guaranteed to success for the first entry
            let _ = self.builder.add(key, value);
        }
        self.key_hashes
            .push(farmhash::fingerprint32(key.into_inner()));

        self.last_key.clear();
        self.last_key.extend(key.into_inner())
    }

    fn finish_current_block(&mut self) {
        let new_builder = BlockBuilder::new(self.block_size);
        let prev_builder = std::mem::replace(&mut self.builder, new_builder);

        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
        };
        self.meta.push(meta);
        self.data.put(prev_builder.build().encode());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_current_block();

        let block_meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u32(block_meta_offset as u32);

        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);

        let bloom_filter_offset = self.data.len();
        bloom.encode(&mut self.data);
        self.data.put_u32(bloom_filter_offset as u32);

        let file = FileObject::create(path.as_ref(), self.data)?;

        let first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
            self.meta.first().unwrap().first_key.raw_ref(),
        ));
        let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
            self.meta.last().unwrap().last_key.raw_ref(),
        ));

        let sst = SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0u64,
        };
        Ok(sst)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
