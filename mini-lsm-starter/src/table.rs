pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        buf.put_u32(block_meta.len() as u32);
        // offset (4B) | fkey_len (2B) | fkey | lkey_len (2B) | lkey
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: &mut impl Buf) -> Vec<BlockMeta> {
        let size = buf.get_u32() as usize;
        let mut metas = Vec::with_capacity(size);
        for _ in 0..size {
            let offset = buf.get_u32() as usize;
            let fkey_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(fkey_len));
            let lkey_len = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(lkey_len));

            let meta = BlockMeta {
                offset,
                first_key,
                last_key,
            };
            metas.push(meta);
        }
        metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let sst_size = file.size();

        let bloom_filter_offset = Bytes::from(file.read(sst_size - 4, 4)?).get_u32() as u64;
        let bloom_filter_bytes =
            Bytes::from(file.read(bloom_filter_offset, sst_size - bloom_filter_offset - 4)?);
        let bloom = Bloom::decode(&bloom_filter_bytes)?;

        let block_meta_offset =
            Bytes::from(file.read(bloom_filter_offset - 4, 4)?).get_u32() as u64;
        let mut block_meta_bytes = Bytes::from(file.read(
            block_meta_offset,
            bloom_filter_offset - block_meta_offset - 4,
        )?);
        let block_meta = BlockMeta::decode_block_meta(&mut block_meta_bytes);

        let first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
            block_meta.first().unwrap().first_key.raw_ref(),
        ));
        let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
            block_meta.last().unwrap().last_key.raw_ref(),
        ));

        let table = SsTable {
            file,
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0u64,
        };
        Ok(table)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            bail!("Invalid block index: {}", block_idx)
        }

        let block_start = self.block_meta.get(block_idx).unwrap().offset as u64;
        let block_end = self
            .block_meta
            .get(block_idx + 1)
            .map(|x| x.offset)
            .unwrap_or_else(|| self.block_meta_offset) as u64;

        // end offset for block data, excluding checksum
        let data_end = block_end - 4;

        // TODO could do one single read for both data and checksum to improve performance
        let data = &self.file.read(block_start, data_end - block_start)?;
        if crc32fast::hash(data) != Bytes::from(self.file.read(data_end, 4)?).get_u32() {
            bail!("Checksum mismatch for sst {}, block {}", self.id, block_idx);
        }

        Ok(Arc::new(Block::decode(data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = &self.block_cache {
            let key = (self.sst_id(), block_idx);
            cache
                .try_get_with(key, || self.read_block(block_idx))
                .map_err(|e| anyhow!(e.clone()))
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        if key < self.block_meta.first().unwrap().first_key.as_key_slice() {
            return 0;
        }

        // TODO use binary search
        // TODO how to handle duplicated entries
        for (idx, meta) in self.block_meta.iter().enumerate() {
            if key >= meta.first_key.as_key_slice() && key <= meta.last_key.as_key_slice() {
                return idx;
            }
        }

        // Returns out-of-range block id when key is not found
        // The iterator should be invalid if the key is not found
        self.block_meta.len()
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
