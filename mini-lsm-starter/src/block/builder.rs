use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    block_size: usize,
    current_size: usize,
    kvs: Vec<(Vec<u8>, Vec<u8>)>
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            block_size,
            current_size: 0,
            kvs: Vec::new()
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        // key_len + value_len + offset_len(2 bytes) + sizes * 2 (2 bytes each)
        let size = key.len() + value.len() + 6;
        if self.current_size + size > self.block_size {
            return false;
        }

        self.current_size += size;
        self.kvs.push((key.to_vec(), value.to_vec()));
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.kvs.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        let mut current_offset = 0u16;
        for (key, value) in self.kvs {
            let key_len = u16::try_from(key.len()).unwrap();
            let value_len = u16::try_from(value.len()).unwrap();
            data.extend_from_slice(&key_len.to_ne_bytes());
            data.extend_from_slice(&key);
            data.extend_from_slice(&value_len.to_ne_bytes());
            data.extend_from_slice(&value);
            offsets.push(current_offset);
            current_offset += key_len + value_len + 4;
        }
        Block {
            data,
            offsets
        }
    }
}
