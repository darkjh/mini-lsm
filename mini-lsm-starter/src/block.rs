mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        let num_of_elements = self.offsets.len() as u16;
        buf.put_u16(num_of_elements);
        buf.into()
    }

    // TODO check bytes and its From/Into typeclasses
    // TODO check how bytes position works, why data need to be extracted after offsets
    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - 2..]).get_u16() as usize;
        let data_end = data.len() - 2 - num_of_elements * 2;

        let block_data = data[0..data_end].to_vec();
        let offsets = data[data_end..data.len() - 2]
            .chunks(2)
            .map(|mut x| x.get_u16())
            .collect::<Vec<u16>>();

        Block {
            data: block_data,
            offsets: offsets.to_vec(),
        }
    }
}
