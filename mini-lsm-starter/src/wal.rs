use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};
use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let wal_file = OpenOptions::new()
            .read(true)
            .create_new(true)
            .write(true)
            .open(path)
            .context("failed to create WAL file")?;
        let writer = BufWriter::new(wal_file);
        Ok(Self {
            file: Arc::new(Mutex::new(writer)),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut wal_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to open WAL file")?;

        let mut buf = Vec::new();
        // read WAL into memory since its size is upper bounded by memtable size
        wal_file.read_to_end(&mut buf)?;

        let mut buf = buf.as_slice();
        while buf.has_remaining() {
            let batch_size = buf.get_u32() as usize;
            let mut kvs = Vec::with_capacity(batch_size);
            let mut hasher = crc32fast::Hasher::new();

            for _ in 0..batch_size {
                hasher.update(&buf[..2]);
                let key_len = buf.get_u16() as usize;
                hasher.update(&buf[..key_len]);
                let key_bytes = Bytes::copy_from_slice(&buf[..key_len]);
                buf.advance(key_len);

                hasher.update(&buf[..8]);
                let ts = buf.get_u64();
                let key = KeyBytes::from_bytes_with_ts(key_bytes, ts);

                hasher.update(&buf[..2]);
                let value_len = buf.get_u16() as usize;
                hasher.update(&buf[..value_len]);
                let value = Bytes::copy_from_slice(&buf[..value_len]);
                buf.advance(value_len);

                kvs.push((key, value));
            }
            let checksum = buf.get_u32();
            if checksum != hasher.finalize() {
                bail!("WAL record checksum mismatch");
            }

            for (key, value) in kvs {
                skiplist.insert(key, value);
            }
        }

        let wal = Self {
            file: Arc::new(Mutex::new(BufWriter::new(wal_file))),
        };
        Ok(wal)
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    /// WAL format
    /// |   HEADER   |                          BODY                                      |  FOOTER  |
    /// |     u32    |   u16   | var | u64 |    u16    |  var  |           ...            |    u32   |
    /// | batch_size | key_len | key | ts  | value_len | value | more key-value pairs ... | checksum |
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut writer = self.file.lock();
        let batch_size = data.len() as u32;
        // header + footer
        let mut data_size = std::mem::size_of::<u32>() * 2;
        // count body size
        for (key, value) in data {
            let size = key.key_len()
                + std::mem::size_of::<u64>()
                + value.len()
                + std::mem::size_of::<u16>() * 2;
            data_size += size;
        }
        let mut buf: Vec<u8> = Vec::with_capacity(data_size + 4);

        buf.put_u32(batch_size);

        for (key, value) in data {
            buf.put_u16(key.key_len() as u16);
            buf.put(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value.len() as u16);
            buf.put(*value);
        }
        let end = buf.len();
        // checksum of body, excluding header and footer (itself)
        let checksum = crc32fast::hash(&buf[4..end]);
        buf.put_u32(checksum);

        writer.write_all(&buf).context("failed to write to WAL")?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        writer.flush().context("failed to flush WAL")?;
        writer.get_mut().sync_all().context("failed to sync WAL")?;
        Ok(())
    }
}
