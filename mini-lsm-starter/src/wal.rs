use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
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
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&buf[..2]);
            let key_len = buf.get_u16() as usize;
            hasher.update(&buf[..key_len]);
            let key = Bytes::copy_from_slice(&buf[..key_len]);
            buf.advance(key_len);

            hasher.update(&buf[..2]);
            let value_len = buf.get_u16() as usize;
            hasher.update(&buf[..value_len]);
            let value = Bytes::copy_from_slice(&buf[..value_len]);
            buf.advance(value_len);

            let checksum = buf.get_u32();
            if checksum != hasher.finalize() {
                bail!("WAL record checksum mismatch");
            }

            skiplist.insert(key, value);
        }

        let wal = Self {
            file: Arc::new(Mutex::new(BufWriter::new(wal_file))),
        };
        Ok(wal)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut writer = self.file.lock();
        let data_size = key.len() + value.len() + std::mem::size_of::<u16>() * 2;
        // data (data_size) | checksum (4B)
        let mut buf: Vec<u8> = Vec::with_capacity(data_size + 4);
        buf.put_u16(key.len() as u16);
        buf.put(key);
        buf.put_u16(value.len() as u16);
        buf.put(value);

        let end = buf.len();
        let checksum = crc32fast::hash(&buf[..end]);
        buf.put_u32(checksum);

        writer.write_all(&buf).context("failed to write to WAL")?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        writer.flush().context("failed to flush WAL")?;
        writer.get_mut().sync_all().context("failed to sync WAL")?;
        Ok(())
    }
}
