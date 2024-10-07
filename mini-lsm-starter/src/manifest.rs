use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::compact::CompactionTask;
use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create manifest")?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover manifest")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut records = Vec::new();
        let mut buf = buf.as_slice();
        while buf.has_remaining() {
            let record_len = buf.get_u64() as usize;

            let record_bytes = &buf[..record_len];
            buf.advance(record_len);
            let checksum = buf.get_u32();
            if checksum != crc32fast::hash(record_bytes) {
                bail!("checksum mismatched!");
            }

            let record = serde_json::from_slice::<ManifestRecord>(record_bytes)?;
            records.push(record);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();

        let mut record_bytes = serde_json::to_vec(&record)?;
        let record_len = record_bytes.len() as u64;
        let checksum = crc32fast::hash(&record_bytes);
        record_bytes.put_u32(checksum);

        file.write_all(&record_len.to_be_bytes())?;
        file.write_all(&record_bytes)?;
        file.sync_all()?;
        Ok(())
    }
}
