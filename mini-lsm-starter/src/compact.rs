#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(_) => Err(anyhow::anyhow!("not implemented")),
            CompactionTask::Tiered(_) => Err(anyhow::anyhow!("not implemented")),
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => {
                let snapshot = { self.state.read().clone() };
                if let Some(_) = upper_level {
                    // non-L0 compaction
                    let upper_iters = SstConcatIterator::create_and_seek_to_first(
                        upper_level_sst_ids
                            .iter()
                            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                            .collect(),
                    )?;

                    let lower_iters = SstConcatIterator::create_and_seek_to_first(
                        lower_level_sst_ids
                            .iter()
                            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                            .collect(),
                    )?;

                    let iter = TwoMergeIterator::create(upper_iters, lower_iters)?;
                    self.sst_tables_from_iter(iter)
                } else {
                    // L0 compaction
                    // L0 is not a sorted run, cannot use a concat iterator
                    let l0_iters = {
                        let iters: Result<Vec<Box<SsTableIterator>>> = upper_level_sst_ids
                            .iter()
                            .map(|idx| {
                                let iter = SsTableIterator::create_and_seek_to_first(
                                    snapshot.sstables.get(idx).unwrap().clone(),
                                )?;
                                Ok(Box::new(iter))
                            })
                            .collect();
                        MergeIterator::create(iters?)
                    };

                    // lower level must be a sorted run
                    let lower_iters = SstConcatIterator::create_and_seek_to_first(
                        lower_level_sst_ids
                            .iter()
                            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                            .collect(),
                    )?;

                    let iter = TwoMergeIterator::create(l0_iters, lower_iters)?;
                    self.sst_tables_from_iter(iter)
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let snapshot = { self.state.read().clone() };
                let iter = {
                    let iters: Result<Vec<Box<SsTableIterator>>> = l0_sstables
                        .iter()
                        .chain(l1_sstables.iter())
                        .map(|idx| {
                            let iter = SsTableIterator::create_and_seek_to_first(
                                snapshot.sstables.get(idx).unwrap().clone(),
                            )?;
                            Ok(Box::new(iter))
                        })
                        .collect();
                    MergeIterator::create(iters?)
                };

                self.sst_tables_from_iter(iter)
            }
        }
    }

    fn build_sst_table(&self, builder: SsTableBuilder) -> Result<Arc<SsTable>> {
        let sst_id = self.next_sst_id();
        let sst = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        Ok(Arc::new(sst))
    }

    fn sst_tables_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut result = vec![];

        while iter.is_valid() {
            if !iter.value().is_empty() {
                builder.add(iter.key(), iter.value());
            }

            if builder.estimated_size() >= self.options.target_sst_size {
                let sst = self.build_sst_table(builder)?;
                result.push(sst);
                builder = SsTableBuilder::new(self.options.block_size);
            }

            iter.next()?;
        }

        let sst = self.build_sst_table(builder)?;
        result.push(sst);

        Ok(result)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (task, target_sst_tables) = {
            let guard = self.state.read();
            let l0s = guard.l0_sstables.clone();
            let l1s = guard.levels[0].1.clone();
            let all: Vec<usize> = l0s.iter().chain(l1s.iter()).cloned().collect();

            (
                CompactionTask::ForceFullCompaction {
                    l0_sstables: l0s,
                    l1_sstables: l1s,
                },
                all,
            )
        };
        let sstables = self.compact(&task)?;

        {
            let _lock = self.state_lock.lock();
            let mut guard = self.state.write();

            let mut state_snapshot = guard.as_ref().clone();

            // Remove compacted sstables from state structure
            state_snapshot
                .l0_sstables
                .retain(|id| !&target_sst_tables.contains(id));
            for sst_id in &target_sst_tables {
                state_snapshot.sstables.remove(sst_id);
            }

            let mut new_l1_sstables = vec![];
            for sst in sstables {
                state_snapshot.sstables.insert(sst.sst_id(), sst.clone());
                new_l1_sstables.push(sst.sst_id());
            }
            state_snapshot.levels = vec![(1, new_l1_sstables)];

            *guard = Arc::new(state_snapshot);
        }

        // remove compacted sstables from disk
        for sst_id in target_sst_tables {
            let sst = self.path_of_sst(sst_id);
            std::fs::remove_file(sst)?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let task = {
            let snapshot = self.state.read();
            self.compaction_controller
                .generate_compaction_task(&snapshot)
        };

        if let Some(task) = task {
            println!("running compaction task: {:?}", task);
            let new_ssts = self.compact(&task)?;
            let new_sst_ids = new_ssts.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();
            let mut to_delete = vec![];
            {
                let _lock = self.state_lock.lock();
                let mut snapshot = self.state.write();
                let (mut new_state, sstables_to_delete) = self
                    .compaction_controller
                    .apply_compaction_result(&snapshot, &task, &new_sst_ids);

                // update `sstable` map in the new state
                // other structures has already been updated by the compaction controller
                for sst in new_ssts {
                    let _ = new_state.sstables.insert(sst.sst_id(), sst.clone());
                }
                for sst_id in sstables_to_delete {
                    let _ = new_state.sstables.remove(&sst_id);
                    to_delete.push(sst_id);
                }
                *snapshot = Arc::new(new_state);
            }

            // remove compacted sstables from disk
            for sst_id in to_delete {
                let sst = self.path_of_sst(sst_id);
                std::fs::remove_file(sst)?;
            }
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let memtable_num = {
            let guard = self.state.read();
            guard.imm_memtables.len()
        };

        if memtable_num >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
