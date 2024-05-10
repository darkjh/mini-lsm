use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction
    /// task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // TODO if l0 has 2 tables, but l1 has 4 tables, should we compact ??? What order to apply the rules ???
        // check l0 size trigger
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            });
        }

        // check size trigger, from level 1 to max level
        let mut upper_level = 1usize;
        while upper_level <= self.options.max_levels - 1 {
            let upper_size = snapshot.levels[upper_level - 1].1.len();
            let lower_size = snapshot.levels[upper_level].1.len();

            if self.should_trigger_compaction(upper_size, lower_size) {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(upper_level),
                    upper_level_sst_ids: snapshot.levels[upper_level - 1].1.clone(),
                    lower_level: upper_level + 1,
                    lower_level_sst_ids: snapshot.levels[upper_level].1.clone(),
                    is_lower_level_bottom_level: upper_level == self.options.max_levels - 1,
                });
            }
            upper_level += 1;
        }

        None
    }

    fn should_trigger_compaction(&self, upper_size: usize, lower_size: usize) -> bool {
        if upper_size == 0 {
            return false;
        } else {
            (lower_size as f64 / upper_size as f64) * 100f64
                < self.options.size_ratio_percent as f64
        }
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids
    /// generated. This function applies the result and generates a new LSM state. The functions
    /// should only change `l0_sstables` and `levels` without changing memtables and `sstables`
    /// hash map. Though there should only be one thread running compaction jobs, you should think
    /// about the case where an L0 SST gets flushed while the compactor generates new SSTs, and with
    /// that in mind, you should do some sanity checks in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        let mut sst_to_delete = vec![];

        // update L0 tables
        if let None = task.upper_level {
            let mut new_l0_sstables = vec![];
            for sst_id in snapshot.l0_sstables.iter() {
                if !task.upper_level_sst_ids.contains(sst_id) {
                    new_l0_sstables.push(*sst_id);
                }
            }
            new_state.l0_sstables = new_l0_sstables;
            sst_to_delete.extend(task.upper_level_sst_ids.iter());
        }

        // update non-L0, upper level tables
        if let Some(upper_level) = task.upper_level {
            // update upper level tables
            new_state.levels[upper_level - 1].1.clear();
            sst_to_delete.extend(task.upper_level_sst_ids.iter());
        }

        // update lower level tables
        let lower_level = task.lower_level;
        new_state.levels[lower_level - 1].1.clear();
        new_state.levels[lower_level - 1].1.extend(output.iter());
        sst_to_delete.extend(task.lower_level_sst_ids.iter());

        (new_state, sst_to_delete)
    }
}
