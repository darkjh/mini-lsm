use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let target_level_sizes = self.target_level_sizes(snapshot);

        // always check L0 compactions first
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // determine the lower level to compact with
            let lower_level_idx = target_level_sizes.iter().position(|x| *x > 0).unwrap();

            println!(
                "compaction triggered by L0 file num trigger, targets {:?}",
                target_level_sizes
            );

            let upper_ssts = snapshot.l0_sstables.clone();
            let lower_ssts = self.find_overlapping_ssts(snapshot, &upper_ssts, lower_level_idx + 1);

            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: upper_ssts,
                lower_level: lower_level_idx + 1,
                lower_level_sst_ids: lower_ssts,
                is_lower_level_bottom_level: lower_level_idx == snapshot.levels.len() - 1,
            });
        }

        let priorities = target_level_sizes
            .iter()
            .enumerate()
            .map(|(idx, target_size)| {
                let current_size = snapshot.levels[idx]
                    .1
                    .iter()
                    .map(|x| snapshot.sstables.get(x).unwrap().file.size())
                    .sum::<u64>();
                if current_size == 0 {
                    (idx, 0f64)
                } else {
                    (idx, current_size as f64 / (*target_size as f64))
                }
            })
            .collect::<Vec<(usize, f64)>>();

        println!("priorities: {:?}", priorities);

        let target_level = priorities
            .iter()
            .max_by(|x, y| x.1.total_cmp(&y.1))
            .unwrap();

        if target_level.1 > 1f64 {
            let level = target_level.0 + 1;
            let ratio = target_level.1;
            println!(
                "compaction triggered by priority ratio, target level {} with ratio {}",
                level, ratio
            );

            // chose oldest sst in target level to compact
            let upper_ssts = vec![*snapshot.levels[level - 1].1.iter().min().unwrap()];
            let lower_ssts = self.find_overlapping_ssts(snapshot, &upper_ssts, level + 1);

            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: upper_ssts,
                lower_level: level + 1,
                lower_level_sst_ids: lower_ssts,
                is_lower_level_bottom_level: level == snapshot.levels.len() - 1,
            });
        }

        None
    }

    // Target size in bytes for each level
    fn target_level_sizes(&self, snapshot: &LsmStorageState) -> Vec<u64> {
        let last_level_size: u64 = snapshot
            .levels
            .last()
            .unwrap()
            .1
            .iter()
            .map(|x| snapshot.sstables.get(x).clone().unwrap().file.size())
            .sum();
        let base_level_size = self.options.base_level_size_mb as u64 * 1024 * 1024;

        // last level size not yet exceed `base_level_size_mb`
        // ssts should be place directly into the last level
        if last_level_size <= base_level_size {
            let mut targets = vec![0; snapshot.levels.len()];
            let last_idx = targets.len() - 1;
            targets[last_idx] = base_level_size;
            return targets;
        }

        let mut targets = vec![last_level_size; snapshot.levels.len()];
        let mut has_lowest_target = false;
        for i in (1..snapshot.levels.len()).rev() {
            let mut target = targets[i] / self.options.level_size_multiplier as u64;
            if target < base_level_size {
                if has_lowest_target {
                    // all upper levels should be skipped
                    target = 0u64;
                } else {
                    has_lowest_target = true;
                }
            }
            targets[i - 1] = target;
        }
        return targets;
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let mut result = HashSet::new();
        let ssts = sst_ids
            .iter()
            .map(|x| snapshot.sstables.get(x).unwrap())
            .collect::<Vec<_>>();
        let range_start = ssts.iter().map(|x| x.first_key()).min().unwrap();
        let range_end = ssts.iter().map(|x| x.last_key()).max().unwrap();

        for lower_sst_id in snapshot.levels[in_level - 1].1.iter() {
            let lower_sst = snapshot.sstables.get(lower_sst_id).unwrap();
            // check if intersection
            // x_start <= y_end && y_start <= x_end
            if range_start <= lower_sst.last_key() && lower_sst.first_key() <= range_end {
                result.insert(*lower_sst_id);
            }
        }
        result.into_iter().collect()
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        _in_recovery: bool,
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
            new_state.levels[upper_level - 1]
                .1
                .retain(|x| !task.upper_level_sst_ids.contains(x));
            sst_to_delete.extend(task.upper_level_sst_ids.iter());
        }

        // update lower level tables
        let lower_level = task.lower_level;
        let new_lower_level = &mut new_state.levels[lower_level - 1].1;

        new_lower_level.retain(|x| !task.lower_level_sst_ids.contains(x));
        new_lower_level.extend(output.iter());
        // TODO find another way to keep the order
        new_lower_level.sort_by_key(|x| new_state.sstables.get(x).unwrap().first_key());

        sst_to_delete.extend(task.lower_level_sst_ids.iter());
        (new_state, sst_to_delete)
    }
}
