use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let space_amplification = if snapshot.levels.len() > 1 {
            let mut total_size = 0;
            for (_, files) in &snapshot.levels {
                total_size += files.len();
            }
            let last_level_size = snapshot.levels.last().unwrap().1.len();
            (total_size - last_level_size) as f64 / last_level_size as f64
        } else {
            0f64
        };

        // trigger for space amplification ratio
        if space_amplification * 100f64 >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amplification * 100f64
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        };

        // trigger for size ratio
        let mut previous_total_size = 0usize;
        let mut num_tiers = 0usize;
        for (idx, (_, ssts)) in snapshot.levels.iter().enumerate() {
            num_tiers += 1;
            if previous_total_size as f64 / ssts.len() as f64
                >= (100f64 + self.options.size_ratio as f64) / 100f64
                && num_tiers >= self.options.min_merge_width
            {
                println!(
                    "compaction triggered by size ratio: {}",
                    previous_total_size as f64 / ssts.len() as f64 * 100f64
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..idx + 1].to_vec(),
                    bottom_tier_included: idx == snapshot.levels.len() - 1,
                });
            } else {
                previous_total_size += ssts.len();
            }
        }

        // finally, limited to num_tiers sorted runs
        if snapshot.levels.len() >= self.options.num_tiers {
            println!("compaction triggered to reduce number of sorted runs");
            return Some(TieredCompactionTask {
                tiers: snapshot.levels[..2].to_vec(),
                bottom_tier_included: false,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        let sst_to_delete = task
            .tiers
            .iter()
            .flat_map(|(_, ssts)| ssts.iter())
            .cloned()
            .collect();
        let compacted_levels = task
            .tiers
            .iter()
            .map(|(level, _)| level)
            .collect::<HashSet<_>>();

        let mut new_tiers = vec![];
        let mut inserted = false;
        for (level, ssts) in &snapshot.levels {
            if !compacted_levels.contains(level) && !inserted {
                // newly flushed sst, in its own tier
                new_tiers.push((*level, ssts.clone()));
            } else if !compacted_levels.contains(level) && inserted {
                // carry over this tier
                new_tiers.push((*level, ssts.clone()));
            } else {
                // replace compacted tiers
                if !inserted {
                    new_tiers.push((*output.first().unwrap(), output.to_vec()));
                    inserted = true
                }

                if task.bottom_tier_included {
                    break;
                }
            }
        }

        new_state.levels = new_tiers;
        (new_state, sst_to_delete)
    }
}
