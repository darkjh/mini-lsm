use parking_lot::Mutex;
use tempfile::tempdir;

use crate::manifest::{Manifest, ManifestRecord};
use crate::{
    compact::{
        CompactionOptions, CompactionTask, LeveledCompactionOptions,
        SimpleLeveledCompactionOptions, TieredCompactionOptions,
    },
    lsm_storage::{LsmStorageOptions, MiniLsm},
    tests::harness::dump_files_in_dir,
};

#[test]
fn test_manifest_round_trip() {
    fn records() -> Vec<ManifestRecord> {
        vec![
            ManifestRecord::Flush(1),
            ManifestRecord::NewMemtable(2),
            ManifestRecord::Compaction(
                CompactionTask::ForceFullCompaction {
                    l0_sstables: vec![3, 4],
                    l1_sstables: vec![1, 2],
                },
                vec![5, 6],
            ),
        ]
    }

    let dir = tempdir().unwrap();
    let manifest_path = dir.path().join("manifest");
    let manifest = Manifest::create(&manifest_path).unwrap();

    {
        let lock = Mutex::new(());
        for record in records() {
            manifest.add_record(&lock.lock(), record).unwrap();
        }
    }

    let (_, recovered_records) = Manifest::recover(&manifest_path).unwrap();
    assert_eq!(records(), recovered_records);
}

#[test]
fn test_integration_leveled() {
    test_integration(CompactionOptions::Leveled(LeveledCompactionOptions {
        level_size_multiplier: 2,
        level0_file_num_compaction_trigger: 2,
        max_levels: 3,
        base_level_size_mb: 1,
    }))
}

#[test]
fn test_integration_tiered() {
    test_integration(CompactionOptions::Tiered(TieredCompactionOptions {
        num_tiers: 3,
        max_size_amplification_percent: 200,
        size_ratio: 1,
        min_merge_width: 3,
    }))
}

#[test]
fn test_integration_simple() {
    test_integration(CompactionOptions::Simple(SimpleLeveledCompactionOptions {
        size_ratio_percent: 200,
        level0_file_num_compaction_trigger: 2,
        max_levels: 3,
    }));
}

fn test_integration(compaction_options: CompactionOptions) {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(
        &dir,
        LsmStorageOptions::default_for_week2_test(compaction_options.clone()),
    )
    .unwrap();
    for i in 0..=20 {
        storage.put(b"0", format!("v{}", i).as_bytes()).unwrap();
        if i % 2 == 0 {
            storage.put(b"1", format!("v{}", i).as_bytes()).unwrap();
        } else {
            storage.delete(b"1").unwrap();
        }
        if i % 2 == 1 {
            storage.put(b"2", format!("v{}", i).as_bytes()).unwrap();
        } else {
            storage.delete(b"2").unwrap();
        }
        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
    }
    storage.close().unwrap();
    // ensure all SSTs are flushed
    assert!(storage.inner.state.read().memtable.is_empty());
    assert!(storage.inner.state.read().imm_memtables.is_empty());
    storage.dump_structure();
    drop(storage);
    dump_files_in_dir(&dir);

    let storage = MiniLsm::open(
        &dir,
        LsmStorageOptions::default_for_week2_test(compaction_options.clone()),
    )
    .unwrap();
    assert_eq!(&storage.get(b"0").unwrap().unwrap()[..], b"v20".as_slice());
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"v20".as_slice());
    assert_eq!(storage.get(b"2").unwrap(), None);
}
