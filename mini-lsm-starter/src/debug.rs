use crate::lsm_storage::{LsmStorageInner, MiniLsm};

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();
        // immutable memtables
        println!(
            "MT ({}): {:?}",
            snapshot.imm_memtables.len(),
            snapshot
                .imm_memtables
                .iter()
                .map(|x| x.id())
                .collect::<Vec<_>>()
        );
        // L0
        println!(
            "L0 ({}): {:?}",
            snapshot.l0_sstables.len(),
            snapshot.l0_sstables,
        );
        for (level, files) in &snapshot.levels {
            println!("L{level} ({}): {:?}", files.len(), files);
        }
    }
}

impl MiniLsm {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}
