use std::collections::BTreeMap;

// keep track of the lowest timestamp being used by transactions
pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|c| *c += 1).or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let v = self.readers.get_mut(&ts);
        if let Some(c) = v {
            *c -= 1;
            if *c == 0 {
                self.readers.remove(&ts);
            }
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|(ts, _)| *ts)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
