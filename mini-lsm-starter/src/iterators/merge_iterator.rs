use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator> {
    pub gen: usize,
    pub iter: Box<I>,
}

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.iter.key().cmp(&other.iter.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.gen.partial_cmp(&other.gen),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.iter
            .key()
            .cmp(&other.iter.key())
            .then(self.gen.cmp(&other.gen))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return MergeIterator::invalid();
        }

        let mut gen = 0;
        let mut heap = BinaryHeap::with_capacity(iters.len());

        for iter in iters {
            if iter.is_valid() {
                let item = HeapWrapper { gen, iter };
                gen += 1;
                heap.push(item);
            }
        }

        // all iters are invalid
        if heap.is_empty() {
            return MergeIterator::invalid();
        }

        let current = heap.pop();
        MergeIterator {
            iters: heap,
            current,
        }
    }

    fn invalid() -> Self {
        MergeIterator {
            iters: BinaryHeap::new(),
            current: None,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().iter.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().iter.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        // if there are the same key in other iterators, drop them
        let current_key = self.current.as_ref().unwrap().iter.key();
        while let Some(mut peek) = self.iters.peek_mut() {
            if peek.iter.key() == current_key {
                match peek.iter.next() {
                    Ok(_) => {
                        if !peek.iter.is_valid() {
                            // remove invalid iterator from the heap
                            PeekMut::pop(peek);
                        }
                    }
                    e @ Err(_) => return e,
                }
            } else {
                break;
            }
        }

        let mut current_iter = self.current.take().unwrap();
        match current_iter.iter.next() {
            Ok(_) => {
                // make sure only valid iterator is pushed back into the heap
                if current_iter.iter.is_valid() {
                    self.iters.push(current_iter);
                }
            }
            e @ Err(_) => return e,
        }

        let mut next = self.iters.pop().take();
        while next.is_some() {
            let n = next.unwrap();
            if n.iter.is_valid() {
                let _ = self.current.insert(n);
                break;
            } else {
                next = self.iters.pop().take();
            }
        }
        Ok(())
    }
}
