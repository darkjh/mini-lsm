use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    should_read_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let should_read_a = Self::should_read_a(&a, &b);
        Ok(TwoMergeIterator {
            a,
            b,
            should_read_a,
        })
    }

    fn should_read_a(a: &A, b: &B) -> bool {
        if !a.is_valid() {
            false
        } else if !b.is_valid() {
            true
        } else if !a.is_valid() && !b.is_valid() {
            // does not matter, the merged iterator is simply not valid
            true
        } else {
            a.key() <= b.key()
        }
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.should_read_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.should_read_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.should_read_a {
            // skip values in b if necessary
            if self.a.is_valid() && self.b.is_valid() {
                if self.a.key() == self.b.key() {
                    self.a.next()?;
                    self.b.next()?;
                } else {
                    self.a.next()?
                }
            } else {
                self.a.next()?;
            }
        } else if self.b.is_valid() {
            self.b.next()?;
        }

        self.should_read_a = Self::should_read_a(&self.a, &self.b);
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
