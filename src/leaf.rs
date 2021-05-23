use flize::{Atomic, NullTag, Shared};

pub struct LeafNode<V> {
    pub key: usize,
    pub data: Atomic<V, NullTag, NullTag, 0, 0>,
    pub low: Atomic<LeafNode<V>, NullTag, NullTag, 0, 0>,
    pub high: Atomic<LeafNode<V>, NullTag, NullTag, 0, 0>,
}

impl<V> LeafNode<V> {
    #[inline]
    pub fn new(key: usize, data: Shared<V, NullTag, NullTag, 0, 0>) -> Self {
        Self {
            key,
            data: Atomic::new(data),
            low: Atomic::null(),
            high: Atomic::null(),
        }
    }

    pub fn empty_with_key(key: usize) -> Self {
        Self {
            key,
            data: Atomic::null(),
            low: Atomic::null(),
            high: Atomic::null(),
        }
    }
}
