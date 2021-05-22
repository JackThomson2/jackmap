use flize::{Atomic, Shared};

pub struct LeafNode<V> {
    pub key: usize,
    pub data: Atomic<V>,
    pub low: Atomic<LeafNode<V>>,
    pub high: Atomic<LeafNode<V>>,
}

impl<V> LeafNode<V> {
    pub fn new(key: usize, data: Shared<V>) -> Self {
        Self {
            key,
            data: Atomic::new(data),
            low: Atomic::null(),
            high: Atomic::null(),
        }
    }
}
