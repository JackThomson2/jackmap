use flize::Atomic;

pub struct LeafNode<V> {
    pub key: usize,
    pub data: V,
    pub next: Atomic<LeafNode<V>>,
}

impl<V> LeafNode<V> {
    pub fn new(key: usize, data: V) -> Self {
        Self {
            key,
            data,
            next: Atomic::null(),
        }
    }
}
