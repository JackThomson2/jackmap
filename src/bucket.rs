use crate::leaf::LeafNode;
use flize::{Atomic, Collector, NullTag, Shared, ThinShield};
use std::{
    hash::Hasher,
    sync::atomic::Ordering::{Acquire, Relaxed},
    usize,
};
#[derive(Debug)]
pub struct Bucket<V> {
    head: Atomic<LeafNode<V>, NullTag, NullTag, 0, 0>,
    collector: Collector,
}

impl<'b, V> Bucket<V>
where
    V: Clone,
{
    #[inline]
    pub fn find_item<'g>(
        &self,
        key: usize,
    ) -> Option<Shared<'g, LeafNode<V>, NullTag, NullTag, 0, 0>> {
        unsafe {
            let danger = flize::unprotected();
            let mut checking = self.head.load(Relaxed, danger);

            loop {
                if checking.is_null() {
                    return None;
                }

                let leaf = checking.as_ref_unchecked();
                let leaf_key = leaf.key.load(Acquire);
                if leaf_key == key {
                    return Some(checking);
                }

                checking = if leaf_key > key {
                    leaf.low.load(Relaxed, danger)
                } else {
                    leaf.high.load(Relaxed, danger)
                };
            }
        }
    }

    #[inline]
    pub fn insert_item(&self, key: usize, value: V) -> bool {
        let shield = unsafe { flize::unprotected() };
        let data = unsafe { Shared::from_ptr(Box::into_raw(Box::new(value))) };
        let mut item = None;

        'outer: loop {
            let head = self.head.load(Acquire, shield);

            if head.is_null() {
                if item.is_none() {
                    item = Some(unsafe {
                        Shared::from_ptr(Box::into_raw(Box::new(LeafNode::new(key, data))))
                    });
                }
                match self
                    .head
                    .compare_exchange(head, item.unwrap(), Acquire, Relaxed, shield)
                {
                    Ok(_) => return true,
                    Err(_owned) => continue,
                }
            }

            unsafe {
                let mut above = head.as_ref_unchecked();
                let mut checking = above;

                let leaf_key = checking.key.load(Acquire);
                if leaf_key == key {
                    checking.data.store(data, Relaxed);
                    return false;
                }

                let mut low = leaf_key > key;

                let mut from_low = low;
                let mut top_level = true;

                loop {
                    let looking_at = if low { &checking.low } else { &checking.high };
                    let looking_at_cell = looking_at.load(Relaxed, shield);

                    if looking_at_cell.is_null() {
                        if item.is_none() {
                            item = Some(Shared::from_ptr(Box::into_raw(Box::new(LeafNode::new(
                                key, data,
                            )))));
                        }

                        match looking_at.compare_exchange(
                            looking_at_cell,
                            item.unwrap(),
                            Acquire,
                            Relaxed,
                            shield,
                        ) {
                            Ok(_) => {
                                return true;
                            }
                            Err(_) => {
                                // cowardly backout
                                if top_level {
                                    continue 'outer;
                                }

                                checking = if from_low {
                                    above.low.load(Relaxed, shield).as_ref_unchecked()
                                } else {
                                    above.high.load(Relaxed, shield).as_ref_unchecked()
                                };

                                continue;
                            }
                        }
                    }

                    top_level = false;
                    from_low = low;

                    above = checking;
                    checking = looking_at_cell.as_ref_unchecked();

                    let mut leaf_key = checking.key.load(Acquire);

                    if leaf_key == usize::MAX {
                        match checking
                            .key
                            .compare_exchange(leaf_key, key, Acquire, Relaxed)
                        {
                            Ok(_) => {
                                checking.data.store(data, Relaxed);
                                return true;
                            }
                            Err(current) => {
                                leaf_key = current;
                            }
                        }
                    }

                    if leaf_key == key {
                        checking.data.store(data, Relaxed);
                        return false;
                    }

                    low = leaf_key > key;
                }
            }
        }
    }

    #[inline]
    pub fn get_shield(&self) -> ThinShield {
        self.collector.thin_shield()
    }

    #[inline]
    pub fn try_get<'a>(
        &self,
        key: usize,
    ) -> Option<Shared<'a, LeafNode<V>, NullTag, NullTag, 0, 0>> {
        self.find_item(key)
    }
}

impl<V> Default for Bucket<V> {
    fn default() -> Self {
        const MID_POINT: usize = usize::MAX / 2;

        // Used to get a nice start point for the splitting
        let item = unsafe {
            Shared::from_ptr(Box::into_raw(Box::new(LeafNode::empty_with_key(MID_POINT))))
        };

        Self {
            head: Atomic::new(item),
            collector: Collector::new(),
        }
    }
}
