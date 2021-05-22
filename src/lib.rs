#![feature(stdsimd)]

use ahash::RandomState;
use flize::{Atomic, Collector, Shared, ThinShield};
use std::{
    hash::{BuildHasher, Hash, Hasher},
    sync::atomic::Ordering::{Acquire, Relaxed, SeqCst},
    usize, vec,
};

mod leaf;
mod x86;

use leaf::LeafNode;
pub type DefaultHashBuilder = ahash::RandomState;

#[derive(Debug)]
struct Bucket<V> {
    head: Atomic<LeafNode<V>>,
    collector: Collector,
}

impl<'b, V> Bucket<V>
where
    V: Clone,
{
    #[inline]
    fn find_item<'g>(&self, key: usize, guard: &'g ThinShield) -> Option<Shared<'g, LeafNode<V>>> {
        unsafe {
            let mut checking = self.head.load(Relaxed, guard);

            loop {
                if checking.is_null() {
                    return None;
                }

                let leaf = checking.as_mut_ref_unchecked();
                if leaf.key == key {
                    return Some(checking);
                }

                checking = leaf.next.load(Relaxed, guard)
            }
        }
    }

    #[inline]
    pub fn insert_item(&self, key: usize, value: V) {
        let shield = self.collector.thin_shield();

        let new =
            unsafe { Shared::from_ptr(Box::into_raw(Box::new(LeafNode::new(key, value.clone())))) };

        loop {
            let head = self.head.load(Relaxed, &shield);

            if head.is_null() {
                match self
                    .head
                    .compare_exchange(head, new, Acquire, Relaxed, &shield)
                {
                    Ok(_) => return,
                    Err(_owned) => continue,
                }
            }

            unsafe {
                let mut above = head;
                let mut checking = head;

                loop {
                    if checking.is_null() {
                        above.as_mut_ref_unchecked().next.store(new, SeqCst);
                        return;
                    }

                    let item = checking.as_mut_ref_unchecked();

                    if item.key == key {
                        item.data = value.clone();
                        return;
                    }

                    above = checking;
                    checking = checking.as_ref_unchecked().next.load(Acquire, &shield)
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
        shield: &'a ThinShield,
    ) -> Option<Shared<'a, LeafNode<V>>> {
        match self.find_item(key, shield) {
            Some(index) => Some(index),
            None => None,
        }
    }
}

impl<V> Default for Bucket<V> {
    fn default() -> Self {
        Self {
            head: Atomic::null(),
            collector: Collector::new(),
        }
    }
}

pub struct JackMap<V, S = DefaultHashBuilder> {
    size: usize,

    num_buckets: usize,
    buckets: Vec<Bucket<V>>,

    hasher: S,
}

impl<'a, V> JackMap<V, DefaultHashBuilder>
where
    V: 'a + Clone,
{
    pub fn new(num_buckets: usize) -> Self {
        let mut buckets = Vec::with_capacity(num_buckets);

        for _i in 0..num_buckets {
            buckets.push(Bucket::default())
        }

        let hasher = RandomState::new();

        Self {
            size: 0,
            num_buckets,
            buckets,
            hasher,
        }
    }

    #[inline]
    fn determine_bucket(&self, hash: usize) -> usize {
        hash % self.num_buckets
    }

    #[inline]
    fn hash_key<K: 'a + Hash>(&self, key: &K) -> usize {
        let mut hashing = self.hasher.build_hasher();
        key.hash(&mut hashing);
        hashing.finish() as usize
    }

    #[inline]
    pub fn insert<K: 'a + Hash>(&mut self, key: &K, value: V) {
        let key = self.hash_key(key);
        let bucket = self.determine_bucket(key);

        unsafe {
            self.buckets
                .get_unchecked_mut(bucket)
                .insert_item(key, value);
        }

        self.size += 1;
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn get<K: 'a + Hash>(&self, key: &K) -> Option<V> {
        let key = self.hash_key(key);
        let bucket = self.determine_bucket(key);

        let node = unsafe { self.buckets.get_unchecked(bucket) };

        let shield = node.get_shield();

        unsafe {
            match node.try_get(key, &shield) {
                Some(res) => Some(res.as_ref_unchecked().data.clone()),
                None => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::JackMap;

    #[test]
    fn it_works() {
        let mut jacktable = JackMap::new(200);
        const INSTERT_COUNT: usize = 100_000;

        for i in 0..INSTERT_COUNT {
            let string = format!("Key {}", i);
            jacktable.insert(&string, i);
        }

        println!("JackTable size is {}", jacktable.size());

        for i in 0..INSTERT_COUNT {
            let string = format!("Key {}", i);

            let found = jacktable.get(&string).unwrap();

            //assert_eq!(found, i);
        }

        println!("{:?}", jacktable.get(&"Key 0"));
        jacktable.insert(&"Key 0", 123456);
        println!("{:?}", jacktable.get(&"Key 0"));
    }
}
