#![feature(stdsimd)]

use ahash::RandomState;
use arrayvec::ArrayVec;
use flize::{Atomic, Collector, NullTag, Shared, ThinShield};
use std::{
    hash::{BuildHasher, Hash, Hasher},
    sync::atomic::{
        AtomicUsize,
        Ordering::{Acquire, Relaxed},
    },
    usize,
};

mod leaf;

use leaf::LeafNode;
pub type DefaultHashBuilder = ahash::RandomState;

#[derive(Debug)]
struct Bucket<V> {
    head: Atomic<LeafNode<V>, NullTag, NullTag, 0, 0>,
    collector: Collector,
}

impl<'b, V> Bucket<V>
where
    V: Clone,
{
    #[inline]
    fn find_item<'g>(&self, key: usize) -> Option<Shared<'g, LeafNode<V>, NullTag, NullTag, 0, 0>> {
        unsafe {
            let danger = flize::unprotected();
            let mut checking = self.head.load(Relaxed, danger);

            loop {
                if checking.is_null() {
                    return None;
                }

                let leaf = checking.as_ref_unchecked();
                if leaf.key == key {
                    return Some(checking);
                }

                checking = if leaf.key > key {
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

                if checking.key == key {
                    checking.data.store(data, Relaxed);
                    return false;
                }

                let mut low = above.key > key;

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
                                    println!("we looped back");
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

                    if checking.key == key {
                        checking.data.store(data, Relaxed);
                        return false;
                    }

                    low = checking.key > key;
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

pub struct JackMap<V, S = DefaultHashBuilder> {
    size: AtomicUsize,

    num_buckets: usize,
    buckets: ArrayVec<Bucket<V>, 1024>,

    hasher: S,
}

impl<'a, V> JackMap<V, DefaultHashBuilder>
where
    V: 'a + Clone,
{
    pub fn new(num_buckets: usize) -> Self {
        let mut buckets = ArrayVec::new();

        unsafe {
            for _i in 0..num_buckets {
                buckets.push_unchecked(Bucket::default())
            }
        }

        let hasher = RandomState::new();

        Self {
            size: AtomicUsize::new(0),
            num_buckets,
            buckets,
            hasher,
        }
    }

    #[inline]
    fn determine_bucket(&self, hash: usize) -> usize {
        hash % self.num_buckets
    }

    fn hash_key<K: 'a + Hash>(&self, key: &K) -> usize {
        let mut hashing = self.hasher.build_hasher();
        key.hash(&mut hashing);
        hashing.finish() as usize
    }

    #[inline]
    pub fn insert<K: 'a + Hash>(&self, key: &K, value: V) {
        let key = self.hash_key(key);
        let bucket = self.determine_bucket(key);

        unsafe {
            let res = self.buckets.get_unchecked(bucket).insert_item(key, value);
            if res {
                self.size.fetch_add(1, Relaxed);
            }
        }
    }

    #[inline]
    pub fn insert_hashed(&self, key: usize, value: V) {
        let bucket = self.determine_bucket(key);

        unsafe {
            let res = self.buckets.get_unchecked(bucket).insert_item(key, value);
            if res {
                self.size.fetch_add(1, Relaxed);
            }
        }
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size.load(Relaxed)
    }

    #[inline]
    pub fn get<K: 'a + Hash>(&self, key: &K) -> Option<&V> {
        let key = self.hash_key(key);
        let bucket = self.determine_bucket(key);

        let node = unsafe { self.buckets.get_unchecked(bucket) };

        unsafe {
            match node.try_get(key) {
                Some(res) => res
                    .as_ref_unchecked()
                    .data
                    .load(Relaxed, flize::unprotected())
                    .as_ref(),
                None => None,
            }
        }
    }

    #[inline]
    pub fn get_hashed(&self, key: usize) -> Option<&V> {
        let bucket = self.determine_bucket(key);

        let node = unsafe { self.buckets.get_unchecked(bucket) };

        unsafe {
            match node.try_get(key) {
                Some(res) => res
                    .as_ref_unchecked()
                    .data
                    .load(Relaxed, flize::unprotected())
                    .as_ref(),
                None => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::JackMap;
    use std::{sync::Arc, thread};

    #[test]
    fn threaded_test() {
        let jacktable = Arc::new(JackMap::new(200));
        const INSTERT_COUNT: usize = 2_000_000;

        let table_a = jacktable.clone();
        let a = thread::spawn(move || {
            for i in 0..INSTERT_COUNT {
                let string = format!("Key {}", i);
                table_a.insert(&string, "Thread A");
            }
        });

        let table_b = jacktable.clone();
        let b = thread::spawn(move || {
            for i in (0..INSTERT_COUNT).rev() {
                let string = format!("Key {}", i);
                table_b.insert(&string, "Thread B");
            }
        });

        let table_c = jacktable.clone();
        let c = thread::spawn(move || {
            for i in 0..INSTERT_COUNT {
                let string = format!("Key {}", i);
                table_c.insert(&string, "Thread C");
            }
        });

        let table_d = jacktable.clone();
        let d = thread::spawn(move || {
            for i in (0..INSTERT_COUNT).rev() {
                let string = format!("Key {}", i);
                table_d.insert(&string, "Thread D");
            }
        });

        a.join().unwrap();
        b.join().unwrap();
        c.join().unwrap();
        d.join().unwrap();

        println!("Done!! we have {} items ", jacktable.size());
    }

    #[test]
    fn it_works() {
        let jacktable = JackMap::new(60);
        const INSTERT_COUNT: usize = 800_000;

        for i in 0..INSTERT_COUNT {
            let string = format!("Key {}", i);
            jacktable.insert(&string, i);
        }
        println!("JackTable size is {}", jacktable.size());
        let mut cntr = 0;

        for i in 0..INSTERT_COUNT {
            let string = format!("Key {}", i);

            let found = jacktable.get(&string);

            if found.is_none() {
                cntr += 1;
            }

            //assert_eq!(found.unwrap(), i);
        }

        println!("We lost {} bits of data", cntr);

        println!("{:?}", jacktable.get(&"Key 0"));
        jacktable.insert(&"Key 0", 123456);
        println!("{:?}", jacktable.get(&"Key 0"));
    }
}
