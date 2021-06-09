#![feature(stdsimd)]

use ahash::RandomState;
use flize::{Atomic, Collector, NullTag, Shared};
use std::{
    hash::{BuildHasher, Hash, Hasher},
    sync::atomic::{
        AtomicUsize,
        Ordering::{Relaxed, SeqCst},
    },
    usize,
};
use value::Value;

mod bucket;
mod leaf;
mod value;

use crate::bucket::Bucket;
pub type DefaultHashBuilder = ahash::RandomState;

#[inline]
fn h1(hash: u64) -> u64 {
    hash >> 7
}

#[inline]
fn h2(hash: u64) -> u64 {
    hash & 0x7F
}

pub struct JackMap<V, S = DefaultHashBuilder> {
    size: AtomicUsize,
    capacity: usize,
    num_buckets: usize,
    buckets: Vec<Atomic<Value<V>, NullTag, NullTag, 0, 0>>,

    hasher: S,
    collector: Collector,
}

impl<'a, V> JackMap<V, DefaultHashBuilder>
where
    V: 'a + Clone,
{
    pub fn new(capacity: usize) -> Self {
        let buckets = Atomic::null_vec(capacity);
        let hasher = RandomState::new();
        let num_buckets = (capacity / 16) + 1;

        Self {
            size: AtomicUsize::new(0),
            capacity,
            num_buckets,
            buckets,
            hasher,
            collector: Collector::new(),
        }
    }

    #[inline]
    fn determine_bucket(&self, hash: usize) -> usize {
        h1(hash as u64) as usize % self.num_buckets
    }

    fn hash_key<K: 'a + Hash>(&self, key: &K) -> usize {
        let mut hashing = self.hasher.build_hasher();
        key.hash(&mut hashing);
        hashing.finish() as usize
    }

    #[inline]
    pub fn insert<K: 'a + Hash>(&self, key: &K, value: V) {
        let key = self.hash_key(key);
        self.insert_hashed(key, value);
    }

    #[inline]
    pub fn insert_hashed(&self, key: usize, value: V) {
        let bucket = self.determine_bucket(key);

        let start = bucket * 16;
        let mut idx = start;

        let data = value::Value::new_boxed(key as u64, value);
        let shield = self.collector.thin_shield();

        loop {
            let bucket = unsafe { self.buckets.get_unchecked(idx) };
            let loaded = bucket.load(SeqCst, &shield);

            if !loaded.is_null() {
                let found = unsafe { loaded.as_ref_unchecked() };
                if found.hash == key as u64 {
                    match bucket.compare_exchange_weak(loaded, data, SeqCst, Relaxed, &shield) {
                        Ok(_) => return,
                        Err(_) => {
                            idx = start;
                            continue;
                        }
                    }
                }

                idx += 1;
                idx %= self.capacity;
                continue;
            }

            match bucket.compare_exchange_weak(Shared::null(), data, SeqCst, Relaxed, &shield) {
                Ok(_res) => {
                    self.size.fetch_add(1, Relaxed);
                    return;
                }
                Err(_) => {
                    idx = start;
                    continue;
                }
            }
        }
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size.load(Relaxed)
    }

    #[inline]
    pub fn get<K: 'a + Hash>(&self, key: &K) -> Option<V> {
        let key = self.hash_key(key);
        self.get_hashed(key as u64)
    }

    #[inline]
    pub fn get_hashed(&self, key: u64) -> Option<V> {
        let bucket = self.determine_bucket(key as usize);
        let shield = self.collector.thin_shield();

        let mut idx = bucket * 16;

        loop {
            let bucket = unsafe { self.buckets.get_unchecked(idx) };
            let loaded = bucket.load(SeqCst, &shield);

            if loaded.is_null() {
                return None;
            }

            let data = unsafe { loaded.as_ref_unchecked() };

            if data.hash == key {
                return Some(data.value.clone());
            }

            idx += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::JackMap;
    use std::{sync::Arc, thread};

    #[test]
    fn threaded_test() {
        let jacktable = Arc::new(JackMap::new(10_000_000));
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
        let jacktable = JackMap::new(900_000);
        const INSTERT_COUNT: usize = 800;

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
