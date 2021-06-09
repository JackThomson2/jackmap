#![feature(stdsimd)]

use ahash::RandomState;
use std::{
    hash::{BuildHasher, Hash, Hasher},
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
    usize,
};

mod bucket;
mod leaf;

use crate::bucket::Bucket;
pub type DefaultHashBuilder = ahash::RandomState;

pub struct JackMap<V, S = DefaultHashBuilder> {
    size: AtomicUsize,

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
        let jacktable = Arc::new(JackMap::new(2_000_000));
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
        let jacktable = JackMap::new(800_000);
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
