#![feature(stdsimd)]

use ahash::RandomState;
use std::{
    hash::{BuildHasher, Hash, Hasher},
    usize, vec,
};

mod x86;

use x86::find_index;
pub type DefaultHashBuilder = ahash::RandomState;

#[derive(Debug)]
struct Bucket<V> {
    keys: Vec<usize>,
    values: Vec<V>,
}

impl<V> Bucket<V> {
    #[inline]
    pub fn insert_item(&mut self, key: usize, value: V) {
        unsafe {
            if let Some(found) = find_index(&self.keys, key) {
                *self.values.get_unchecked_mut(found) = value;
                return;
            }
        }

        self.keys.push(key);
        self.values.push(value);
    }

    #[inline]
    pub fn try_get(&self, key: usize) -> Option<&V> {
        unsafe {
            match find_index(&self.keys, key) {
                Some(index) => Some(self.values.get_unchecked(index)),
                None => None,
            }
        }
    }
}

impl<V> Default for Bucket<V> {
    fn default() -> Self {
        Self {
            keys: vec![],
            values: vec![],
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
    V: 'a,
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
    pub fn get<K: 'a + Hash>(&self, key: &K) -> Option<&V> {
        let key = self.hash_key(key);
        let bucket = self.determine_bucket(key);

        unsafe { self.buckets.get_unchecked(bucket).try_get(key) }
    }
}

#[cfg(test)]
mod tests {
    use crate::JackMap;

    #[test]
    fn it_works() {
        let mut jacktable = JackMap::new(10);
        const INSTERT_COUNT: usize = 10_000;

        for i in 0..INSTERT_COUNT {
            let string = format!("Key {}", i);

            jacktable.insert(&string, i);
        }

        jacktable.insert(&"Test", 5);

        let found = jacktable.get(&"Test");
        println!("We got {:?} ", found);

        let found = jacktable.get(&"Testing");
        println!("We got {:?} ", found);

        jacktable.insert(&"Test", 100);
        let found = jacktable.get(&"Test");
        println!("We got {:?} ", found);

        println!("JackTable size is {}", jacktable.size());

        for i in 0..INSTERT_COUNT {
            let string = format!("Key {}", i);

            let found = jacktable.get(&string).unwrap();

            assert_eq!(*found, i);
        }
    }
}
