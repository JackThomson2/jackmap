#![feature(atomic_mut_ptr)]
#![feature(core_intrinsics)]

use ahash::RandomState;
use flize::{Atomic, Collector, NullTag, Shared, Shield, ThinShield};
use std::{
    hash::{BuildHasher, Hash, Hasher},
    intrinsics::{likely, unlikely},
    sync::atomic::{
        AtomicU8, AtomicUsize,
        Ordering::{self, Relaxed, SeqCst},
    },
    usize,
};
use value::Value;

mod bucket;
mod value;

use bucket::{any_free, find, Padded, EMPTY};

pub type DefaultHashBuilder = ahash::RandomState;

#[inline]
fn h1(hash: u64) -> u64 {
    hash >> 7
}

#[inline]
fn h2(hash: u64) -> u8 {
    (hash & 0x7F) as u8
}

pub struct JackMap<V, S = DefaultHashBuilder> {
    size: AtomicUsize,
    capacity: usize,
    num_buckets: usize,
    buckets: Vec<Atomic<Value<V>, NullTag, NullTag, 0, 0>>,

    hasher: S,
    collector: Collector,
    lut: Padded<Vec<AtomicU8>>,
}

impl<'a, V> JackMap<V, DefaultHashBuilder>
where
    V: 'a + Clone,
{
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two();
        let buckets = Atomic::null_vec(capacity);
        let hasher = RandomState::new();
        let num_buckets = capacity / 16;
        let lut = Padded((0..capacity).map(|_x| AtomicU8::new(EMPTY)).collect());

        Self {
            size: AtomicUsize::new(0),
            capacity,
            num_buckets,
            buckets,
            hasher,
            collector: Collector::new(),
            lut,
        }
    }

    #[inline]
    unsafe fn load_bucket_ptr(&self, idx: usize) -> *mut u8 {
        debug_assert!(idx + 15 < self.capacity);

        self.lut.0.get_unchecked(idx).load(Ordering::SeqCst);
        self.lut.0.get_unchecked(idx).as_mut_ptr()
    }

    #[inline]
    fn determine_bucket(&self, hash: usize) -> usize {
        h1(hash as u64) as usize % self.num_buckets
    }

    #[inline]
    pub fn hash_key<K: 'a + Hash>(&self, key: &K) -> usize {
        let mut hashing = self.hasher.build_hasher();
        key.hash(&mut hashing);
        hashing.finish() as usize
    }

    #[inline]
    pub fn insert<K: 'a + Hash>(&self, key: &K, value: V) {
        let key = self.hash_key(key);
        let shield = self.collector.thin_shield();

        self.insert_hashed(key, value, &shield);
    }

    #[inline]
    pub fn insert_hashed<'b, S: Shield<'b>>(&self, key: usize, value: V, shield: &'b S) {
        let bucket_idx = self.determine_bucket(key);

        let start = bucket_idx * 16;
        let mut idx = start;
        let h2 = h2(key as u64);

        let data = value::Value::new_boxed(key as u64, value);

        loop {
            let bucket = unsafe { self.buckets.get_unchecked(idx) };
            let loaded = bucket.load(SeqCst, shield);

            if !loaded.is_null() {
                let found = unsafe { loaded.as_ref_unchecked() };
                if unlikely(found.hash == key as u64) {
                    match bucket.compare_exchange_weak(loaded, data, SeqCst, Relaxed, shield) {
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

            match bucket.compare_exchange_weak(Shared::null(), data, SeqCst, Relaxed, shield) {
                Ok(_res) => {
                    unsafe { self.lut.0.get_unchecked(idx).store(h2, Ordering::SeqCst) }
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
    pub fn get_shield(&self) -> ThinShield {
        self.collector.thin_shield()
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size.load(Relaxed)
    }

    #[inline]
    pub fn remove<K: 'a + Hash, S: Shield<'a>>(&self, key: &K, shield: &'a S) -> Option<&'a V> {
        let key = self.hash_key(key);
        self.remove_hashed(key as u64, shield)
    }

    #[inline]
    pub fn remove_hashed<S: Shield<'a>>(&self, key: u64, shield: &'a S) -> Option<&'a V> {
        let mut bucket = self.determine_bucket(key as usize);
        let h2 = h2(key);

        'outer: loop {
            let start = bucket * 16;
            debug_assert!(start + 15 < self.capacity);

            let searched_bucket = unsafe { self.load_bucket_ptr(bucket * 16) };
            let mut search_res = unsafe { find(h2, searched_bucket) };

            while let Some(idx) = search_res.try_get_next() {
                let search_pos = start + idx as usize;

                let bucket = unsafe { self.buckets.get_unchecked(search_pos) };
                let loaded = bucket.load(SeqCst, shield);

                if unlikely(loaded.is_null()) {
                    return None;
                }

                let data = unsafe { loaded.as_ref_unchecked() };

                if likely(data.hash == key) {
                    match bucket.compare_exchange_weak(
                        loaded,
                        Shared::null(),
                        SeqCst,
                        Relaxed,
                        shield,
                    ) {
                        Ok(res) => {
                            unsafe { self.lut.0.get_unchecked(idx).store(EMPTY, SeqCst) };
                            self.size.fetch_sub(1, Relaxed);
                            return Some(unsafe { &res.as_ref_unchecked().value });
                        }
                        Err(_err) => {
                            continue 'outer;
                        }
                    }
                }

                search_res = search_res.remove_top_index()
            }

            if unsafe { any_free(searched_bucket) } {
                return None;
            }

            bucket += 1;
            bucket %= self.num_buckets;
        }
    }

    #[inline]
    pub fn get<K: 'a + Hash, S: Shield<'a>>(&self, key: &K, shield: &'a S) -> Option<&'a V> {
        let key = self.hash_key(key);
        self.get_hashed(key as u64, shield)
    }

    #[inline]
    pub fn get_hashed<S: Shield<'a>>(&self, key: u64, shield: &'a S) -> Option<&'a V> {
        let mut bucket = self.determine_bucket(key as usize);
        let h2 = h2(key);

        loop {
            let start = bucket * 16;
            debug_assert!(start + 15 < self.capacity);

            let searched_bucket = unsafe { self.load_bucket_ptr(bucket * 16) };
            let mut search_res = unsafe { find(h2, searched_bucket) };

            while let Some(idx) = search_res.try_get_next() {
                let search_pos = start + idx as usize;

                let bucket = unsafe { self.buckets.get_unchecked(search_pos) };
                let loaded = bucket.load(SeqCst, shield);

                if unlikely(loaded.is_null()) {
                    return None;
                }

                let data = unsafe { loaded.as_ref_unchecked() };

                if likely(data.hash == key) {
                    return Some(&data.value);
                }

                search_res = search_res.remove_top_index()
            }

            if unsafe { any_free(searched_bucket) } {
                return None;
            }

            bucket += 1;
            bucket %= self.num_buckets;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::JackMap;
    use std::{sync::Arc, thread, time::Instant};

    #[test]
    fn threaded_test() {
        let jacktable = Arc::new(JackMap::new(20_000_000));
        const INSTERT_COUNT: usize = 25165824 / 4;

        let start = Instant::now();
        let table_a = jacktable.clone();
        let a = thread::spawn(move || {
            for i in 0..INSTERT_COUNT {
                table_a.insert(&i, "Thread A");
            }
        });

        let table_b = jacktable.clone();
        let b = thread::spawn(move || {
            for i in (0..INSTERT_COUNT).rev() {
                table_b.insert(&i, "Thread B");
            }
        });

        let table_c = jacktable.clone();
        let c = thread::spawn(move || {
            for i in 0..INSTERT_COUNT {
                table_c.insert(&i, "Thread C");
            }
        });

        let table_d = jacktable.clone();
        let d = thread::spawn(move || {
            for i in (0..INSTERT_COUNT).rev() {
                table_d.insert(&i, "Thread D");
            }
        });

        a.join().unwrap();
        b.join().unwrap();
        c.join().unwrap();
        d.join().unwrap();

        println!("Inserting took {:#?}", start.elapsed());

        let shield = jacktable.get_shield();
        for i in (0..INSTERT_COUNT).rev() {
            let found = jacktable.get(&i, &shield);

            if found.is_none() {
                println!("We lost {}", i);
            }
        }

        println!("Done!! we have {} items ", jacktable.size());
    }

    #[test]
    fn threaded_read() {
        let jacktable = Arc::new(JackMap::new(25165824 * 4));
        const INSTERT_COUNT: usize = 25165824 / 2;

        for i in 0..INSTERT_COUNT {
            jacktable.insert(&i, "Thread A");
        }

        let start = Instant::now();

        let table_a = jacktable.clone();
        let a = thread::spawn(move || {
            let sheild = table_a.get_shield();
            for i in 0..INSTERT_COUNT {
                table_a.get(&i, &sheild);
            }
        });

        let table_b = jacktable.clone();
        let b = thread::spawn(move || {
            let sheild = table_b.get_shield();
            for i in 0..INSTERT_COUNT {
                table_b.get(&i, &sheild);
            }
        });

        a.join().unwrap();
        b.join().unwrap();

        println!("Reading took {:#?}", start.elapsed());

        println!("Done!! we have {} items ", jacktable.size());
    }

    #[test]
    fn it_works() {
        let jacktable = JackMap::new(10_000_000);
        const INSTERT_COUNT: usize = 1_000_000;

        for i in 0..INSTERT_COUNT {
            let string = format!("Key {}", i);
            jacktable.insert(&string, i);
        }
        println!("JackTable size is {}", jacktable.size());
        let mut cntr = 0;

        let shield = jacktable.get_shield();
        for i in 0..INSTERT_COUNT {
            let string = format!("Key {}", i);

            let found = jacktable.get(&string, &shield);

            if found.is_none() {
                cntr += 1;
            }

            //assert_eq!(found.unwrap(), i);
        }
        println!("We lost {} bits of data", cntr);

        println!("{:?}", jacktable.get(&"Key 0", &shield));
        jacktable.insert(&"Key 0", 123456);
        println!("{:?}", jacktable.get(&"Key 0", &shield));
    }

    #[test]
    fn single_add() {
        let jacktable = JackMap::new(10_000);

        for i in 0..500 {
            let start = Instant::now();
            jacktable.insert(&i, 500);

            let end = start.elapsed();
            println!("Inserting took {:#?}", end);
        }

        for i in 0..500 {
            let start = Instant::now();
            let shield = jacktable.get_shield();
            if jacktable.get(&i, &shield).is_none() {
                println!("Error with {}", i)
            }

            let end = start.elapsed();
            println!("Reading took {:#?}ns", end.as_nanos());
        }

        println!("We have {} items in ", jacktable.size());
    }
}
