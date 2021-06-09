use flize::{NullTag, Shared};

pub struct Value<V> {
    pub hash: u64,
    pub value: V,
}

impl<V> Value<V> {
    pub fn new_boxed<'a>(hash: u64, value: V) -> Shared<'a, Value<V>, NullTag, NullTag, 0, 0> {
        let me = Self { hash, value };

        unsafe { Shared::from_ptr(Box::into_raw(Box::new(me))) }
    }
}

impl<V> PartialEq for Value<V> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}
