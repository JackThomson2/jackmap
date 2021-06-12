use std::arch::x86_64 as x86;
use std::sync::atomic::AtomicU8;

#[repr(align(16))]
pub struct Padded<T>(pub T);

pub unsafe fn find(tail: u8, base: *mut u8) -> Bucket {
    let a = x86::_mm_loadu_si128(base.cast());
    let cmp = x86::_mm_cmpeq_epi8(a, x86::_mm_set1_epi8(tail as i8));

    Bucket(x86::_mm_movemask_epi8(cmp) as u16)
}

pub unsafe fn any_free(base: *mut u8) -> bool {
    let a = x86::_mm_loadu_si128(base.cast());
    let cmp = x86::_mm_cmpeq_epi8(a, x86::_mm_set1_epi8(0));

    (x86::_mm_movemask_epi8(cmp) as u16) != 0
}

#[derive(Clone, Copy)]
pub struct Bucket(pub u16);

impl Bucket {
    #[inline]
    pub fn empty(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub fn remove_top_index(self) -> Self {
        Bucket(self.0 & (self.0 - 1))
    }

    #[inline]
    pub fn try_get_next(self) -> Option<usize> {
        if self.empty() {
            None
        } else {
            Some(self.0.trailing_zeros() as usize)
        }
    }

    #[inline]
    pub fn get_next_index(self) -> u32 {
        self.0.trailing_zeros()
    }
}
