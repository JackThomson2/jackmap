use core::arch::x86_64::_mm256_cmpeq_epi64;
use core::arch::x86_64::_mm256_movemask_epi8;
use core::arch::x86_64::_mm256_set1_epi64x;
use core::arch::x86_64::_mm_tzcnt_32;
use std::arch::x86_64::_mm256_set_epi64x;

#[inline]
pub fn find_index(f: &[usize], key: usize) -> Option<usize> {
    return f.iter().position(|&item| item == key);

    let mut index = 0;
    unsafe {
        let finder = _mm256_set1_epi64x(key as i64);

        while index + 4 < f.len() {
            let loading = _mm256_set_epi64x(
                f[index + 3] as i64,
                f[index + 2] as i64,
                f[index + 1] as i64,
                f[index] as i64,
            );

            let cmp = _mm256_cmpeq_epi64(loading, finder);
            let mask = _mm256_movemask_epi8(cmp);

            let masked = _mm_tzcnt_32(mask as u32);

            if masked < 32 {
                return Some(index + (masked / 8) as usize);
            }

            index += 4;
        }
    }

    // Fall back to linear for the rest we can't spread
    for i in index..f.len() {
        if f[i] == key {
            return Some(i);
        }
    }

    None
}

#[cfg(test)]
mod simdtests {
    use crate::x86::find_index;

    #[test]
    fn array_lookup() {
        let array = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

        unsafe {
            assert_eq!(find_index(&array, 0), Some(0));
            assert_eq!(find_index(&array, 1), Some(1));

            assert_eq!(find_index(&array, 2), Some(2));
            assert_eq!(find_index(&array, 3), Some(3));

            assert_eq!(find_index(&array, 4), Some(4));
            assert_eq!(find_index(&array, 5), Some(5));

            assert_eq!(find_index(&array, 6), Some(6));
            assert_eq!(find_index(&array, 7), Some(7));

            assert_eq!(find_index(&array, 8), Some(8));
            assert_eq!(find_index(&array, 9), Some(9));

            assert_eq!(find_index(&array, 10), Some(10));
            assert_eq!(find_index(&array, 11), Some(11));

            assert_eq!(find_index(&array, 12), Some(12));
            assert_eq!(find_index(&array, 13), None);
        }
    }
}
