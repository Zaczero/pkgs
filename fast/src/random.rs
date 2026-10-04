//! Seeded xoshiro256++ for run ordering and simulations.
//!
//! The generator is local so a `--seed` reproduces the same run order across
//! releases; a dependency's stream or sampling algorithm can change underneath it.

use std::io;

#[expect(
    missing_copy_implementations,
    reason = "A copied generator would silently repeat the original's stream"
)]
pub struct Random {
    state: [u64; 4],
}

impl Random {
    /// Expands the seed with SplitMix64, the seeding procedure recommended for xoshiro.
    pub fn new(seed: u64) -> Self {
        let mut seed = seed;
        let mut next = || {
            seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        Self {
            state: [next(), next(), next(), next()],
        }
    }

    /// Reads a seed from the kernel's random source.
    pub fn entropy() -> io::Result<u64> {
        let mut bytes = [0_u8; 8];
        let mut filled = 0;
        while let Some(rest) = bytes.get_mut(filled..).filter(|rest| !rest.is_empty()) {
            // SAFETY: getrandom writes at most rest.len() bytes into the live buffer.
            let count = unsafe { libc::getrandom(rest.as_mut_ptr().cast(), rest.len(), 0) };
            if count < 0 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            } else {
                filled = filled.wrapping_add(count.unsigned_abs());
            }
        }
        Ok(u64::from_ne_bytes(bytes))
    }

    pub const fn next_u64(&mut self) -> u64 {
        let [s0, s1, s2, s3] = &mut self.state;
        let result = s0.wrapping_add(*s3).rotate_left(23).wrapping_add(*s0);
        let shifted = *s1 << 17;
        *s2 ^= *s0;
        *s3 ^= *s1;
        *s1 ^= *s2;
        *s0 ^= *s3;
        *s2 ^= shifted;
        *s3 = s3.rotate_left(45);
        result
    }

    /// Uniform in `0..bound` by Lemire's multiply-and-reject method, without modulo bias.
    pub fn below(&mut self, bound: usize) -> usize {
        assert!(bound > 0, "empty range");
        let bound = bound as u64;
        let mut product = u128::from(self.next_u64()).wrapping_mul(u128::from(bound));
        if (product as u64) < bound {
            let threshold = bound
                .wrapping_neg()
                .checked_rem(bound)
                .expect("asserted nonzero bound");
            while (product as u64) < threshold {
                product = u128::from(self.next_u64()).wrapping_mul(u128::from(bound));
            }
        }
        (product >> 64) as usize
    }

    /// Uniform in `[0, 1)` with 53 random bits.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (f64::EPSILON / 2.0)
    }

    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for index in (1..items.len()).rev() {
            items.swap(index, self.below(index.wrapping_add(1)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Random;

    #[test]
    fn seeded_stream_matches_the_reference_algorithm() {
        // Computed independently from SplitMix64 seeding and the xoshiro256++ reference.
        let mut random = Random::new(0x00C0_FFEE);
        let values: Vec<_> = std::iter::repeat_with(|| random.next_u64())
            .take(3)
            .collect();
        assert_eq!(values, [
            0x8C76_15E9_AF6B_4AE5,
            0xD175_FD6E_7F59_7969,
            0xAC82_3E0A_E898_E8EC
        ]);
    }

    #[test]
    fn shuffle_permutes_and_draws_stay_in_range() {
        let mut random = Random::new(7);
        let mut items: Vec<_> = (0..100).collect();
        random.shuffle(&mut items);
        assert_ne!(items, (0..100).collect::<Vec<_>>());
        items.sort_unstable();
        assert_eq!(items, (0..100).collect::<Vec<_>>());
        assert!((0..10_000).all(|_| random.below(3) < 3));
        assert!((0..10_000).all(|_| (0.0..1.0).contains(&random.unit())));
    }
}
