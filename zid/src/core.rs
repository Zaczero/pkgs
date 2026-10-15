use std::cell::UnsafeCell;
use std::hint;
use std::hint::{likely, unlikely};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::RngCore as _;

use crate::constants::MAX_TIMESTAMP;
use crate::errors::ZidError;

const RAND_BUFFER_SIZE: usize = 8 * 1024; // 8 KiB

static GENERATOR: Generator = Generator::new();

struct Generator {
    last: AtomicU64,
}

thread_local! {
    static RAND_BUFFER: UnsafeCell<RandBuffer> = const { UnsafeCell::new(RandBuffer::new()) };
}

struct RandBuffer {
    buffer: [[u8; 2]; RAND_BUFFER_SIZE / 2],
    pos: usize,
}

impl RandBuffer {
    const fn new() -> Self {
        Self {
            buffer: [[0; 2]; RAND_BUFFER_SIZE / 2],
            pos: RAND_BUFFER_SIZE / 2,
        }
    }

    fn next_u16(&mut self) -> u16 {
        if unlikely(self.pos == self.buffer.len()) {
            rand::rng().fill_bytes(self.buffer.as_flattened_mut());
            self.pos = 0;
        }
        let bytes = self
            .buffer
            .get(self.pos)
            .expect("refill leaves a random word available");
        let value = u16::from_be_bytes(*bytes);
        self.pos = self.pos.wrapping_add(1);
        value
    }
}

fn next_rand_u16() -> u16 {
    // Safety: `RAND_BUFFER` is thread-local, so this `UnsafeCell` is only accessed
    // from the current thread. No reference escapes and the native RNG path
    // invokes no callbacks that could reenter this closure.
    RAND_BUFFER.with(|cell| unsafe { (*cell.get()).next_u16() })
}

fn time() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

const fn make_zid(time: u64, sequence: u16) -> u64 {
    (time << 16) | (sequence as u64)
}

fn random_start_seq(max_start: u16, draw: &mut impl FnMut() -> u16) -> u16 {
    match max_start {
        0 => 0,
        u16::MAX => draw(),
        _ => {
            // Unbiased bounded draw (Lemire's multiply-shift): a plain
            // `% range` would skew low sequences whenever the range does
            // not divide 2^16 — the CSPRNG-backed sequence privacy should
            // not carry modulo bias. The low-sliver rejection re-draws with
            // probability < range / 2^16.
            let range = NonZeroU32::new(u32::from(max_start).wrapping_add(1))
                .expect("u16 successor fits a nonzero u32");
            let threshold = 0x1_0000 % range;
            loop {
                let product = u32::from(draw()).wrapping_mul(range.get());
                if (product & 0xFFFF) >= threshold {
                    break (product >> 16) as u16;
                }
            }
        },
    }
}

impl Generator {
    const fn new() -> Self {
        Self {
            // All valid ZIDs fit in a signed integer, leaving this sentinel free.
            last: AtomicU64::new(u64::MAX),
        }
    }

    fn reserve(
        &self,
        now: u64,
        additional: u16,
        mut draw: impl FnMut() -> u16,
    ) -> Result<(u64, u16), ZidError> {
        if now > MAX_TIMESTAMP {
            return Err(ZidError::TimestampExhausted);
        }
        let max_start = u16::MAX.wrapping_sub(additional);
        loop {
            let last = self.last.load(Ordering::Relaxed);
            let last_time = last >> 16;
            let last_seq = last as u16;
            let mut zid_time = now;

            let start_seq = if last != u64::MAX && now <= last_time {
                zid_time = last_time;
                match last_seq.checked_add(1) {
                    Some(next_seq) if next_seq <= max_start => next_seq,
                    _ => {
                        if zid_time == MAX_TIMESTAMP {
                            return Err(ZidError::TimestampExhausted);
                        }
                        zid_time = zid_time.wrapping_add(1);
                        random_start_seq(max_start, &mut draw)
                    },
                }
            } else {
                random_start_seq(max_start, &mut draw)
            };

            let end_seq = start_seq.wrapping_add(additional);
            let next_last = make_zid(zid_time, end_seq);

            if likely(
                self.last
                    .compare_exchange_weak(last, next_last, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok(),
            ) {
                return Ok((zid_time, start_seq));
            }
            hint::spin_loop();
        }
    }
}

pub(crate) fn reserve_sequences(additional: u16) -> Result<(u64, u16), ZidError> {
    GENERATOR.reserve(time(), additional, next_rand_u16)
}

pub(crate) fn zid() -> Result<u64, ZidError> {
    let (time, seq) = reserve_sequences(0)?;
    Ok(make_zid(time, seq))
}

pub(crate) const fn zid_from_time_and_sequence(time: u64, sequence: u16) -> u64 {
    make_zid(time, sequence)
}

pub(crate) const fn parse_zid_timestamp(zid: u64) -> u64 {
    zid >> 16
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Barrier;
    use std::sync::atomic::Ordering;
    use std::thread;

    use super::{Generator, random_start_seq};
    use crate::errors::ZidError;

    #[test]
    fn clock_and_sequence_transitions() {
        let generator = Generator::new();
        assert_eq!(
            generator.reserve(0, 0, || 0xFFFE).expect("epoch"),
            (0, 0xFFFE)
        );
        assert_eq!(
            generator.reserve(0, 0, || 17).expect("same time"),
            (0, 0xFFFF)
        );
        assert_eq!(generator.reserve(0, 0, || 23).expect("rollover"), (1, 23));
        assert_eq!(generator.reserve(0, 1, || 31).expect("rollback"), (1, 24));
        assert_eq!(generator.reserve(9, 0, || 37).expect("new time"), (9, 37));
    }

    #[test]
    fn batches_reserve_whole_intervals() {
        let generator = Generator::new();
        assert_eq!(
            generator.reserve(7, 0xFFFF, || 999).expect("full batch"),
            (7, 0)
        );
        assert_eq!(
            generator.reserve(7, 0xFFFE, || 0).expect("batch rollover"),
            (8, 0)
        );
        assert_eq!(
            generator.reserve(7, 0, || 999).expect("last slot"),
            (8, 0xFFFF)
        );
        assert_eq!(
            generator.reserve(7, 1, || 0xFFFF).expect("bounded start"),
            (9, 0xFFFE)
        );
    }

    #[test]
    fn horizon_rejects_before_shifting_and_preserves_state() {
        let generator = Generator::new();
        assert!(matches!(
            generator.reserve(1 << 47, 0, || 0),
            Err(ZidError::TimestampExhausted)
        ));
        assert_eq!(
            generator
                .reserve((1 << 47) - 1, 0xFFFE, || 0)
                .expect("final millisecond"),
            ((1 << 47) - 1, 0)
        );
        assert!(matches!(
            generator.reserve(0, 1, || 0),
            Err(ZidError::TimestampExhausted)
        ));
        assert_eq!(
            generator.reserve(0, 0, || 0).expect("remaining slot"),
            ((1 << 47) - 1, 0xFFFF)
        );
        assert_eq!(generator.last.load(Ordering::Relaxed), i64::MAX as u64);
        assert!(matches!(
            generator.reserve(0, 0, || 0),
            Err(ZidError::TimestampExhausted)
        ));
    }

    #[test]
    fn bounded_random_starts_reject_the_biased_sliver() {
        let mut words = [0, 0xFFFF].into_iter();
        assert_eq!(
            random_start_seq(0xFFFE, &mut || words.next().expect("two draws")),
            0xFFFE
        );
        assert!(words.next().is_none());
        // Exhausting the 16-bit input domain gives equal accepted counts per
        // output, including non-power-of-two ranges.
        for max_start in [1_u16, 2, 0x8000, 0xFFFE] {
            let range = u32::from(max_start) + 1;
            let threshold = 0x0001_0000 % range;
            let mut counts = vec![0_u32; range as usize];
            for word in 0..=u16::MAX {
                let product = u32::from(word) * range;
                if product & 0xFFFF >= threshold {
                    let result = random_start_seq(max_start, &mut || word);
                    *counts.get_mut(usize::from(result)).expect("bounded") += 1;
                }
            }
            assert!(counts.iter().all(|count| *count == 0x0001_0000 / range));
        }
    }

    #[test]
    fn overlapping_reservations_are_disjoint_at_rollover() {
        let generator = Generator::new();
        let barrier = Barrier::new(8);
        let reservations = thread::scope(|scope| {
            let handles: Vec<_> = std::iter::repeat_with(|| {
                let generator = &generator;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    std::iter::repeat_with(|| {
                        generator.reserve(11, 1023, || 1).expect("reservation")
                    })
                    .take(32)
                    .collect::<Vec<_>>()
                })
            })
            .take(8)
            .collect();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().expect("worker"))
                .collect::<Vec<_>>()
        });
        let mut ids = BTreeSet::new();
        for (time, sequence) in reservations {
            for offset in 0..1024 {
                assert!(ids.insert((time << 16) | u64::from(sequence + offset)));
            }
        }
        assert_eq!(ids.len(), 0x0004_0000);
        assert_eq!(ids.first(), Some(&(11 << 16)));
        assert_eq!(ids.last(), Some(&((15 << 16) - 1)));
    }
}
