use rand::RngCore;
use std::cell::UnsafeCell;
use std::hint::{likely, unlikely};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const RAND_BUFFER_SIZE: usize = 8 * 1024; // 8 KiB

struct RandBuffer {
    buffer: [u8; RAND_BUFFER_SIZE],
    pos: usize,
}

impl RandBuffer {
    const fn new() -> Self {
        Self {
            buffer: [0; RAND_BUFFER_SIZE],
            pos: RAND_BUFFER_SIZE,
        }
    }

    fn next_u16(&mut self) -> u16 {
        if unlikely(self.pos + 2 > RAND_BUFFER_SIZE) {
            rand::rng().fill_bytes(&mut self.buffer);
            self.pos = 0;
        }
        let value = u16::from_be_bytes([self.buffer[self.pos], self.buffer[self.pos + 1]]);
        self.pos += 2;
        value
    }
}

thread_local! {
    static RAND_BUFFER: UnsafeCell<RandBuffer> = const { UnsafeCell::new(RandBuffer::new()) };
}

fn next_rand_u16() -> u16 {
    // Safety: `RAND_BUFFER` is thread-local, so this `UnsafeCell` is only accessed from the
    // current thread, and we don't leak references outside this closure.
    RAND_BUFFER.with(|cell| unsafe { (*cell.get()).next_u16() })
}

static LAST_ZID: AtomicU64 = AtomicU64::new(0);

fn time() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn make_zid(time: u64, sequence: u16) -> u64 {
    (time << 16) | (sequence as u64)
}

fn random_start_seq(max_start: u16) -> u16 {
    match max_start {
        0 => 0,
        u16::MAX => next_rand_u16(),
        _ => {
            let range = u32::from(max_start) + 1;
            (u32::from(next_rand_u16()) % range) as u16
        }
    }
}

pub(crate) fn reserve_sequences(additional: u16) -> (u64, u16) {
    let now = time();
    let max_start = u16::MAX - additional;
    loop {
        let last = LAST_ZID.load(Ordering::Relaxed);
        let last_time = last >> 16;
        let last_seq = last as u16;

        let mut zid_time = last_time.max(now);

        let start_seq = if zid_time != last_time {
            random_start_seq(max_start)
        } else {
            match last_seq.checked_add(1) {
                Some(next_seq) if next_seq <= max_start => next_seq,
                _ => {
                    zid_time = zid_time.saturating_add(1);
                    random_start_seq(max_start)
                }
            }
        };

        let end_seq = (u32::from(start_seq) + u32::from(additional)) as u16;
        let next_last = make_zid(zid_time, end_seq);

        if likely(
            LAST_ZID
                .compare_exchange_weak(last, next_last, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok(),
        ) {
            return (zid_time, start_seq);
        }
        std::hint::spin_loop();
    }
}

pub(crate) fn zid() -> u64 {
    let (time, seq) = reserve_sequences(0);
    make_zid(time, seq)
}

pub(crate) fn zid_from_time_and_sequence(time: u64, sequence: u16) -> u64 {
    make_zid(time, sequence)
}

pub(crate) fn parse_zid_timestamp(zid: u64) -> u64 {
    zid >> 16
}
