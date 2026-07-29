//! Batched stderr sink for access-log lines, used when stderr is a file.
//!
//! Writing a line per request costs a `write(2)` behind the process-wide
//! stderr lock — and, when several workers share one regular file, behind that
//! file's position lock too. Batching hands the line to a buffer instead, and
//! one writer thread drains whatever accumulated into a single write.
//!
//! Batching emerges from the sink's own latency rather than from a timer: an
//! idle sink wakes on the first line and writes it immediately, while a busy
//! one is still writing when the next lines arrive, so they coalesce. There is
//! nothing to tune.
//!
//! **It is installed only for a regular file**, because that is the only sink
//! where it pays. Measured on this project's benchmark (4 workers, plaintext
//! GET, paired A/B against the line-at-a-time path):
//!
//! | stderr | line-at-a-time | batched |
//! | --- | --- | --- |
//! | regular file | 105k RPS | **225k RPS** |
//! | drained pipe | 233k RPS | 224k RPS |
//! | `/dev/null` | 233k RPS | 222k RPS |
//!
//! A pipe or character device absorbs a small write cheaply, so coordinating a
//! shared buffer across threads costs more than the syscalls it removes. A
//! regular file does not, and there batching removes the entire logging
//! penalty.

use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Once, OnceLock};
use std::time::{Duration, Instant};
use std::{mem, thread};

use parking_lot::{Condvar, Mutex};

/// Initial capacity of each buffer. A batch is never split to fit it: one
/// batch is one `write(2)`, which is what keeps a line whole when several
/// workers append to the same file.
const INITIAL_CAPACITY: usize = 32 * 1024;
/// Backpressure threshold. A sink that stops draining (a stalled disk) must
/// cost bounded memory and must never stall a request; once this much is
/// already queued, further lines are counted and dropped. An idle sink always
/// accepts, however long the line.
const CAPACITY: usize = 1 << 20;

/// Set only once a writer thread exists to drain it, so a queued line always
/// has something that will write it.
static SINK: OnceLock<&'static LogSink> = OnceLock::new();
static START: Once = Once::new();

struct Pending {
    lines: Vec<u8>,
    /// Buffer handed back by the writer, reused for the next batch.
    spare: Vec<u8>,
    /// The writer holds a batch it has not finished writing. A flush that
    /// only looked at `lines` would return while that batch is still in
    /// flight, and the process could exit before it reached the file.
    writing: bool,
}

struct LogSink {
    pending: Mutex<Pending>,
    /// Signalled when the buffer becomes non-empty.
    ready: Condvar,
    /// Signalled when the writer has drained everything it took.
    drained: Condvar,
    dropped: AtomicU64,
}

/// Start the batching writer thread for this process.
///
/// Called from the serve path, which runs in the worker process — never before
/// the supervisor forks, since the child of a fork inherits the buffer but not
/// the thread that drains it. Until this is called (embedded use, tests), lines
/// are written inline. Two servers in one process share the one writer: a
/// second buffer would need a second writer, and a second writer interleaving
/// batches into the same file is exactly what batching exists to avoid.
pub(crate) fn start() {
    if !stderr_is_regular_file() {
        return;
    }
    START.call_once(|| {
        let sink: &'static LogSink = Box::leak(Box::new(LogSink {
            pending: Mutex::new(Pending {
                lines: Vec::with_capacity(INITIAL_CAPACITY),
                spare: Vec::with_capacity(INITIAL_CAPACITY),
                writing: false,
            }),
            ready: Condvar::new(),
            drained: Condvar::new(),
            dropped: AtomicU64::new(0),
        }));
        // Publish the buffer only once its writer exists. A queue nothing
        // drains is the one state `write_line` must never find.
        if thread::Builder::new()
            .name("h2corn-access-log".to_owned())
            .spawn(move || drain_forever(sink))
            .is_ok()
        {
            let _ = SINK.set(sink);
        }
    });
}

/// Queue one formatted line, or write it inline if there is no writer.
pub(crate) fn write_line(line: &[u8]) {
    let Some(sink) = SINK.get() else {
        let _ = io::stderr().write_all(line);
        return;
    };

    let mut pending = sink.pending.lock();
    if !pending.lines.is_empty() && pending.lines.len() + line.len() > CAPACITY {
        drop(pending);
        sink.dropped.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let was_empty = pending.lines.is_empty();
    pending.lines.extend_from_slice(line);
    drop(pending);
    // Only the empty -> non-empty transition can have a parked writer to wake;
    // while it is writing there is no waiter and the notify costs nothing.
    if was_empty {
        sink.ready.notify_one();
    }
}

/// Wait for queued lines to reach stderr, giving up after `timeout`.
///
/// Called on graceful shutdown so a worker's last requests are logged.
pub(crate) fn flush(timeout: Duration) {
    let Some(sink) = SINK.get() else {
        return;
    };
    let deadline = Instant::now() + timeout;
    let mut pending = sink.pending.lock();
    while !pending.lines.is_empty() || pending.writing {
        if sink.drained.wait_until(&mut pending, deadline).timed_out() {
            return;
        }
    }
}

fn stderr_is_regular_file() -> bool {
    #[cfg(unix)]
    {
        use std::os::fd::BorrowedFd;

        use rustix::fs::{FileType, fstat};

        // SAFETY: file descriptor 2 is stderr, open for the whole life of the
        // process, and the borrow does not outlive this call.
        let stderr = unsafe { BorrowedFd::borrow_raw(2) };
        fstat(stderr)
            .is_ok_and(|stat| FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn drain_forever(sink: &'static LogSink) {
    let mut stderr = io::stderr();
    loop {
        // The two buffers swap roles every round: producers fill one while the
        // writer drains the other, so a batch never blocks an append.
        let mut batch = {
            let mut pending = sink.pending.lock();
            while pending.lines.is_empty() {
                sink.ready.wait(&mut pending);
            }
            let spare = mem::take(&mut pending.spare);
            pending.writing = true;
            mem::replace(&mut pending.lines, spare)
        };

        let dropped = sink.dropped.swap(0, Ordering::Relaxed);
        if dropped != 0 {
            let _ = writeln!(stderr, "dropped {dropped} access log lines: sink too slow");
        }
        // One batch, one write. Linux holds the file's lock for the whole of a
        // regular-file write, so a batch appended by one worker cannot have
        // another worker's batch spliced into the middle of a line. Splitting
        // it here — at any fixed size, since lines do not land on that
        // boundary — is what would break that.
        let _ = stderr.write_all(&batch);

        batch.clear();
        let mut pending = sink.pending.lock();
        pending.spare = mem::take(&mut batch);
        pending.writing = false;
        if pending.lines.is_empty() {
            sink.drained.notify_all();
        }
    }
}
