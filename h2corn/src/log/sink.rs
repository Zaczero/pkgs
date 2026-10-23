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

#[cfg(all(test, unix))]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::os::fd::AsRawFd as _;
    use std::os::unix::net::UnixStream;

    use super::{CAPACITY, LogSink, fd_is_regular_file};
    use crate::config::LogFormat;

    #[rstest::rstest]
    #[case::text(LogFormat::Text, "dropped 3 access log lines: sink too slow")]
    #[case::json(
        LogFormat::Json,
        "{\"level\":\"error\",\"event\":\"failed\",\"pid\":PID,\"subject\":\"access log sink\",\"detail\":\"sink too slow\",\"dropped_lines\":3}"
    )]
    fn saturated_sink_reports_drops_directly_before_draining(
        #[case] format: LogFormat,
        #[case] notice: &str,
    ) {
        let sink = LogSink::new();
        let mut output = tempfile::NamedTempFile::new().unwrap();
        assert!(fd_is_regular_file(output.as_raw_fd()));
        let oversized = format!(
            "{{\"event\":\"request\",\"target\":\"{}\"}}\n",
            "x".repeat(CAPACITY)
        );
        sink.enqueue(format, oversized.as_bytes());
        for _ in 0..3 {
            sink.enqueue(format, b"{\"event\":\"request\",\"target\":\"/dropped\"}\n");
        }

        sink.drain_once(&mut output);
        sink.enqueue(format, b"{\"event\":\"request\",\"target\":\"/after\"}\n");
        sink.enqueue(format, b"{\"event\":\"request\",\"target\":\"/batch\"}\n");
        sink.drain_once(&mut output);

        let actual = std::fs::read_to_string(output.path()).unwrap();
        let mut lines = actual.lines();
        let notice = notice.replace("PID", &std::process::id().to_string());
        assert_eq!(lines.next(), Some(notice.as_str()));
        assert_eq!(lines.next(), Some(oversized.trim_end()));
        assert_eq!(
            lines.next(),
            Some("{\"event\":\"request\",\"target\":\"/after\"}")
        );
        assert_eq!(
            lines.next(),
            Some("{\"event\":\"request\",\"target\":\"/batch\"}")
        );
        assert_eq!(lines.next(), None);
    }

    #[test]
    fn saturated_sink_keeps_each_servers_notice_format() {
        let sink = LogSink::new();
        sink.enqueue(LogFormat::Text, &vec![b'x'; CAPACITY]);
        sink.enqueue(LogFormat::Text, b"dropped text\n");
        sink.enqueue(LogFormat::Json, b"{\"event\":\"dropped\"}\n");
        sink.enqueue(LogFormat::Json, b"{\"event\":\"dropped\"}\n");

        let mut output = Vec::new();
        sink.drain_once(&mut output);
        let output = String::from_utf8(output).unwrap();
        let mut lines = output.lines();
        assert_eq!(
            lines.next(),
            Some("dropped 1 access log lines: sink too slow")
        );
        let json = lines.next().unwrap();
        assert!(json.starts_with("{\"level\":\"error\",\"event\":\"failed\","));
        assert!(json.contains("\"dropped_lines\":2"));
    }

    #[test]
    fn regular_file_is_classified_without_taking_ownership() {
        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(fd_is_regular_file(file.as_raw_fd()));
        file.as_file().metadata().unwrap();
    }

    #[test]
    fn socket_and_invalid_descriptors_are_not_regular_files() {
        let (mut left, mut right) = UnixStream::pair().unwrap();
        assert!(!fd_is_regular_file(left.as_raw_fd()));
        assert!(!fd_is_regular_file(right.as_raw_fd()));
        assert!(!fd_is_regular_file(-1));
        assert!(!fd_is_regular_file(libc::c_int::MAX));
        left.write_all(b"alive").unwrap();
        let mut received = [0; 5];
        right.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"alive");
    }
}

use std::io::{self, Write as _};
use std::sync::{Once, OnceLock};
use std::time::{Duration, Instant};
use std::{mem, thread};

use parking_lot::{Condvar, Mutex};

use super::Event;
use crate::config::LogFormat;

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
    dropped_text: u64,
    dropped_json: u64,
}

struct LogSink {
    pending: Mutex<Pending>,
    /// Signalled when the buffer becomes non-empty.
    ready: Condvar,
    /// Signalled when the writer has drained everything it took.
    drained: Condvar,
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
        let sink: &'static LogSink = Box::leak(Box::new(LogSink::new()));
        // Publish the buffer only once its writer exists. A queue nothing
        // drains is the one state `write_line` must never find.
        if thread::Builder::new()
            .name("h2corn-access-log".to_owned())
            .spawn(move || drain_forever(sink))
            .is_ok()
        {
            SINK.get_or_init(|| sink);
        }
    });
}

/// Queue one formatted line, or write it inline if there is no writer.
pub(crate) fn write_line(format: LogFormat, line: &[u8]) {
    let Some(sink) = SINK.get() else {
        drop(io::stderr().write_all(line));
        return;
    };

    sink.enqueue(format, line);
}

/// Wait for queued lines to reach stderr, giving up after `timeout`.
///
/// Called on graceful shutdown so a worker's last requests are logged.
pub(crate) fn flush(timeout: Duration) {
    let Some(sink) = SINK.get() else {
        return;
    };
    let deadline = Instant::now()
        .checked_add(timeout)
        .expect("log flush timeout fits monotonic clock");
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
        fd_is_regular_file(libc::STDERR_FILENO)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

#[cfg(unix)]
fn fd_is_regular_file(fd: libc::c_int) -> bool {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat initializes stat completely when it succeeds.
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return false;
    }
    // SAFETY: the preceding successful fstat initialized every stat field.
    let stat = unsafe { stat.assume_init() };
    stat.st_mode & libc::S_IFMT == libc::S_IFREG
}

fn drain_forever(sink: &'static LogSink) -> ! {
    let mut stderr = io::stderr();
    loop {
        sink.drain_once(&mut stderr);
    }
}

impl LogSink {
    fn new() -> Self {
        Self {
            pending: Mutex::new(Pending {
                lines: Vec::with_capacity(INITIAL_CAPACITY),
                spare: Vec::with_capacity(INITIAL_CAPACITY),
                writing: false,
                dropped_text: 0,
                dropped_json: 0,
            }),
            ready: Condvar::new(),
            drained: Condvar::new(),
        }
    }

    fn enqueue(&self, format: LogFormat, line: &[u8]) {
        let mut pending = self.pending.lock();
        if !pending.lines.is_empty() && line.len() > CAPACITY.saturating_sub(pending.lines.len()) {
            let dropped = match format {
                LogFormat::Text => &mut pending.dropped_text,
                LogFormat::Json => &mut pending.dropped_json,
            };
            *dropped = dropped.saturating_add(1);
            return;
        }
        let was_empty = pending.lines.is_empty();
        pending.lines.extend_from_slice(line);
        drop(pending);
        // Only the empty -> non-empty transition can have a parked writer to wake;
        // while it is writing there is no waiter and the notify costs nothing.
        if was_empty {
            self.ready.notify_one();
        }
    }

    fn drain_once(&self, output: &mut impl io::Write) {
        // The two buffers swap roles every round: producers fill one while the
        // writer drains the other, so a batch never blocks an append.
        let (mut batch, dropped_text, dropped_json) = {
            let mut pending = self.pending.lock();
            while pending.lines.is_empty() {
                self.ready.wait(&mut pending);
            }
            let spare = mem::take(&mut pending.spare);
            pending.writing = true;
            (
                mem::replace(&mut pending.lines, spare),
                mem::take(&mut pending.dropped_text),
                mem::take(&mut pending.dropped_json),
            )
        };

        for (format, dropped) in [
            (LogFormat::Text, dropped_text),
            (LogFormat::Json, dropped_json),
        ] {
            if dropped == 0 {
                continue;
            }
            let notice = Event::Failed.encode(
                format,
                format_args!("dropped {dropped} access log lines: sink too slow"),
                |fields| {
                    fields.str("subject", "access log sink");
                    fields.str("detail", "sink too slow");
                    fields.num("dropped_lines", dropped);
                },
            );
            // Enqueueing here could drop the notice into the saturated sink again.
            drop(output.write_all(notice.as_slice()));
        }
        // One batch, one write. Linux holds the file's lock for the whole of a
        // regular-file write, so a batch appended by one worker cannot have
        // another worker's batch spliced into the middle of a line. Splitting
        // it here — at any fixed size, since lines do not land on that
        // boundary — is what would break that.
        drop(output.write_all(&batch));

        batch.clear();
        let mut pending = self.pending.lock();
        pending.spare = mem::take(&mut batch);
        pending.writing = false;
        if pending.lines.is_empty() {
            self.drained.notify_all();
        }
    }
}
