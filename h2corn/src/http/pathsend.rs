use std::fs::File;
use std::io::{self, Read as _};
use std::mem;
use std::ops::Range;
use std::path::PathBuf;

use bytes::Bytes;
#[cfg(target_os = "macos")]
use rustix::fs::fcntl_rdadvise;
#[cfg(target_os = "linux")]
use rustix::fs::{Advice, fadvise};
#[cfg(unix)]
use rustix::fs::{CWD, FileType, Mode, OFlags, fstat, openat};
use tokio::task::{JoinHandle, spawn_blocking};

use crate::config::{PATHSEND_PRELOAD_MAX, PATHSEND_READ_BUFFER_SIZE};
use crate::error::{H2CornError, PathsendError};
use crate::http::response::ResponseBytePermit;

/// Below this size the HTTP/2 path serves files from the rolling read buffer,
/// whose DATA frames can share vectored writes. Larger files retain sendfile
/// so avoiding the userspace payload copy can amortize its per-frame setup.
pub(crate) const PATHSEND_SENDFILE_MIN: usize = 1 << 20;

#[derive(Debug)]
pub(crate) struct FileStreamer {
    read: PathReadState,
    cursor: PathCursor,
    pub(crate) end_stream: bool,
    /// Decided once per response from the total length (mode flapping
    /// mid-stream would complicate end-of-stream bookkeeping).
    pub(crate) prefers_sendfile: bool,
    /// Admission credit for a queued segment, released when this is dropped.
    ///
    /// The streamer outlives the `FileSegment` it came from -- it *is* what
    /// sits in the HTTP/2 body queue -- so the credit has to move here, or the
    /// descriptor bound would end the moment the segment was lowered into a
    /// command.
    ///
    /// Boxed, and this one is measured: inline it takes the streamer past a
    /// buffered chunk in size, which then sets every slot of the writer's
    /// inline body queue. `None` for pathsend, so the allocation only happens
    /// for a credited zerocopysend segment -- next to the file I/O it is
    /// already doing.
    _credit: Option<Box<ResponseBytePermit>>,
}

#[derive(Debug)]
struct PathCursor {
    buffered: Range<usize>,
    unread_file_len: usize,
    file_offset: u64,
}

/// Exclusive sendfile view. The kernel-facing offset is the source of truth:
/// dropping the cursor accounts every byte advanced, including a partial
/// write followed by an error.
pub(crate) struct SendfileCursor<'a> {
    file: &'a mut File,
    file_offset: &'a mut u64,
    unread_file_len: &'a mut usize,
    initial_offset: u64,
}

impl SendfileCursor<'_> {
    pub(crate) const fn remaining(&self) -> usize {
        *self.unread_file_len
    }

    pub(crate) const fn parts(&mut self) -> (&mut File, &mut u64) {
        (self.file, self.file_offset)
    }
}

impl Drop for SendfileCursor<'_> {
    fn drop(&mut self) {
        let advanced = self.file_offset.saturating_sub(self.initial_offset);
        let advanced = usize::try_from(advanced).unwrap_or(usize::MAX);
        debug_assert!(advanced <= *self.unread_file_len);
        *self.unread_file_len = (*self.unread_file_len).saturating_sub(advanced);
    }
}

impl PathCursor {
    const fn new(start: u64, len: usize) -> Self {
        Self {
            buffered: 0..0,
            unread_file_len: len,
            file_offset: start,
        }
    }

    const fn is_drained(&self) -> bool {
        self.buffered.start == self.buffered.end && self.unread_file_len == 0
    }

    const fn needs_fill(&self) -> bool {
        self.buffered.start == self.buffered.end && self.unread_file_len != 0
    }

    fn record_read(&mut self, read: usize) -> io::Result<()> {
        self.buffered = 0..read;
        self.file_offset += read as u64;
        if read == 0 {
            if self.unread_file_len != 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
        } else {
            self.unread_file_len = self.unread_file_len.saturating_sub(read);
        }
        Ok(())
    }

    fn consume_buffered(&mut self, len: usize) {
        debug_assert!(len <= self.buffered.end - self.buffered.start);
        self.buffered.start += len;
        if self.buffered.start == self.buffered.end {
            self.buffered = 0..0;
        }
    }
}

#[derive(Debug)]
enum PathReadState {
    Ready {
        file: File,
        buffer: Option<Box<[u8]>>,
    },
    Reading(JoinHandle<PathReadResult>),
    Failed,
}

impl PathReadState {
    /// Take a ready file and its buffer, leaving `Failed`. Any other state is
    /// put back untouched, so the caller never has to re-match -- and panic on
    /// -- an arm it has already proved impossible.
    fn take_ready(&mut self) -> Option<(File, Option<Box<[u8]>>)> {
        match mem::replace(self, Self::Failed) {
            Self::Ready { file, buffer } => Some((file, buffer)),
            other => {
                *self = other;
                None
            },
        }
    }
}

#[derive(Debug)]
struct PathReadResult {
    file: File,
    buffer: Box<[u8]>,
    read: io::Result<usize>,
}

impl FileStreamer {
    /// `start` is the file offset the transfer begins at: zero for a pathsend
    /// file this server opened itself, and the resolved `offset` for a
    /// `zerocopysend` range over a descriptor the application still owns.
    pub(crate) const fn new(file: File, start: u64, len: usize, end_stream: bool) -> Self {
        Self::with_credit(file, start, len, end_stream, None)
    }

    pub(crate) const fn with_credit(
        file: File,
        start: u64,
        len: usize,
        end_stream: bool,
        credit: Option<Box<ResponseBytePermit>>,
    ) -> Self {
        Self {
            read: PathReadState::Ready { file, buffer: None },
            cursor: PathCursor::new(start, len),
            end_stream,
            prefers_sendfile: len >= PATHSEND_SENDFILE_MIN,
            _credit: credit,
        }
    }

    pub(crate) async fn fill(&mut self) -> Result<(), H2CornError> {
        debug_assert!(self.cursor.buffered.is_empty());
        if let Some((file, buffer)) = self.read.take_ready() {
            // Sized to the segment, not to the constant: pathsend only reaches
            // here for files above its preload threshold, but a zerocopysend
            // range may be a few bytes, and allocating and zeroing 128 KiB for
            // them turns a descriptor-and-range message into real work. A
            // reused buffer is kept whatever its size.
            let mut buffer = buffer.unwrap_or_else(|| {
                vec![0; PATHSEND_READ_BUFFER_SIZE.min(self.cursor.unread_file_len)]
                    .into_boxed_slice()
            });
            let read_len = buffer.len().min(self.cursor.unread_file_len);
            // Positional, like the sendfile path beside it, which already
            // takes an explicit offset. The cursor is the single source of
            // truth for where the transfer is, so the handle's own position is
            // never read and never moved -- the two paths cannot disagree, and
            // neither depends on holding an exclusive handle.
            let offset = self.cursor.file_offset;
            self.read = PathReadState::Reading(spawn_blocking(move || {
                let read = read_at(&file, &mut buffer[..read_len], offset);
                PathReadResult { file, buffer, read }
            }));
        }

        let joined = match &mut self.read {
            PathReadState::Reading(task) => task.await,
            PathReadState::Ready { .. } | PathReadState::Failed => {
                unreachable!("fill always owns or resumes one read task")
            },
        };
        let PathReadResult { file, buffer, read } = match joined {
            Ok(result) => result,
            Err(err) => {
                self.read = PathReadState::Failed;
                return Err(err.into());
            },
        };
        self.read = PathReadState::Ready {
            file,
            buffer: Some(buffer),
        };
        let read = read?;
        self.cursor.record_read(read)?;
        Ok(())
    }

    pub(crate) const fn is_drained(&self) -> bool {
        self.cursor.is_drained()
    }

    pub(crate) fn remaining(&self) -> &[u8] {
        match &self.read {
            PathReadState::Ready { buffer, .. } => buffer
                .as_deref()
                .map_or(&[], |buffer| &buffer[self.cursor.buffered.clone()]),
            PathReadState::Reading(_) | PathReadState::Failed => &[],
        }
    }

    pub(crate) fn consume(&mut self, len: usize) {
        self.cursor.consume_buffered(len);
    }

    pub(crate) const fn needs_fill(&self) -> bool {
        self.cursor.needs_fill()
    }

    pub(crate) const fn buffered_is_final(&self) -> bool {
        self.cursor.unread_file_len == 0
    }

    pub(crate) const fn sendfile_cursor(&mut self) -> Option<SendfileCursor<'_>> {
        if !self.prefers_sendfile || !self.cursor.needs_fill() {
            return None;
        }
        let PathReadState::Ready { file, .. } = &mut self.read else {
            return None;
        };
        let initial_offset = self.cursor.file_offset;
        Some(SendfileCursor {
            file,
            file_offset: &mut self.cursor.file_offset,
            unread_file_len: &mut self.cursor.unread_file_len,
            initial_offset,
        })
    }
}

/// What [`open_pathsend_file`] produced in its single blocking hop.
#[derive(Debug)]
pub(crate) enum PathSource {
    /// Whole file preloaded (≤ [`PATHSEND_PRELOAD_MAX`]) and already closed:
    /// served through the ordinary body path with the same per-stream memory
    /// bound as the rolling buffer, but open + read + close collapse into
    /// one blocking-pool hop instead of one per operation.
    Buffered(Bytes),
    /// Open handle for the rolling-read (> preload limit) and sendfile
    /// (≥ [`PATHSEND_SENDFILE_MIN`]) tiers.
    // Boxed at the blocking boundary because the response action already needs
    // rare file bodies boxed to keep its common variants compact.
    File(File),
}

pub(crate) async fn open_pathsend_file(path: PathBuf) -> Result<(PathSource, usize), H2CornError> {
    Ok(spawn_blocking(move || open_pathsend_file_blocking(path)).await??)
}

/// Read at an explicit offset without disturbing the handle's position.
#[cfg(unix)]
pub(crate) fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buffer, offset)
}

#[cfg(windows)]
pub(crate) fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buffer, offset)
}

/// Single blocking-pool hop: open the path, admit only a regular file, then
/// either preload or retain the same descriptor. Length comes from that fd's
/// fstat (streamed) or from the bytes actually read (preloaded) — never from
/// application `Content-Length`.
#[cfg(unix)]
fn open_pathsend_file_blocking(path: PathBuf) -> Result<(PathSource, usize), PathsendError> {
    let pathsend_io_error = |err: rustix::io::Errno| PathsendError::open_failed(&path, err.into());
    // NONBLOCK so FIFOs / other blocking objects cannot pin a pool thread on
    // open; the subsequent fstat type check rejects anything that is not a
    // regular file. No NOFOLLOW: a symlink to a regular file is a normal
    // file-serving case.
    let fd = openat(
        CWD,
        &path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(pathsend_io_error)?;
    let stat = fstat(&fd).map_err(pathsend_io_error)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
        return Err(PathsendError::NotRegularFile { path });
    }
    let len = usize::try_from(stat.st_size).map_err(|_| {
        PathsendError::open_failed(
            &path,
            io::Error::new(io::ErrorKind::InvalidData, "file size does not fit usize"),
        )
    })?;
    let mut file = File::from(fd);
    if len <= PATHSEND_PRELOAD_MAX {
        let mut data = Vec::with_capacity(len);
        (&mut file)
            .take(len as u64)
            .read_to_end(&mut data)
            .map_err(|err| PathsendError::open_failed(&path, err))?;
        // Preloaded length is the bytes actually read, not the fstat snapshot
        // alone — a concurrent truncate is reflected in the response length.
        let read_len = data.len();
        return Ok((PathSource::Buffered(data.into()), read_len));
    }
    // Best-effort sequential read-ahead hint for the streamed file.
    #[cfg(target_os = "linux")]
    let _ = fadvise(&file, 0, None, Advice::Sequential);
    #[cfg(target_os = "macos")]
    let _ = fcntl_rdadvise(&file, 0, len as u64);
    Ok((PathSource::File(file), len))
}

#[cfg(windows)]
fn open_pathsend_file_blocking(path: PathBuf) -> Result<(PathSource, usize), PathsendError> {
    let mut file = File::open(&path).map_err(|err| PathsendError::open_failed(&path, err))?;
    let metadata = file
        .metadata()
        .map_err(|err| PathsendError::open_failed(&path, err))?;
    if !metadata.is_file() {
        return Err(PathsendError::NotRegularFile { path });
    }
    let len = usize::try_from(metadata.len()).map_err(|_| {
        PathsendError::open_failed(
            &path,
            io::Error::new(io::ErrorKind::InvalidData, "file size does not fit usize"),
        )
    })?;
    if len <= PATHSEND_PRELOAD_MAX {
        let mut data = Vec::with_capacity(len);
        (&mut file)
            .take(len as u64)
            .read_to_end(&mut data)
            .map_err(|err| PathsendError::open_failed(&path, err))?;
        let read_len = data.len();
        return Ok((PathSource::Buffered(data.into()), read_len));
    }
    Ok((PathSource::File(file), len))
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs::{File, create_dir, remove_dir, remove_file, write};
    use std::io::Read as _;
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::process::id;
    use std::sync::mpsc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rustix::fs::{CWD, Mode, mkfifoat};
    use tokio::task::{spawn_blocking, yield_now};

    use super::{
        FileStreamer, PATHSEND_SENDFILE_MIN, PathReadResult, PathReadState, PathSource,
        open_pathsend_file, open_pathsend_file_blocking,
    };
    use crate::config::{PATHSEND_PRELOAD_MAX, PATHSEND_READ_BUFFER_SIZE};
    use crate::error::{ErrorKind, PathsendError};

    #[test]
    fn sendfile_cursor_accounts_partial_offset_progress_on_drop() {
        let path = temp_file(b"partial");
        let file = File::open(&path).expect("temporary pathsend file opens");
        let mut streamer = FileStreamer::new(file, 0, PATHSEND_SENDFILE_MIN, true);

        {
            let mut cursor = streamer.sendfile_cursor().expect("sendfile is selected");
            assert_eq!(cursor.remaining(), PATHSEND_SENDFILE_MIN);
            let (_, offset) = cursor.parts();
            *offset += 7;
        }

        assert_eq!(
            streamer
                .sendfile_cursor()
                .expect("remaining file stays sendfile eligible")
                .remaining(),
            PATHSEND_SENDFILE_MIN - 7,
        );
        remove_file(path).expect("temporary pathsend file is removed");
    }

    fn temp_file(payload: &[u8]) -> PathBuf {
        let path = temp_dir().join(format!(
            "h2corn-pathsend-{}-{}",
            id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after the epoch")
                .as_nanos(),
        ));
        write(&path, payload).expect("temporary pathsend file is written");
        path
    }

    #[tokio::test]
    async fn rolling_reader_reuses_one_configured_buffer() {
        let payload = vec![b'x'; PATHSEND_READ_BUFFER_SIZE * 2 + 17];
        let path = temp_file(&payload);
        let file = File::open(&path).expect("temporary pathsend file opens");
        let mut streamer = FileStreamer::new(file, 0, payload.len(), true);
        let mut received = Vec::with_capacity(payload.len());

        while !streamer.is_drained() {
            if streamer.needs_fill() {
                streamer.fill().await.expect("rolling read succeeds");
            }
            let chunk = streamer.remaining();
            let chunk_len = chunk.len();
            received.extend_from_slice(chunk);
            let PathReadState::Ready {
                buffer: Some(buffer),
                ..
            } = &streamer.read
            else {
                panic!("completed reads retain their one reusable buffer")
            };
            assert_eq!(buffer.len(), PATHSEND_READ_BUFFER_SIZE);
            streamer.consume(chunk_len);
        }

        assert_eq!(received, payload);
        remove_file(path).expect("temporary pathsend file is removed");
    }

    #[tokio::test]
    async fn rolling_reader_rejects_eof_before_the_admitted_length() {
        let path = temp_file(b"partial");
        let file = File::open(&path).expect("temporary pathsend file opens");
        let mut streamer = FileStreamer::new(file, 0, b"partial".len() + 1, true);

        streamer
            .fill()
            .await
            .expect("the delivered prefix is read normally");
        let delivered = streamer.remaining().len();
        streamer.consume(delivered);

        let err = streamer
            .fill()
            .await
            .expect_err("a short file cannot end a declared response cleanly");
        assert!(
            matches!(err.kind(), ErrorKind::Io(err) if err.kind() == std::io::ErrorKind::UnexpectedEof)
        );
        assert!(
            !streamer.is_drained(),
            "the missing byte remains observable"
        );
        remove_file(path).expect("temporary pathsend file is removed");
    }

    #[tokio::test]
    async fn cancelled_fill_resumes_the_owned_blocking_read() {
        let payload = vec![b'y'; 4096];
        let payload_len = payload.len();
        let path = temp_file(&payload);
        let mut file = File::open(&path).expect("temporary pathsend file opens");
        let mut streamer = FileStreamer::new(
            File::open(&path).expect("temporary pathsend file opens"),
            0,
            payload_len,
            true,
        );
        let (release_tx, release_rx) = mpsc::channel();
        let mut buffer = vec![0; PATHSEND_READ_BUFFER_SIZE].into_boxed_slice();
        streamer.read = PathReadState::Reading(spawn_blocking(move || {
            release_rx.recv().expect("test releases the blocking read");
            let read = file.read(&mut buffer[..payload_len]);
            PathReadResult { file, buffer, read }
        }));

        let mut first_fill = Box::pin(streamer.fill());
        tokio::select! {
            biased;
            result = &mut first_fill => panic!("gated read completed unexpectedly: {result:?}"),
            () = yield_now() => {},
        }
        drop(first_fill);
        release_tx.send(()).expect("blocking read is released");

        streamer
            .fill()
            .await
            .expect("the next fill resumes the same owned read task");
        assert_eq!(streamer.remaining(), payload);
        remove_file(path).expect("temporary pathsend file is removed");
    }

    #[test]
    fn open_admits_regular_file_and_preloads_under_limit() {
        let payload = b"pathsend-regular";
        let path = temp_file(payload);
        let (source, len) =
            open_pathsend_file_blocking(path.clone()).expect("regular file is admitted");
        assert_eq!(len, payload.len());
        match source {
            PathSource::Buffered(data) => assert_eq!(&data[..], payload),
            PathSource::File(_) => panic!("payload under preload limit must be buffered"),
        }
        remove_file(path).expect("temporary pathsend file is removed");
    }

    #[test]
    fn open_streams_when_over_preload_limit() {
        let payload = vec![b'z'; PATHSEND_PRELOAD_MAX + 1];
        let path = temp_file(&payload);
        let (source, len) =
            open_pathsend_file_blocking(path.clone()).expect("large regular file is admitted");
        assert_eq!(len, payload.len());
        assert!(matches!(source, PathSource::File(_)));
        remove_file(path).expect("temporary pathsend file is removed");
    }

    #[test]
    fn open_rejects_directory() {
        let path = temp_dir().join(format!("h2corn-pathsend-dir-{}-{}", id(), unique_suffix()));
        create_dir(&path).expect("temporary directory is created");
        let err = open_pathsend_file_blocking(path.clone()).expect_err("directory is rejected");
        assert!(matches!(err, PathsendError::NotRegularFile { .. }));
        remove_dir(path).expect("temporary directory is removed");
    }

    #[test]
    fn open_rejects_fifo_without_blocking() {
        let path = temp_dir().join(format!("h2corn-pathsend-fifo-{}-{}", id(), unique_suffix()));
        mkfifoat(CWD, &path, Mode::RUSR | Mode::WUSR).expect("fifo is created");
        let err = open_pathsend_file_blocking(path.clone()).expect_err("fifo is rejected");
        assert!(matches!(err, PathsendError::NotRegularFile { .. }));
        remove_file(path).expect("fifo is removed");
    }

    #[test]
    fn open_rejects_unix_socket() {
        let path = temp_dir().join(format!("h2corn-pathsend-sock-{}-{}", id(), unique_suffix()));
        let _listener = UnixListener::bind(&path).expect("unix socket is bound");
        let err = open_pathsend_file_blocking(path.clone()).expect_err("socket is rejected");
        // Linux reports ENXIO and Darwin reports EOPNOTSUPP before fstat;
        // either OpenFailed or NotRegularFile keeps it out of the body path.
        assert!(
            matches!(
                err,
                PathsendError::NotRegularFile { .. } | PathsendError::OpenFailed { .. }
            ),
            "unexpected rejection: {err:?}"
        );
        remove_file(path).expect("socket is removed");
    }

    #[test]
    fn open_rejects_character_device() {
        let path = PathBuf::from("/dev/null");
        let err = open_pathsend_file_blocking(path).expect_err("device is rejected");
        assert!(matches!(err, PathsendError::NotRegularFile { .. }));
    }

    #[test]
    fn open_follows_symlink_to_regular_file() {
        let payload = b"via-symlink";
        let target = temp_file(payload);
        let link = temp_dir().join(format!("h2corn-pathsend-link-{}-{}", id(), unique_suffix()));
        symlink(&target, &link).expect("symlink is created");
        let (source, len) =
            open_pathsend_file_blocking(link.clone()).expect("symlink to regular file is admitted");
        assert_eq!(len, payload.len());
        match source {
            PathSource::Buffered(data) => assert_eq!(&data[..], payload),
            PathSource::File(_) => panic!("payload under preload limit must be buffered"),
        }
        remove_file(link).expect("symlink is removed");
        remove_file(target).expect("temporary pathsend file is removed");
    }

    #[tokio::test]
    async fn open_async_returns_descriptor_length() {
        let payload = b"async-open";
        let path = temp_file(payload);
        let (source, len) = open_pathsend_file(path.clone())
            .await
            .expect("async open admits regular file");
        assert_eq!(len, payload.len());
        assert!(matches!(source, PathSource::Buffered(_)));
        remove_file(path).expect("temporary pathsend file is removed");
    }

    fn unique_suffix() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after the epoch")
            .as_nanos()
    }
}
