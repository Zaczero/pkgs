use std::fs::File;
use std::io::{self, Read};
use std::mem;
use std::ops::Range;
use std::path::PathBuf;

use bytes::Bytes;
#[cfg(target_os = "macos")]
use rustix::fs::fcntl_rdadvise;
#[cfg(target_os = "linux")]
use rustix::fs::{Advice, fadvise};
use tokio::task::{JoinHandle, spawn_blocking};

use crate::config::{PATHSEND_PRELOAD_MAX, PATHSEND_READ_BUFFER_SIZE};
use crate::error::{H2CornError, PathsendError};

/// Below this size the HTTP/2 path serves files from the rolling read buffer,
/// whose DATA frames can share vectored writes. Larger files retain sendfile
/// so avoiding the userspace payload copy can amortize its per-frame setup.
pub(crate) const PATHSEND_SENDFILE_MIN: usize = 1 << 20;

#[derive(Debug)]
pub(crate) struct PathStreamer {
    read: PathReadState,
    cursor: PathCursor,
    pub(crate) end_stream: bool,
    /// Decided once per response from the total length (mode flapping
    /// mid-stream would complicate end-of-stream bookkeeping).
    pub(crate) prefers_sendfile: bool,
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
    const fn new(len: usize) -> Self {
        Self {
            buffered: 0..0,
            unread_file_len: len,
            file_offset: 0,
        }
    }

    const fn is_drained(&self) -> bool {
        self.buffered.start == self.buffered.end && self.unread_file_len == 0
    }

    const fn needs_fill(&self) -> bool {
        self.buffered.start == self.buffered.end && self.unread_file_len != 0
    }

    const fn record_read(&mut self, read: usize) {
        self.buffered = 0..read;
        self.file_offset += read as u64;
        if read == 0 {
            self.unread_file_len = 0;
        } else {
            self.unread_file_len = self.unread_file_len.saturating_sub(read);
        }
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

#[derive(Debug)]
struct PathReadResult {
    file: File,
    buffer: Box<[u8]>,
    read: io::Result<usize>,
}

impl PathStreamer {
    pub(crate) const fn new(file: File, len: usize, end_stream: bool) -> Self {
        Self {
            read: PathReadState::Ready { file, buffer: None },
            cursor: PathCursor::new(len),
            end_stream,
            prefers_sendfile: len >= PATHSEND_SENDFILE_MIN,
        }
    }

    pub(crate) async fn fill(&mut self) -> Result<(), H2CornError> {
        debug_assert!(self.cursor.buffered.is_empty());
        if let PathReadState::Ready { .. } = self.read {
            let PathReadState::Ready { mut file, buffer } =
                mem::replace(&mut self.read, PathReadState::Failed)
            else {
                unreachable!("the ready state was matched")
            };
            let mut buffer =
                buffer.unwrap_or_else(|| vec![0; PATHSEND_READ_BUFFER_SIZE].into_boxed_slice());
            let read_len = buffer.len().min(self.cursor.unread_file_len);
            self.read = PathReadState::Reading(spawn_blocking(move || {
                let read = file.read(&mut buffer[..read_len]);
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
        self.cursor.record_read(read);
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
    File(Box<File>),
}

pub(crate) async fn open_pathsend_file(
    path: PathBuf,
    len_hint: Option<usize>,
) -> Result<(PathSource, usize), H2CornError> {
    Ok(spawn_blocking(move || {
        let pathsend_io_error = |err| PathsendError::open_failed(&path, err);
        let mut file = File::open(&path).map_err(pathsend_io_error)?;
        let len = if let Some(len) = len_hint {
            len
        } else {
            file.metadata().map_err(pathsend_io_error)?.len() as usize
        };
        if len <= PATHSEND_PRELOAD_MAX {
            let mut data = Vec::with_capacity(len);
            (&mut file)
                .take(len as u64)
                .read_to_end(&mut data)
                .map_err(pathsend_io_error)?;
            return Ok((PathSource::Buffered(data.into()), len));
        }
        // Best-effort sequential read-ahead hint for the streamed file.
        #[cfg(target_os = "linux")]
        let _ = fadvise(&file, 0, None, Advice::Sequential);
        #[cfg(target_os = "macos")]
        let _ = fcntl_rdadvise(&file, 0, len as u64);
        Ok::<_, PathsendError>((PathSource::File(Box::new(file)), len))
    })
    .await??)
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs::{File, remove_file, write};
    use std::io::Read;
    use std::mem::size_of;
    use std::path::PathBuf;
    use std::process::id;
    use std::sync::mpsc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio::task::{spawn_blocking, yield_now};

    use super::{PATHSEND_SENDFILE_MIN, PathReadResult, PathReadState, PathStreamer};
    use crate::config::PATHSEND_READ_BUFFER_SIZE;

    #[test]
    fn accounting_cursor_preserves_streamer_layout_budget() {
        assert!(size_of::<PathStreamer>() <= 64);
    }

    #[test]
    fn sendfile_cursor_accounts_partial_offset_progress_on_drop() {
        let path = temp_file(b"partial");
        let file = File::open(&path).expect("temporary pathsend file opens");
        let mut streamer = PathStreamer::new(file, PATHSEND_SENDFILE_MIN, true);

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
        let mut streamer = PathStreamer::new(file, payload.len(), true);
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
    async fn cancelled_fill_resumes_the_owned_blocking_read() {
        let payload = vec![b'y'; 4096];
        let payload_len = payload.len();
        let path = temp_file(&payload);
        let mut file = File::open(&path).expect("temporary pathsend file opens");
        let mut streamer = PathStreamer::new(
            File::open(&path).expect("temporary pathsend file opens"),
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
}
