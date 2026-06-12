use std::fs::File as SyncFile;
use std::io::Read;
use std::path::PathBuf;

use bytes::Bytes;
#[cfg(target_os = "linux")]
use rustix::fs::{Advice, fadvise};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::task::spawn_blocking;

use crate::config::PATHSEND_BUFFER_SIZE;
use crate::error::{H2CornError, PathsendError};

/// Below this size the HTTP/2 path serves files from the read buffer with
/// vectored frame writes (one syscall for many frames) instead of per-frame
/// sendfile: peers cap DATA frames (typically 16 KiB), so per-frame sendfile
/// costs ~2 syscalls per frame and never amortizes. Zero-copy still wins on
/// large files where memory bandwidth dominates.
pub const PATHSEND_SENDFILE_MIN: usize = 1 << 20;

#[derive(Debug)]
pub struct PathStreamer {
    file: File,
    buffer: Option<Box<[u8]>>,
    filled: usize,
    offset: usize,
    remaining_len: usize,
    next_file_offset: u64,
    pub(crate) end_stream: bool,
    /// Decided once per response from the total length (mode flapping
    /// mid-stream would complicate end-of-stream bookkeeping).
    pub(crate) prefers_sendfile: bool,
}

impl PathStreamer {
    pub const fn new(file: File, len: usize, end_stream: bool) -> Self {
        Self {
            file,
            buffer: None,
            filled: 0,
            offset: 0,
            remaining_len: len,
            next_file_offset: 0,
            end_stream,
            prefers_sendfile: len >= PATHSEND_SENDFILE_MIN,
        }
    }

    pub async fn fill(&mut self) -> Result<(), H2CornError> {
        assert_eq!(self.offset, self.filled);
        let mut buffer = self
            .buffer
            .take()
            .unwrap_or_else(|| vec![0; PATHSEND_BUFFER_SIZE].into_boxed_slice());
        let read_len = buffer.len().min(self.remaining_len);
        let read = self.file.read(&mut buffer[..read_len]).await?;
        self.buffer = Some(buffer);
        self.offset = 0;
        self.filled = read;
        self.next_file_offset += read as u64;
        if read == 0 {
            self.remaining_len = 0;
        } else {
            self.remaining_len = self.remaining_len.saturating_sub(read);
        }
        Ok(())
    }

    pub const fn is_drained(&self) -> bool {
        self.offset == self.filled && self.remaining_len == 0
    }

    pub fn remaining(&self) -> &[u8] {
        self.buffer
            .as_deref()
            .map_or(&[], |buffer| &buffer[self.offset..self.filled])
    }

    pub const fn consume(&mut self, len: usize) {
        self.offset += len;
        if self.offset == self.filled {
            self.offset = 0;
            self.filled = 0;
        }
    }

    pub const fn needs_fill(&self) -> bool {
        self.offset == self.filled && self.remaining_len != 0
    }

    pub const fn sendfile_remaining_len(&self) -> usize {
        self.remaining_len
    }

    pub const fn sendfile_parts(&mut self) -> (&mut File, &mut u64) {
        (&mut self.file, &mut self.next_file_offset)
    }

    pub fn advance_after_sendfile(&mut self, len: usize) {
        debug_assert!(len <= self.remaining_len);
        self.remaining_len -= len;
    }
}

/// What [`open_pathsend_file`] produced in its single blocking hop.
#[derive(Debug)]
pub enum PathSource {
    /// Whole file preloaded (≤ [`PATHSEND_BUFFER_SIZE`]) and already closed:
    /// served through the ordinary body path with the same per-stream memory
    /// bound as the rolling buffer, but open + read + close collapse into
    /// one blocking-pool hop instead of one per operation.
    Buffered(Bytes),
    /// Open handle for the rolling-read (> buffer size) and sendfile
    /// (≥ [`PATHSEND_SENDFILE_MIN`]) tiers.
    File(File),
}

pub async fn open_pathsend_file(
    path: PathBuf,
    len_hint: Option<usize>,
) -> Result<(PathSource, usize), H2CornError> {
    Ok(spawn_blocking(move || {
        let pathsend_io_error = |err| PathsendError::open_failed(&path, err);
        let mut file = SyncFile::open(&path).map_err(pathsend_io_error)?;
        let len = if let Some(len) = len_hint {
            len
        } else {
            file.metadata().map_err(pathsend_io_error)?.len() as usize
        };
        if len <= PATHSEND_BUFFER_SIZE {
            let mut data = Vec::with_capacity(len);
            (&mut file)
                .take(len as u64)
                .read_to_end(&mut data)
                .map_err(pathsend_io_error)?;
            return Ok((PathSource::Buffered(data.into()), len));
        }
        #[cfg(target_os = "linux")]
        let _ = fadvise(&file, 0, None, Advice::Sequential);
        Ok::<_, PathsendError>((PathSource::File(File::from_std(file)), len))
    })
    .await??)
}
