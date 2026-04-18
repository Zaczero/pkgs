use std::path::Path;

#[cfg(unix)]
use rustix::fs::{Advice, fadvise};
use std::fs::File as SyncFile;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::task::spawn_blocking;

use crate::config::PATHSEND_BUFFER_SIZE;
use crate::error::{ErrorExt, H2CornError, PathsendError};

#[derive(Debug)]
pub struct PathStreamer {
    file: File,
    buffer: Option<Box<[u8]>>,
    filled: usize,
    offset: usize,
    remaining_len: usize,
    next_file_offset: u64,
    pub(crate) end_stream: bool,
}

impl PathStreamer {
    pub fn new(file: File, len: usize, end_stream: bool) -> Self {
        Self {
            file,
            buffer: None,
            filled: 0,
            offset: 0,
            remaining_len: len,
            next_file_offset: 0,
            end_stream,
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

    pub fn is_drained(&self) -> bool {
        self.offset == self.filled && self.remaining_len == 0
    }

    pub fn remaining(&self) -> &[u8] {
        self.buffer
            .as_deref()
            .map_or(&[], |buffer| &buffer[self.offset..self.filled])
    }

    pub fn consume(&mut self, len: usize) {
        self.offset += len;
        if self.offset == self.filled {
            self.offset = 0;
            self.filled = 0;
        }
    }

    pub fn needs_fill(&self) -> bool {
        self.offset == self.filled && self.remaining_len != 0
    }

    pub fn sendfile_remaining_len(&self) -> usize {
        self.remaining_len
    }

    pub fn sendfile_parts(&mut self) -> (&mut File, &mut u64) {
        (&mut self.file, &mut self.next_file_offset)
    }

    pub fn advance_after_sendfile(&mut self, len: usize) {
        debug_assert!(len <= self.remaining_len);
        self.remaining_len -= len;
    }
}

pub(crate) async fn open_pathsend_file(
    path: &Path,
    len_hint: Option<usize>,
) -> Result<(File, usize), H2CornError> {
    if !path.is_absolute() {
        return PathsendError::RequiresAbsoluteFilePath.err();
    }

    let path = path.to_path_buf();
    let (file, len) = spawn_blocking(move || {
        let pathsend_io_error = |err| PathsendError::open_failed(&path, err);
        let file = SyncFile::open(&path).map_err(pathsend_io_error)?;
        let len = if let Some(len) = len_hint {
            len
        } else {
            file.metadata().map_err(pathsend_io_error)?.len() as usize
        };
        if len >= PATHSEND_BUFFER_SIZE {
            #[cfg(unix)]
            let _ = fadvise(&file, 0, None, Advice::Sequential);
        }
        Ok::<_, PathsendError>((file, len))
    })
    .await??;

    Ok((File::from_std(file), len))
}
