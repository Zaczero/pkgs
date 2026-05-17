mod driver;
mod flush;
mod header_encode;
mod ingress;
mod stream_state;

use std::io;

use smallvec::SmallVec;
use tokio::fs::File;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf as TcpOwnedWriteHalf;
#[cfg(unix)]
use tokio::net::unix::OwnedWriteHalf as UnixOwnedWriteHalf;

pub use self::driver::{ConnectionHandle, WriterState, init_writer};
use crate::bridge::PayloadBytes;
use crate::frame::{ErrorCode, PeerSettings, StreamId, WindowIncrement};
use crate::http::pathsend::PathStreamer;
use crate::http::types::{HttpStatusCode, ResponseHeaders};
use crate::sendfile::sendfile_all_tcp;
use crate::smallvec_deque::SmallVecDeque;

const WRITER_CHANNEL_CAPACITY: usize = 64;
const ENCODED_HEADER_BLOCK_CAPACITY: usize = 1024;
const FRAME_BUFFER_CAPACITY: usize = 64;
const FAIR_WRITE_QUANTUM: usize = 64 * 1024;
const H2_WRITER_BUFFER_CAPACITY: usize = 64 * 1024;
const H2_OUTBOUND_DATA_FRAME_SIZE_TARGET: usize = 64 * 1024;

type ResponseCloseBatch = SmallVec<[StreamId; 8]>;
pub type WriterCommandBatch = SmallVecDeque<WriterCommand, 3>;

pub trait H2WriteTarget: AsyncWrite + Unpin + Send + Sync + 'static {
    const SUPPORTS_SENDFILE: bool;

    async fn write_file_chunk(
        writer: &mut BufWriter<Self>,
        header: [u8; 9],
        file: &mut File,
        offset: &mut u64,
        len: usize,
    ) -> io::Result<()>
    where
        Self: Sized;
}

impl H2WriteTarget for TcpOwnedWriteHalf {
    const SUPPORTS_SENDFILE: bool = true;

    async fn write_file_chunk(
        writer: &mut BufWriter<Self>,
        header: [u8; 9],
        file: &mut File,
        offset: &mut u64,
        len: usize,
    ) -> io::Result<()> {
        writer.write_all(&header).await?;
        writer.flush().await?;
        sendfile_all_tcp(writer, file, offset, len).await
    }
}

#[cfg(unix)]
impl H2WriteTarget for UnixOwnedWriteHalf {
    const SUPPORTS_SENDFILE: bool = false;

    async fn write_file_chunk(
        _writer: &mut BufWriter<Self>,
        _header: [u8; 9],
        _file: &mut File,
        _offset: &mut u64,
        _len: usize,
    ) -> io::Result<()> {
        unreachable!("unix H2 writer does not use direct sendfile")
    }
}

#[derive(Debug)]
pub enum WindowTarget {
    Connection,
    Stream(StreamId),
}

#[derive(Debug)]
pub enum WriterCommand {
    SendHeaders {
        stream_id: StreamId,
        status: HttpStatusCode,
        headers: ResponseHeaders,
        end_stream: bool,
    },
    SendFinal {
        stream_id: StreamId,
        status: HttpStatusCode,
        headers: ResponseHeaders,
        data: PayloadBytes,
    },
    SendTrailers {
        stream_id: StreamId,
        headers: ResponseHeaders,
    },
    SendData {
        stream_id: StreamId,
        data: PayloadBytes,
        end_stream: bool,
    },
    SendPath {
        stream_id: StreamId,
        // Boxed because `PathStreamer` is 160+ bytes and `SendPath` is rare
        // (only `http.response.pathsend` triggers it); inlining inflates every
        // `WriterCommand` slot — including the per-stream `SmallVecDeque` — to
        // ~168 bytes when 80 would otherwise be enough for the common send/data
        // commands.
        streamer: Box<PathStreamer>,
    },
    FlushBufferedOutput,
    SendReset {
        stream_id: StreamId,
        error_code: ErrorCode,
    },
    SendSettingsAck,
    UpdatePeerSettings(PeerSettings),
    PeerReset {
        stream_id: StreamId,
    },
    GrantSendWindow {
        target: WindowTarget,
        increment: WindowIncrement,
    },
    SendWindowUpdate {
        target: WindowTarget,
        increment: WindowIncrement,
    },
    PingAck([u8; 8]),
    Goaway {
        last_stream_id: Option<StreamId>,
        error_code: ErrorCode,
        debug: Vec<u8>,
        close: bool,
    },
}

#[cfg(test)]
mod size_check {
    use super::*;
    use crate::bridge::PayloadBytes;
    use crate::http::pathsend::PathStreamer;
    use crate::http::response::ResponseAction;

    /// Guard against accidental enum bloat — `WriterCommand` is queued by value
    /// in `SmallVecDeque<_, 3>` slots and `ResponseAction` in `SmallVec<_, 2>`,
    /// so growth on either type silently inflates per-stream and per-request
    /// memory across the whole writer pipeline. Bumping these caps should be a
    /// deliberate decision after weighing the per-connection footprint.
    #[test]
    fn writer_command_and_response_action_sizes_are_bounded() {
        assert!(
            size_of::<WriterCommand>() <= 72,
            "WriterCommand grew: {} bytes",
            size_of::<WriterCommand>(),
        );
        assert!(
            size_of::<ResponseAction>() <= 96,
            "ResponseAction grew: {} bytes",
            size_of::<ResponseAction>(),
        );
        assert!(size_of::<PayloadBytes>() <= 48);
        assert!(size_of::<PathStreamer>() <= 192);
    }
}
