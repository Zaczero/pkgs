mod driver;
mod flush;
mod header_encode;
mod ingress;
mod stream_state;

use smallvec::SmallVec;

pub(super) use self::driver::{ConnectionHandle, WriterState, init_writer};
use crate::bridge::PayloadBytes;
use crate::frame::{ErrorCode, PeerSettings, StreamId, WindowIncrement};
use crate::http::pathsend::PathStreamer;
use crate::http::types::{HttpStatusCode, ResponseHeaders};
use crate::smallvec_deque::SmallVecDeque;

const WRITER_CHANNEL_CAPACITY: usize = 64;
const ENCODED_HEADER_BLOCK_CAPACITY: usize = 1024;
const FRAME_BUFFER_CAPACITY: usize = 64;
const FAIR_WRITE_QUANTUM: usize = 64 * 1024;
const H2_WRITER_BUFFER_CAPACITY: usize = 8 * 1024;
const H2_OUTBOUND_DATA_FRAME_SIZE_TARGET: usize = 64 * 1024;

type ResponseCloseBatch = SmallVec<[StreamId; 8]>;
pub(super) type WriterCommandBatch = SmallVecDeque<WriterCommand, 3>;

#[derive(Debug)]
pub(super) enum WindowTarget {
    Connection,
    Stream(StreamId),
}

#[derive(Debug)]
pub(super) enum WriterCommand {
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
