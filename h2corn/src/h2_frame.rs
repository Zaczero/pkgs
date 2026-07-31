use std::fmt;
use std::mem::size_of;
use std::num::NonZeroU32;

use bitflags::bitflags;
use bytes::{Buf as _, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt as _};
use zerocopy::byteorder::network_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{ErrorExt as _, H2CornError, H2Error};

pub(crate) const DEFAULT_HEADER_TABLE_SIZE: usize = 4096;
pub(crate) const DEFAULT_WINDOW_SIZE: u32 = 0xFFFF;
pub(crate) const DEFAULT_MAX_FRAME_SIZE: usize = 0x4000;
/// Initial (and shrink-target) capacity of the connection read buffer.
pub(crate) const READ_BUFFER_INITIAL_CAPACITY: usize = 4096;
pub(crate) const MAX_FRAME_SIZE_UPPER_BOUND: usize = 0x00FF_FFFF;
pub(crate) const MAX_FLOW_CONTROL_WINDOW: u32 = (1 << 31) - 1;
pub(crate) const CONNECTION_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
pub(crate) const STREAM_ID_MASK: u32 = 0x7FFF_FFFF;
pub(crate) const FRAME_HEADER_LEN: usize = 9;
pub(crate) const GOAWAY_FIXED_PAYLOAD_LEN: usize = 8;
pub(crate) const GOAWAY_FRAME_PREFIX_LEN: usize = FRAME_HEADER_LEN + GOAWAY_FIXED_PAYLOAD_LEN;
pub(crate) const SETTING_ENTRY_LEN: usize = size_of::<WireSetting>();
/// One wire entry per field of [`Settings`]. `Settings::wire_entries` is typed by
/// this *and* destructures `Settings`, so neither the bound nor a new field
/// can drift away from the wire encoding.
const MAX_SETTINGS_ENTRIES: usize = 7;
/// GOAWAY debug data is advisory, so it is truncated to keep the frame length
/// provably encodable instead of failing the connection teardown it explains.
const MAX_GOAWAY_DEBUG_LEN: usize = 1024;

const _: () = assert!(size_of::<WireSetting>() == size_of::<[u8; 6]>());

/// A frame-header rejection whose stream identity is available before any
/// payload is buffered. Keeping that identity lets the HTTP/2 driver apply a
/// stream-scoped FRAME_SIZE_ERROR where the frame type requires one.
#[derive(Debug)]
pub(crate) enum FrameReadError {
    Other(H2CornError),
    DeclaredPayloadLength {
        /// `Some` only when RFC 9113 scopes this violation to the stream —
        /// see [`DeclaredLengthError`].
        reset_stream: Option<StreamId>,
        error: H2Error,
    },
}

impl From<H2CornError> for FrameReadError {
    fn from(error: H2CornError) -> Self {
        Self::Other(error)
    }
}

/// The wire length of a frame payload.
///
/// The HTTP/2 length field is three bytes, so a longer payload would silently
/// truncate on the wire. Every construction is either a compile-time constant
/// or a narrowing of one, which is why the encoder below has no runtime check
/// and no panic path at all.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct FramePayloadLen(u32);

impl FramePayloadLen {
    pub(crate) const ZERO: Self = Self(0);

    /// Build from a runtime length, rejecting anything the wire cannot carry.
    pub(crate) const fn new(len: usize) -> Option<Self> {
        if len > MAX_FRAME_SIZE_UPPER_BOUND {
            return None;
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the bound above limits the value to 24 bits"
        )]
        Some(Self(len as u32))
    }

    /// Build from a constant. An over-long constant fails the build.
    pub(crate) const fn constant(len: usize) -> Self {
        assert!(
            len <= MAX_FRAME_SIZE_UPPER_BOUND,
            "frame payload constant exceeds the HTTP/2 length field"
        );
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the assertion above bounds the value to 24 bits"
        )]
        Self(len as u32)
    }

    /// Narrow to `other` when that is smaller. A valid length can only stay
    /// valid, so clamping chains need no further checks.
    pub(crate) const fn min_usize(self, other: usize) -> Self {
        if other < self.0 as usize {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "the comparison above bounds `other` by an already valid length"
            )]
            Self(other as u32)
        } else {
            self
        }
    }

    pub(crate) const fn get(self) -> u32 {
        self.0
    }

    pub(crate) const fn as_usize(self) -> usize {
        self.0 as usize
    }

    pub(crate) const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

/// A frame payload whose length is known to fit the wire length field.
///
/// The bytes and their encoded length are produced together, so they cannot
/// disagree, and there is no way to build one that is too long.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FramePayload<'a> {
    bytes: &'a [u8],
    len: FramePayloadLen,
}

impl<'a> FramePayload<'a> {
    pub(crate) const EMPTY: Self = Self {
        bytes: &[],
        len: FramePayloadLen::ZERO,
    };

    /// A payload whose size is fixed by its type.
    pub(crate) const fn fixed<const N: usize>(bytes: &'a [u8; N]) -> Self {
        Self {
            bytes,
            len: FramePayloadLen::constant(N),
        }
    }

    /// Split off at most `limit` bytes; the remainder feeds the next frame.
    pub(crate) const fn take(bytes: &'a [u8], limit: FramePayloadLen) -> (Self, &'a [u8]) {
        let len = limit.min_usize(bytes.len());
        let (payload, rest) = bytes.split_at(len.as_usize());
        (
            Self {
                bytes: payload,
                len,
            },
            rest,
        )
    }

    pub(crate) const fn len(self) -> FramePayloadLen {
        self.len
    }

    pub(crate) const fn as_bytes(self) -> &'a [u8] {
        self.bytes
    }

    pub(crate) const fn is_empty(self) -> bool {
        self.len.is_zero()
    }
}

/// A valid HTTP/2 stream identifier.
///
/// The wire field is 31 bits wide and zero denotes the connection, not a
/// stream. Keeping both constraints in the type prevents masked/raw integers
/// from entering stream-keyed state after frame parsing.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub(crate) struct StreamId(NonZeroU32);

impl StreamId {
    pub(crate) const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) if value.get() <= STREAM_ID_MASK => Some(Self(value)),
            Some(_) | None => None,
        }
    }

    pub(crate) const fn get(self) -> u32 {
        self.0.get()
    }

    pub(crate) const fn is_client_initiated(self) -> bool {
        self.get() & 1 != 0
    }
}

// `StreamId` hashes exactly as its wrapped integer. Opting into nohash-hasher
// is therefore sound and lets stream maps retain integer-key lookup cost.
impl nohash_hasher::IsEnabled for StreamId {}

/// A legal HTTP/2 WINDOW_UPDATE increment (RFC 9113: 1..=2^31-1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct WindowIncrement(NonZeroU32);

impl WindowIncrement {
    pub(crate) const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) if value.get() <= MAX_FLOW_CONTROL_WINDOW => Some(Self(value)),
            Some(_) | None => None,
        }
    }

    pub(crate) const fn get(self) -> u32 {
        self.0.get()
    }
}

const _: () = assert!(size_of::<StreamId>() == size_of::<u32>());
const _: () = assert!(size_of::<Option<StreamId>>() == size_of::<u32>());
const _: () = assert!(size_of::<WindowIncrement>() == size_of::<u32>());
const _: () = assert!(size_of::<Option<WindowIncrement>>() == size_of::<u32>());

bitflags! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    #[repr(transparent)]
    pub(crate) struct FrameFlags: u8 {
        // RFC 9113 reuses bit 0: ACK on SETTINGS/PING, END_STREAM on
        // HEADERS/DATA. Two names for one bit is the protocol's shape.
        const ACK = 0x01;
        const END_STREAM = 0x01;
        const END_HEADERS = 0x04;
        const PADDED = 0x08;
        const PRIORITY = 0x20;
    }
}

impl FrameFlags {
    pub(crate) const EMPTY: Self = Self::empty();

    /// Unknown flag bits are retained, not rejected: RFC 9113 §4.1 requires a
    /// receiver to ignore flags it does not define.
    pub(crate) const fn new(bits: u8) -> Self {
        Self::from_bits_retain(bits)
    }
}


#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct FrameType(u8);

impl FrameType {
    pub(crate) const CONTINUATION: Self = Self(0x09);
    pub(crate) const DATA: Self = Self(0x00);
    pub(crate) const GOAWAY: Self = Self(0x07);
    pub(crate) const HEADERS: Self = Self(0x01);
    pub(crate) const PING: Self = Self(0x06);
    pub(crate) const PRIORITY: Self = Self(0x02);
    pub(crate) const PUSH_PROMISE: Self = Self(0x05);
    pub(crate) const RST_STREAM: Self = Self(0x03);
    pub(crate) const SETTINGS: Self = Self(0x04);
    pub(crate) const WINDOW_UPDATE: Self = Self(0x08);

    pub(crate) const fn new(bits: u8) -> Self {
        Self(bits)
    }

    pub(crate) const fn bits(self) -> u8 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct ErrorCode(u32);

impl ErrorCode {
    pub(crate) const CANCEL: Self = Self(0x8);
    pub(crate) const COMPRESSION_ERROR: Self = Self(0x9);
    pub(crate) const ENHANCE_YOUR_CALM: Self = Self(0xB);
    pub(crate) const FLOW_CONTROL_ERROR: Self = Self(0x3);
    pub(crate) const FRAME_SIZE_ERROR: Self = Self(0x6);
    pub(crate) const INTERNAL_ERROR: Self = Self(0x2);
    pub(crate) const NO_ERROR: Self = Self(0x0);
    pub(crate) const PROTOCOL_ERROR: Self = Self(0x1);
    pub(crate) const REFUSED_STREAM: Self = Self(0x7);
    pub(crate) const STREAM_CLOSED: Self = Self(0x5);

    pub(crate) const fn new(value: u32) -> Self {
        Self(value)
    }

    pub(crate) const fn to_be_bytes(self) -> [u8; 4] {
        self.0.to_be_bytes()
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub(crate) struct SettingId(u16);

impl SettingId {
    pub(crate) const ENABLE_CONNECT_PROTOCOL: Self = Self(0x08);
    pub(crate) const ENABLE_PUSH: Self = Self(0x02);
    pub(crate) const HEADER_TABLE_SIZE: Self = Self(0x01);
    pub(crate) const INITIAL_WINDOW_SIZE: Self = Self(0x04);
    pub(crate) const MAX_CONCURRENT_STREAMS: Self = Self(0x03);
    pub(crate) const MAX_FRAME_SIZE: Self = Self(0x05);
    pub(crate) const MAX_HEADER_LIST_SIZE: Self = Self(0x06);

    pub(crate) const fn new(bits: u16) -> Self {
        Self(bits)
    }

    pub(crate) const fn bits(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrameHeader {
    pub frame_type: FrameType,
    pub flags: FrameFlags,
    pub stream_id: Option<StreamId>,
}

#[derive(Clone, Debug)]
pub(crate) struct RawFrame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

/// A frame read from the peer. Unhandled extension frames retain their header
/// for connection-order validation but never materialize their payload.
#[derive(Clone, Debug)]
pub(crate) enum FrameRead {
    Handled(RawFrame, HandledFrameKind),
    Ignored(FrameHeader),
}

/// The frames a connection dispatches, decided once at the wire boundary.
///
/// The reader materializes a payload only for these; RFC 9113 §4.1 has it
/// consume and ignore anything else as an extension frame. Carrying the
/// classification instead of re-deriving it downstream removes a two-list
/// arrangement that had to be edited in the right order: adding a dispatch arm
/// without the reader's predicate silently discarded the frame before dispatch,
/// and adding the predicate without the arm routed peer input into a panic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HandledFrameKind {
    Continuation,
    Data,
    Goaway,
    Headers,
    Ping,
    Priority,
    PushPromise,
    RstStream,
    Settings,
    WindowUpdate,
}

impl HandledFrameKind {
    pub(crate) const fn classify(frame_type: FrameType) -> Option<Self> {
        match frame_type {
            FrameType::CONTINUATION => Some(Self::Continuation),
            FrameType::DATA => Some(Self::Data),
            FrameType::GOAWAY => Some(Self::Goaway),
            FrameType::HEADERS => Some(Self::Headers),
            FrameType::PING => Some(Self::Ping),
            FrameType::PRIORITY => Some(Self::Priority),
            FrameType::PUSH_PROMISE => Some(Self::PushPromise),
            FrameType::RST_STREAM => Some(Self::RstStream),
            FrameType::SETTINGS => Some(Self::Settings),
            FrameType::WINDOW_UPDATE => Some(Self::WindowUpdate),
            _ => None,
        }
    }
}

#[derive(FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned)]
#[repr(C)]
struct WireSetting {
    id: U16,
    value: U32,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Settings {
    pub header_table_size: Option<u32>,
    pub enable_push: Option<bool>,
    pub max_concurrent_streams: Option<u32>,
    pub initial_window_size: Option<u32>,
    pub max_frame_size: Option<NonZeroU32>,
    pub max_header_list_size: Option<u32>,
    pub enable_connect_protocol: Option<bool>,
}

impl Settings {
    /// Every field in its wire form, present or not.
    ///
    /// The return type is sized by [`MAX_SETTINGS_ENTRIES`], which is what
    /// bounds the declared payload length in `append_settings`. Adding a field
    /// without growing that bound is therefore a compile error, where before
    /// it produced a frame declaring fewer bytes than it went on to write —
    /// the surplus reads as the header of a frame the peer never sent.
    fn wire_entries(self) -> [(SettingId, Option<u32>); MAX_SETTINGS_ENTRIES] {
        // Destructured rather than read field by field: a new field fails to
        // bind here, so it cannot be added without deciding how it reaches the
        // wire. Reading `self.x` would compile happily and silently never emit
        // it — the array length alone only catches the *bound*, not the field.
        let Self {
            header_table_size,
            enable_push,
            max_concurrent_streams,
            initial_window_size,
            max_frame_size,
            max_header_list_size,
            enable_connect_protocol,
        } = self;
        [
            (SettingId::HEADER_TABLE_SIZE, header_table_size),
            (SettingId::ENABLE_PUSH, enable_push.map(u32::from)),
            (SettingId::MAX_CONCURRENT_STREAMS, max_concurrent_streams),
            (SettingId::INITIAL_WINDOW_SIZE, initial_window_size),
            (
                SettingId::MAX_FRAME_SIZE,
                max_frame_size.map(NonZeroU32::get),
            ),
            (SettingId::MAX_HEADER_LIST_SIZE, max_header_list_size),
            (
                SettingId::ENABLE_CONNECT_PROTOCOL,
                enable_connect_protocol.map(u32::from),
            ),
        ]
    }

    pub(crate) fn iter(self) -> impl Iterator<Item = (SettingId, u32)> {
        self.wire_entries()
            .into_iter()
            .filter_map(|(id, value)| value.map(|value| (id, value)))
    }

    pub(crate) fn apply_wire_pair(&mut self, id: SettingId, value: u32) -> Result<(), H2CornError> {
        match id {
            SettingId::HEADER_TABLE_SIZE => self.header_table_size = Some(value),
            SettingId::ENABLE_PUSH => {
                if value > 1 {
                    return H2Error::SettingsEnablePushInvalid.err();
                }
                self.enable_push = Some(value != 0);
            },
            SettingId::MAX_CONCURRENT_STREAMS => self.max_concurrent_streams = Some(value),
            SettingId::INITIAL_WINDOW_SIZE => self.initial_window_size = Some(value),
            SettingId::MAX_FRAME_SIZE => {
                self.max_frame_size =
                    Some(NonZeroU32::new(value).ok_or(H2Error::SettingsMaxFrameSizeInvalid)?);
            },
            SettingId::MAX_HEADER_LIST_SIZE => self.max_header_list_size = Some(value),
            SettingId::ENABLE_CONNECT_PROTOCOL => {
                if value > 1 {
                    return H2Error::SettingsEnableConnectProtocolInvalid.err();
                }
                self.enable_connect_protocol = Some(value != 0);
            },
            _ => {},
        }
        Ok(())
    }
}

/// Peer settings after validation.
///
/// `max_frame_size` is already a [`FramePayloadLen`]: the range check happens
/// once here, at the wire boundary, so nothing downstream re-clamps it.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PeerSettings {
    pub header_table_size: Option<usize>,
    pub initial_window_size: Option<u32>,
    pub max_frame_size: Option<FramePayloadLen>,
}

impl TryFrom<Settings> for PeerSettings {
    type Error = H2CornError;

    fn try_from(settings: Settings) -> Result<Self, Self::Error> {
        if let Some(value) = settings.initial_window_size
            && value > MAX_FLOW_CONTROL_WINDOW
        {
            return H2Error::SettingsInitialWindowSizeExceededLimit.err();
        }

        let max_frame_size = settings
            .max_frame_size
            .map(|size| {
                FramePayloadLen::new(size.get() as usize)
                    .filter(|size| size.as_usize() >= DEFAULT_MAX_FRAME_SIZE)
                    .ok_or(H2Error::SettingsMaxFrameSizeOutOfRange)
            })
            .transpose()?;

        Ok(Self {
            header_table_size: settings.header_table_size.map(|value| value as usize),
            initial_window_size: settings.initial_window_size,
            max_frame_size,
        })
    }
}

#[derive(Debug)]
pub(crate) struct BufferedConnectionReader<R> {
    reader: R,
    buffer: BytesMut,
    /// A fixed-size control frame can be rejected from its nine-byte header.
    /// Its payload still occupies the wire and must be skipped before the
    /// next header is decoded, otherwise one stream error becomes an infinite
    /// loop over the same frame.
    discard_payload_len: usize,
}

impl<R> BufferedConnectionReader<R>
where
    R: AsyncRead + Unpin,
{
    pub(crate) fn new(reader: R) -> Self {
        // Small initial capacity; `read_at_least`/`read_more` reserve on
        // demand, so large frames grow the buffer only when they occur.
        Self {
            reader,
            buffer: BytesMut::with_capacity(READ_BUFFER_INITIAL_CAPACITY),
            discard_payload_len: 0,
        }
    }

    pub(crate) const fn with_buffer(reader: R, buffer: BytesMut) -> Self {
        Self {
            reader,
            buffer,
            discard_payload_len: 0,
        }
    }

    pub(crate) async fn read_at_least(&mut self, len: usize) -> Result<bool, H2CornError> {
        while self.buffer.len() < len {
            self.buffer.reserve(len - self.buffer.len());

            let read = self.reader.read_buf(&mut self.buffer).await?;
            if read == 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) async fn read_more_capped(&mut self, max_bytes: usize) -> Result<bool, H2CornError> {
        debug_assert!(max_bytes != 0);
        self.buffer.reserve(max_bytes);
        let mut limited = (&mut self.reader).take(max_bytes as u64);
        let read = limited.read_buf(&mut self.buffer).await?;
        Ok(read != 0)
    }

    pub(crate) fn buffered(&self) -> &[u8] {
        self.buffer.as_ref()
    }

    pub(crate) fn consume(&mut self, len: usize) {
        self.buffer.advance(len);
    }

    pub(crate) fn into_parts(self) -> (R, BytesMut) {
        (self.reader, self.buffer)
    }

    pub(crate) async fn read_frame(
        &mut self,
        max_frame_size: usize,
    ) -> Result<Option<FrameRead>, FrameReadError> {
        self.discard_rejected_payload().await?;
        if !self.read_at_least(FRAME_HEADER_LEN).await? {
            if self.buffer.is_empty() {
                return Ok(None);
            }
            return Err(H2CornError::from(H2Error::FrameHeaderClosed).into());
        }

        let Some(header) = self.buffer.as_ref().first_chunk::<FRAME_HEADER_LEN>() else {
            unreachable!("frame header is buffered")
        };
        let payload_len = decode_u24([header[0], header[1], header[2]]);
        let stream_id = StreamId::new(
            u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & STREAM_ID_MASK,
        );
        if payload_len > max_frame_size {
            return Err(H2CornError::from(H2Error::frame_length_exceeds_peer_max(
                payload_len,
                max_frame_size,
            ))
            .into());
        }

        let frame_type = FrameType::new(header[3]);
        let flags = FrameFlags::new(header[4]);
        if let Err(violation) = validate_declared_payload_length(frame_type, flags, payload_len) {
            self.buffer.advance(FRAME_HEADER_LEN);
            self.discard_payload_len = payload_len;
            let (reset_stream, error) = match violation {
                DeclaredLengthError::Connection(error) => (None, error),
                DeclaredLengthError::Stream(error) => (stream_id, error),
            };
            return Err(FrameReadError::DeclaredPayloadLength {
                reset_stream,
                error,
            });
        }

        let Some(kind) = HandledFrameKind::classify(frame_type) else {
            // RFC 9113 permits extension frames that this connection does
            // not implement. Their payload is inert here, so consume it
            // through the capped discard path instead of growing the
            // connection buffer just to create a RawFrame the dispatcher
            // would immediately drop.
            self.buffer.advance(FRAME_HEADER_LEN);
            self.discard_payload_len = payload_len;
            self.discard_rejected_payload().await?;
            return Ok(Some(FrameRead::Ignored(FrameHeader {
                frame_type,
                flags,
                stream_id,
            })));
        };

        let total_len = FRAME_HEADER_LEN + payload_len;
        if !self.read_at_least(total_len).await? {
            return Err(H2CornError::from(H2Error::FramePayloadClosed).into());
        }

        let Some(header) = self.buffer.as_ref().first_chunk::<FRAME_HEADER_LEN>() else {
            unreachable!("frame header is buffered")
        };
        let stream_id = StreamId::new(
            u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & STREAM_ID_MASK,
        );
        self.buffer.advance(FRAME_HEADER_LEN);
        let payload = self.buffer.split_to(payload_len).freeze();

        Ok(Some(FrameRead::Handled(
            RawFrame {
                header: FrameHeader {
                    frame_type,
                    flags,
                    stream_id,
                },
                payload,
            },
            kind,
        )))
    }

    async fn discard_rejected_payload(&mut self) -> Result<(), H2CornError> {
        while self.discard_payload_len != 0 {
            if self.buffer.is_empty()
                && !self
                    .read_more_capped(self.discard_payload_len.min(READ_BUFFER_INITIAL_CAPACITY))
                    .await?
            {
                return H2Error::FramePayloadClosed.err();
            }
            let consumed = self.buffer.len().min(self.discard_payload_len);
            self.buffer.advance(consumed);
            self.discard_payload_len -= consumed;
        }
        Ok(())
    }
}

/// A fixed-length frame rejected from its nine-byte header.
///
/// RFC 9113 scopes these violations by **frame type, not by stream**. Reading
/// the scope off the frame's stream id instead let a malformed PING, SETTINGS,
/// RST_STREAM, WINDOW_UPDATE or GOAWAY sent on a live stream be answered with
/// RST_STREAM while the connection stayed open and out of step.
#[derive(Debug)]
pub(crate) enum DeclaredLengthError {
    /// RFC 9113 §§6.4, 6.5, 6.7, 6.8, 6.9 — the connection ends.
    Connection(H2Error),
    /// RFC 9113 §6.3 — PRIORITY alone survives as a stream error.
    Stream(H2Error),
}

/// Reject fixed-size control frames from their nine-byte header, before a
/// hostile declared length can grow the connection buffer.
pub(crate) fn validate_declared_payload_length(
    frame_type: FrameType,
    flags: FrameFlags,
    payload_len: usize,
) -> Result<(), DeclaredLengthError> {
    let exact = |expected, error| {
        (payload_len == expected)
            .then_some(())
            .ok_or(DeclaredLengthError::Connection(error))
    };
    match frame_type {
        FrameType::PING => exact(8, H2Error::PingPayloadInvalidLength),
        FrameType::RST_STREAM => exact(4, H2Error::RstStreamPayloadInvalidLength),
        FrameType::WINDOW_UPDATE => exact(4, H2Error::WindowUpdatePayloadInvalidLength),
        FrameType::PRIORITY => (payload_len == 5)
            .then_some(())
            .ok_or(DeclaredLengthError::Stream(
                H2Error::PriorityPayloadInvalidLength,
            )),
        FrameType::SETTINGS if flags.contains(FrameFlags::ACK) => {
            exact(0, H2Error::SettingsAckPayloadNotEmpty)
        },
        FrameType::SETTINGS => (payload_len.is_multiple_of(SETTING_ENTRY_LEN))
            .then_some(())
            .ok_or(DeclaredLengthError::Connection(
                H2Error::SettingsPayloadLengthInvalid,
            )),
        FrameType::GOAWAY => (payload_len >= GOAWAY_FIXED_PAYLOAD_LEN)
            .then_some(())
            .ok_or(DeclaredLengthError::Connection(H2Error::InvalidGoawayFrame)),
        _ => Ok(()),
    }
}

const fn decode_u24(bytes: [u8; 3]) -> usize {
    let [b0, b1, b2] = bytes;
    ((b0 as usize) << 16) | ((b1 as usize) << 8) | (b2 as usize)
}

pub(crate) fn encode_frame_header(header: FrameHeader, payload_len: FramePayloadLen) -> [u8; 9] {
    let len = payload_len.get().to_be_bytes();
    let stream_id = header.stream_id.map_or(0, StreamId::get).to_be_bytes();
    [
        len[1],
        len[2],
        len[3],
        header.frame_type.bits(),
        header.flags.bits(),
        stream_id[0],
        stream_id[1],
        stream_id[2],
        stream_id[3],
    ]
}

/// Append a frame whose payload has a fixed size, so its length is validated
/// at compile time.
pub(crate) fn append_frame<const N: usize>(
    dst: &mut BytesMut,
    header: FrameHeader,
    payload: &[u8; N],
) {
    dst.reserve(FRAME_HEADER_LEN + N);
    dst.extend_from_slice(&encode_frame_header(
        header,
        const { FramePayloadLen::constant(N) },
    ));
    dst.extend_from_slice(payload);
}

pub(crate) fn append_settings(dst: &mut BytesMut, settings: Settings) {
    // One entry per field of `Settings`, so the whole payload is bounded by a
    // constant and the narrowing below carries that proof.
    let payload_len = const { FramePayloadLen::constant(MAX_SETTINGS_ENTRIES * SETTING_ENTRY_LEN) }
        .min_usize(settings.iter().count() * SETTING_ENTRY_LEN);
    dst.reserve(FRAME_HEADER_LEN + payload_len.as_usize());
    dst.extend_from_slice(&encode_frame_header(
        FrameHeader {
            frame_type: FrameType::SETTINGS,
            flags: FrameFlags::EMPTY,
            stream_id: None,
        },
        payload_len,
    ));
    for (id, value) in settings.iter() {
        let entry = WireSetting {
            id: U16::new(id.bits()),
            value: U32::new(value),
        };
        dst.extend_from_slice(entry.as_bytes());
    }
}

pub(crate) fn append_settings_ack(dst: &mut BytesMut) {
    append_frame(
        dst,
        FrameHeader {
            frame_type: FrameType::SETTINGS,
            flags: FrameFlags::ACK,
            stream_id: None,
        },
        &[],
    );
}

pub(crate) fn append_window_update(
    dst: &mut BytesMut,
    stream_id: Option<StreamId>,
    increment: WindowIncrement,
) {
    append_frame(
        dst,
        FrameHeader {
            frame_type: FrameType::WINDOW_UPDATE,
            flags: FrameFlags::EMPTY,
            stream_id,
        },
        &increment.get().to_be_bytes(),
    );
}

pub(crate) fn append_ping_ack(dst: &mut BytesMut, payload: [u8; 8]) {
    append_frame(
        dst,
        FrameHeader {
            frame_type: FrameType::PING,
            flags: FrameFlags::ACK,
            stream_id: None,
        },
        &payload,
    );
}

pub(crate) fn append_rst_stream(dst: &mut BytesMut, stream_id: StreamId, error_code: ErrorCode) {
    append_frame(
        dst,
        FrameHeader {
            frame_type: FrameType::RST_STREAM,
            flags: FrameFlags::EMPTY,
            stream_id: Some(stream_id),
        },
        &error_code.to_be_bytes(),
    );
}

pub(crate) fn append_goaway(
    dst: &mut BytesMut,
    last_stream_id: Option<StreamId>,
    error_code: ErrorCode,
    debug: &[u8],
) {
    let debug = &debug[..debug.len().min(MAX_GOAWAY_DEBUG_LEN)];
    let payload_len =
        const { FramePayloadLen::constant(GOAWAY_FIXED_PAYLOAD_LEN + MAX_GOAWAY_DEBUG_LEN) }
            .min_usize(GOAWAY_FIXED_PAYLOAD_LEN + debug.len());
    dst.reserve(GOAWAY_FRAME_PREFIX_LEN + debug.len());
    dst.extend_from_slice(&encode_frame_header(
        FrameHeader {
            frame_type: FrameType::GOAWAY,
            flags: FrameFlags::EMPTY,
            stream_id: None,
        },
        payload_len,
    ));
    dst.extend_from_slice(&last_stream_id.map_or(0, StreamId::get).to_be_bytes());
    dst.extend_from_slice(&error_code.to_be_bytes());
    dst.extend_from_slice(debug);
}

pub(crate) fn parse_settings_payload(payload: &[u8]) -> Result<PeerSettings, H2CornError> {
    // `WireSetting` is `Unaligned + FromBytes`, so this is a zero-copy view;
    // it fails exactly when the payload is not a whole number of entries.
    let Ok(entries) = <[WireSetting]>::ref_from_bytes(payload) else {
        return H2Error::SettingsPayloadLengthInvalid.err();
    };
    let mut settings = Settings::default();
    for entry in entries {
        settings.apply_wire_pair(SettingId::new(entry.id.get()), entry.value.get())?;
    }
    PeerSettings::try_from(settings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_wire_settings(bytes: &[u8]) -> Vec<(u16, u32)> {
        bytes
            .as_chunks::<SETTING_ENTRY_LEN>()
            .0
            .iter()
            .map(|entry| {
                (
                    u16::from_be_bytes([entry[0], entry[1]]),
                    u32::from_be_bytes([entry[2], entry[3], entry[4], entry[5]]),
                )
            })
            .collect()
    }

    #[test]
    fn stream_id_rejects_connection_and_reserved_bit_values() {
        assert_eq!(StreamId::new(0), None);
        assert_eq!(StreamId::new(STREAM_ID_MASK + 1), None);
        assert_eq!(StreamId::new(u32::MAX), None);
        assert_eq!(StreamId::new(STREAM_ID_MASK).unwrap().get(), STREAM_ID_MASK);
        assert!(StreamId::new(1).unwrap().is_client_initiated());
        assert!(!StreamId::new(2).unwrap().is_client_initiated());
    }

    #[test]
    fn window_increment_rejects_zero_and_reserved_bit_values() {
        assert_eq!(WindowIncrement::new(0), None);
        assert_eq!(
            WindowIncrement::new(MAX_FLOW_CONTROL_WINDOW).unwrap().get(),
            MAX_FLOW_CONTROL_WINDOW
        );
        assert_eq!(WindowIncrement::new(MAX_FLOW_CONTROL_WINDOW + 1), None);
        assert_eq!(WindowIncrement::new(u32::MAX), None);
    }

    #[test]
    fn settings_zero_values_round_trip() {
        let settings = Settings {
            header_table_size: Some(0),
            enable_push: Some(false),
            max_concurrent_streams: Some(0),
            initial_window_size: Some(0),
            max_frame_size: Some(NonZeroU32::new(DEFAULT_MAX_FRAME_SIZE as u32).unwrap()),
            max_header_list_size: Some(0),
            enable_connect_protocol: Some(true),
        };
        let mut frame = BytesMut::new();
        append_settings(&mut frame, settings);

        let parsed = parse_settings_payload(&frame[FRAME_HEADER_LEN..]).unwrap();
        assert_eq!(parsed.header_table_size, Some(0));
        assert_eq!(parsed.initial_window_size, Some(0));
    }

    #[test]
    fn append_settings_preserves_full_u32_values() {
        let settings = Settings {
            header_table_size: Some(u32::MAX),
            enable_push: None,
            max_concurrent_streams: Some(u32::MAX),
            initial_window_size: None,
            max_frame_size: None,
            max_header_list_size: Some(u32::MAX),
            enable_connect_protocol: None,
        };
        let mut frame = BytesMut::new();
        append_settings(&mut frame, settings);

        assert_eq!(parse_wire_settings(&frame[FRAME_HEADER_LEN..]), vec![
            (SettingId::HEADER_TABLE_SIZE.bits(), u32::MAX),
            (SettingId::MAX_CONCURRENT_STREAMS.bits(), u32::MAX),
            (SettingId::MAX_HEADER_LIST_SIZE.bits(), u32::MAX),
        ],);
    }

    #[test]
    fn encode_frame_header_uses_explicit_payload_length() {
        let encoded = encode_frame_header(
            FrameHeader {
                frame_type: FrameType::DATA,
                flags: FrameFlags::EMPTY,
                stream_id: Some(StreamId::new(1).unwrap()),
            },
            FramePayloadLen::constant(0x4000),
        );
        assert_eq!(&encoded[..3], &[0x00, 0x40, 0x00]);
    }

    #[test]
    fn append_goaway_writes_unknown_error_code() {
        let mut frame = BytesMut::new();
        append_goaway(&mut frame, None, ErrorCode::new(0xFEED_BEEF), b"debug");
        assert_eq!(&frame[13..17], &0xFEED_BEEF_u32.to_be_bytes());
    }

    #[test]
    fn settings_wire_pair_rejects_invalid_zero_max_frame_size() {
        let mut settings = Settings::default();
        let err = settings
            .apply_wire_pair(SettingId::MAX_FRAME_SIZE, 0)
            .unwrap_err();
        assert_eq!(err.to_string(), "invalid SETTINGS_MAX_FRAME_SIZE value");
    }

    #[test]
    fn fixed_control_lengths_fail_from_the_nine_byte_header() {
        for (frame_type, valid, invalid) in [
            (FrameType::PING, 8, [7, 9]),
            (FrameType::RST_STREAM, 4, [3, 5]),
            (FrameType::WINDOW_UPDATE, 4, [3, 5]),
            (FrameType::PRIORITY, 5, [4, 6]),
        ] {
            validate_declared_payload_length(frame_type, FrameFlags::EMPTY, valid).unwrap();
            for len in invalid {
                assert!(
                    validate_declared_payload_length(frame_type, FrameFlags::EMPTY, len).is_err()
                );
            }
        }
        validate_declared_payload_length(FrameType::SETTINGS, FrameFlags::ACK, 0).unwrap();
        assert!(validate_declared_payload_length(FrameType::SETTINGS, FrameFlags::ACK, 1).is_err());
        validate_declared_payload_length(FrameType::SETTINGS, FrameFlags::EMPTY, 6).unwrap();
        assert!(
            validate_declared_payload_length(FrameType::SETTINGS, FrameFlags::EMPTY, 5).is_err()
        );
        validate_declared_payload_length(FrameType::GOAWAY, FrameFlags::EMPTY, 8).unwrap();
        assert!(validate_declared_payload_length(FrameType::GOAWAY, FrameFlags::EMPTY, 7).is_err());
    }

    #[tokio::test]
    async fn malformed_control_length_fails_without_waiting_for_its_payload() {
        use tokio::io::{AsyncWriteExt as _, duplex};
        use tokio::time::{Duration, timeout};

        let (mut client, server) = duplex(FRAME_HEADER_LEN);
        client
            .write_all(&[
                0,
                0,
                9, // invalid PING length, but below the peer frame limit
                FrameType::PING.bits(),
                FrameFlags::EMPTY.bits(),
                0,
                0,
                0,
                0,
            ])
            .await
            .unwrap();
        let mut reader = BufferedConnectionReader::new(server);

        let result = timeout(
            Duration::from_millis(100),
            reader.read_frame(DEFAULT_MAX_FRAME_SIZE),
        )
        .await
        .expect("the nine-byte header must reject before the payload is read");
        let error = result.expect_err("the invalid PING header must be rejected");
        assert!(matches!(error, FrameReadError::DeclaredPayloadLength {
            error: H2Error::PingPayloadInvalidLength,
            ..
        }));
    }

    #[tokio::test]
    async fn malformed_control_length_discards_its_payload_before_the_next_header() {
        use tokio::io::{AsyncWriteExt as _, duplex};

        let (mut client, server) = duplex(64);
        client
            .write_all(&[
                0,
                0,
                4, // PRIORITY must have a five-byte payload.
                FrameType::PRIORITY.bits(),
                FrameFlags::EMPTY.bits(),
                0,
                0,
                0,
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                8,
                FrameType::PING.bits(),
                FrameFlags::EMPTY.bits(),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ])
            .await
            .unwrap();
        let mut reader = BufferedConnectionReader::new(server);

        let error = reader
            .read_frame(DEFAULT_MAX_FRAME_SIZE)
            .await
            .expect_err("the short PRIORITY is rejected");
        assert!(matches!(error, FrameReadError::DeclaredPayloadLength {
            reset_stream: Some(stream_id),
            error: H2Error::PriorityPayloadInvalidLength,
        } if stream_id.get() == 1));

        let frame = reader
            .read_frame(DEFAULT_MAX_FRAME_SIZE)
            .await
            .expect("the rejected payload is skipped")
            .expect("the following PING is present");
        let FrameRead::Handled(frame, _) = frame else {
            panic!("PING belongs to the handled frame vocabulary")
        };
        assert_eq!(frame.header.frame_type, FrameType::PING);
        assert_eq!(frame.payload.as_ref(), &[0; 8]);
    }

    #[tokio::test]
    async fn unknown_frame_payload_is_discarded_in_capped_fragments() {
        use tokio::io::{AsyncWriteExt as _, duplex};

        const UNKNOWN_FRAME: FrameType = FrameType::new(0xF0);
        const PAYLOAD_LEN: usize = READ_BUFFER_INITIAL_CAPACITY * 4;

        let (mut client, server) = duplex(READ_BUFFER_INITIAL_CAPACITY);
        let writer = tokio::spawn(async move {
            client
                .write_all(&encode_frame_header(
                    FrameHeader {
                        frame_type: UNKNOWN_FRAME,
                        flags: FrameFlags::EMPTY,
                        stream_id: None,
                    },
                    FramePayloadLen::constant(PAYLOAD_LEN),
                ))
                .await
                .expect("unknown frame header is written");
            for _ in 0..4 {
                client
                    .write_all(&[0xA5; READ_BUFFER_INITIAL_CAPACITY])
                    .await
                    .expect("one payload fragment is written");
            }
            client
                .write_all(&[
                    0,
                    0,
                    8,
                    FrameType::PING.bits(),
                    FrameFlags::EMPTY.bits(),
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ])
                .await
                .expect("following PING is written");
        });

        let mut reader = BufferedConnectionReader::new(server);
        let ignored = reader
            .read_frame(DEFAULT_MAX_FRAME_SIZE)
            .await
            .expect("unknown payload is consumed")
            .expect("unknown frame header is returned after discard");
        assert!(matches!(
            ignored,
            FrameRead::Ignored(FrameHeader { frame_type, .. }) if frame_type == UNKNOWN_FRAME
        ));
        let frame = reader
            .read_frame(DEFAULT_MAX_FRAME_SIZE)
            .await
            .expect("following PING is readable")
            .expect("following PING is returned");
        writer.await.expect("writer task joins");

        let FrameRead::Handled(frame, _) = frame else {
            panic!("PING belongs to the handled frame vocabulary")
        };
        assert_eq!(frame.header.frame_type, FrameType::PING);
        assert_eq!(frame.payload.as_ref(), &[0; 8]);
        let (_, buffer) = reader.into_parts();
        assert!(buffer.is_empty());
        assert!(
            buffer.capacity() <= READ_BUFFER_INITIAL_CAPACITY * 2,
            "discarding must retain only the capped reader buffer, not the unknown payload"
        );
    }

    #[tokio::test]
    async fn eof_while_discarding_unknown_frame_payload_is_reported() {
        use tokio::io::{AsyncWriteExt as _, duplex};

        let (mut client, server) = duplex(64);
        client
            .write_all(&encode_frame_header(
                FrameHeader {
                    frame_type: FrameType::new(0xF0),
                    flags: FrameFlags::EMPTY,
                    stream_id: None,
                },
                FramePayloadLen::constant(128),
            ))
            .await
            .expect("unknown frame header is written");
        client
            .write_all(&[0xA5; 17])
            .await
            .expect("partial payload is written");
        drop(client);

        let mut reader = BufferedConnectionReader::new(server);
        let error = reader
            .read_frame(DEFAULT_MAX_FRAME_SIZE)
            .await
            .expect_err("EOF in discarded payload is a framing error");
        assert!(matches!(
            error,
            FrameReadError::Other(error)
                if matches!(error.kind(), crate::error::ErrorKind::H2(H2Error::FramePayloadClosed))
        ));
    }
}
