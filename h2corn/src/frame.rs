use std::mem::size_of;
use std::num::NonZeroU32;
use std::ops::{BitOr, BitOrAssign};
use std::{fmt, slice};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use zerocopy::byteorder::network_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{ErrorExt, H2CornError, H2Error};

pub const DEFAULT_HEADER_TABLE_SIZE: usize = 4096;
pub const DEFAULT_WINDOW_SIZE: u32 = 0xFFFF;
pub const DEFAULT_MAX_FRAME_SIZE: usize = 0x4000;
pub const MAX_FRAME_SIZE_UPPER_BOUND: usize = 0x00FF_FFFF;
pub const MAX_FLOW_CONTROL_WINDOW: u32 = (1 << 31) - 1;
pub const CONNECTION_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
pub const STREAM_ID_MASK: u32 = 0x7FFF_FFFF;
pub const FRAME_HEADER_LEN: usize = 9;
pub const GOAWAY_FIXED_PAYLOAD_LEN: usize = 8;
pub const GOAWAY_FRAME_PREFIX_LEN: usize = FRAME_HEADER_LEN + GOAWAY_FIXED_PAYLOAD_LEN;
pub const SETTING_ENTRY_LEN: usize = size_of::<WireSetting>();

const _: () = assert!(size_of::<WireSetting>() == size_of::<[u8; 6]>());

pub type StreamId = NonZeroU32;
pub type WindowIncrement = NonZeroU32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct FrameFlags(u8);

impl FrameFlags {
    pub const EMPTY: Self = Self(0);
    pub const ACK: Self = Self(0x01);
    pub const END_STREAM: Self = Self(0x01);
    pub const END_HEADERS: Self = Self(0x04);
    pub const PADDED: Self = Self(0x08);
    pub const PRIORITY: Self = Self(0x20);

    pub const fn new(bits: u8) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl BitOr for FrameFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for FrameFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct FrameType(u8);

impl FrameType {
    pub const DATA: Self = Self(0x00);
    pub const HEADERS: Self = Self(0x01);
    pub const PRIORITY: Self = Self(0x02);
    pub const RST_STREAM: Self = Self(0x03);
    pub const SETTINGS: Self = Self(0x04);
    pub const PUSH_PROMISE: Self = Self(0x05);
    pub const PING: Self = Self(0x06);
    pub const GOAWAY: Self = Self(0x07);
    pub const WINDOW_UPDATE: Self = Self(0x08);
    pub const CONTINUATION: Self = Self(0x09);

    pub const fn new(bits: u8) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct ErrorCode(u32);

impl ErrorCode {
    pub const NO_ERROR: Self = Self(0x0);
    pub const PROTOCOL_ERROR: Self = Self(0x1);
    pub const INTERNAL_ERROR: Self = Self(0x2);
    pub const FLOW_CONTROL_ERROR: Self = Self(0x3);
    pub const STREAM_CLOSED: Self = Self(0x5);
    pub const FRAME_SIZE_ERROR: Self = Self(0x6);
    pub const REFUSED_STREAM: Self = Self(0x7);
    pub const CANCEL: Self = Self(0x8);
    pub const COMPRESSION_ERROR: Self = Self(0x9);

    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn to_be_bytes(self) -> [u8; 4] {
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
pub struct SettingId(u16);

impl SettingId {
    pub const HEADER_TABLE_SIZE: Self = Self(0x01);
    pub const ENABLE_PUSH: Self = Self(0x02);
    pub const MAX_CONCURRENT_STREAMS: Self = Self(0x03);
    pub const INITIAL_WINDOW_SIZE: Self = Self(0x04);
    pub const MAX_FRAME_SIZE: Self = Self(0x05);
    pub const MAX_HEADER_LIST_SIZE: Self = Self(0x06);
    pub const ENABLE_CONNECT_PROTOCOL: Self = Self(0x08);

    pub const fn new(bits: u16) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    pub len: usize,
    pub frame_type: FrameType,
    pub flags: FrameFlags,
    pub stream_id: Option<StreamId>,
}

#[derive(Clone, Debug)]
pub struct RawFrame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

#[derive(FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned)]
#[repr(C)]
struct WireSetting {
    id: U16,
    value: U32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Settings {
    pub header_table_size: Option<u32>,
    pub enable_push: Option<bool>,
    pub max_concurrent_streams: Option<u32>,
    pub initial_window_size: Option<u32>,
    pub max_frame_size: Option<NonZeroU32>,
    pub max_header_list_size: Option<u32>,
    pub enable_connect_protocol: Option<bool>,
}

impl Settings {
    pub fn iter(self) -> impl Iterator<Item = (SettingId, u32)> {
        [
            (SettingId::HEADER_TABLE_SIZE, self.header_table_size),
            (SettingId::ENABLE_PUSH, self.enable_push.map(u32::from)),
            (
                SettingId::MAX_CONCURRENT_STREAMS,
                self.max_concurrent_streams,
            ),
            (SettingId::INITIAL_WINDOW_SIZE, self.initial_window_size),
            (
                SettingId::MAX_FRAME_SIZE,
                self.max_frame_size.map(NonZeroU32::get),
            ),
            (SettingId::MAX_HEADER_LIST_SIZE, self.max_header_list_size),
            (
                SettingId::ENABLE_CONNECT_PROTOCOL,
                self.enable_connect_protocol.map(u32::from),
            ),
        ]
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

#[derive(Clone, Copy, Debug, Default)]
pub struct PeerSettings {
    pub header_table_size: Option<usize>,
    pub initial_window_size: Option<u32>,
    pub max_frame_size: Option<NonZeroU32>,
}

impl TryFrom<Settings> for PeerSettings {
    type Error = H2CornError;

    fn try_from(settings: Settings) -> Result<Self, Self::Error> {
        if let Some(value) = settings.initial_window_size
            && value > MAX_FLOW_CONTROL_WINDOW
        {
            return H2Error::SettingsInitialWindowSizeExceededLimit.err();
        }

        if let Some(size) = settings.max_frame_size
            && !(DEFAULT_MAX_FRAME_SIZE..=MAX_FRAME_SIZE_UPPER_BOUND)
                .contains(&(size.get() as usize))
        {
            return H2Error::SettingsMaxFrameSizeOutOfRange.err();
        }

        Ok(Self {
            header_table_size: settings.header_table_size.map(|value| value as usize),
            initial_window_size: settings.initial_window_size,
            max_frame_size: settings.max_frame_size,
        })
    }
}

#[derive(Debug)]
pub struct FrameReader<R> {
    reader: R,
    buffer: BytesMut,
}

impl<R> FrameReader<R>
where
    R: AsyncRead + Unpin,
{
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: BytesMut::with_capacity(DEFAULT_MAX_FRAME_SIZE + FRAME_HEADER_LEN),
        }
    }

    pub(crate) const fn with_buffer(reader: R, buffer: BytesMut) -> Self {
        Self { reader, buffer }
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
    ) -> Result<Option<RawFrame>, H2CornError> {
        if !self.read_at_least(FRAME_HEADER_LEN).await? {
            if self.buffer.is_empty() {
                return Ok(None);
            }
            return H2Error::FrameHeaderClosed.err();
        }

        let Some(header) = self.buffer.as_ref().first_chunk::<FRAME_HEADER_LEN>() else {
            unreachable!("frame header is buffered")
        };
        let payload_len = decode_u24([header[0], header[1], header[2]]);
        if payload_len > max_frame_size {
            return H2Error::frame_length_exceeds_peer_max(payload_len, max_frame_size).err();
        }

        let total_len = FRAME_HEADER_LEN + payload_len;
        if !self.read_at_least(total_len).await? {
            return H2Error::FramePayloadClosed.err();
        }

        let Some(header) = self.buffer.as_ref().first_chunk::<FRAME_HEADER_LEN>() else {
            unreachable!("frame header is buffered")
        };
        let frame_type = FrameType::new(header[3]);
        let flags = FrameFlags::new(header[4]);
        let stream_id = StreamId::new(
            u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & STREAM_ID_MASK,
        );
        self.buffer.advance(FRAME_HEADER_LEN);
        let payload = self.buffer.split_to(payload_len).freeze();

        Ok(Some(RawFrame {
            header: FrameHeader {
                len: payload_len,
                frame_type,
                flags,
                stream_id,
            },
            payload,
        }))
    }
}

const fn decode_u24(bytes: [u8; 3]) -> usize {
    let [b0, b1, b2] = bytes;
    ((b0 as usize) << 16) | ((b1 as usize) << 8) | (b2 as usize)
}

pub fn encode_frame_header(header: FrameHeader) -> [u8; 9] {
    debug_assert!(header.len <= MAX_FRAME_SIZE_UPPER_BOUND);
    let len = (header.len as u32).to_be_bytes();
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

pub fn append_frame(dst: &mut BytesMut, header: FrameHeader, payload: &[u8]) {
    assert_eq!(header.len, payload.len());
    dst.reserve(FRAME_HEADER_LEN + payload.len());
    dst.extend_from_slice(&encode_frame_header(header));
    dst.extend_from_slice(payload);
}

pub fn append_settings(dst: &mut BytesMut, settings: Settings) {
    let payload_len = settings.iter().count() * size_of::<WireSetting>();
    dst.reserve(FRAME_HEADER_LEN + payload_len);
    dst.extend_from_slice(&encode_frame_header(FrameHeader {
        len: payload_len,
        frame_type: FrameType::SETTINGS,
        flags: FrameFlags::EMPTY,
        stream_id: None,
    }));
    for (id, value) in settings.iter() {
        let entry = WireSetting {
            id: U16::new(id.bits()),
            value: U32::new(value),
        };
        dst.extend_from_slice(entry.as_bytes());
    }
}

pub fn append_settings_ack(dst: &mut BytesMut) {
    append_frame(
        dst,
        FrameHeader {
            len: 0,
            frame_type: FrameType::SETTINGS,
            flags: FrameFlags::ACK,
            stream_id: None,
        },
        &[],
    );
}

pub fn append_window_update(
    dst: &mut BytesMut,
    stream_id: Option<StreamId>,
    increment: WindowIncrement,
) {
    append_frame(
        dst,
        FrameHeader {
            len: 4,
            frame_type: FrameType::WINDOW_UPDATE,
            flags: FrameFlags::EMPTY,
            stream_id,
        },
        &increment.get().to_be_bytes(),
    );
}

pub fn append_ping_ack(dst: &mut BytesMut, payload: [u8; 8]) {
    append_frame(
        dst,
        FrameHeader {
            len: 8,
            frame_type: FrameType::PING,
            flags: FrameFlags::ACK,
            stream_id: None,
        },
        &payload,
    );
}

pub fn append_rst_stream(dst: &mut BytesMut, stream_id: StreamId, error_code: ErrorCode) {
    append_frame(
        dst,
        FrameHeader {
            len: 4,
            frame_type: FrameType::RST_STREAM,
            flags: FrameFlags::EMPTY,
            stream_id: Some(stream_id),
        },
        &error_code.to_be_bytes(),
    );
}

pub fn append_goaway(
    dst: &mut BytesMut,
    last_stream_id: Option<StreamId>,
    error_code: ErrorCode,
    debug: &[u8],
) {
    dst.reserve(GOAWAY_FRAME_PREFIX_LEN + debug.len());
    dst.extend_from_slice(&encode_frame_header(FrameHeader {
        len: 8 + debug.len(),
        frame_type: FrameType::GOAWAY,
        flags: FrameFlags::EMPTY,
        stream_id: None,
    }));
    dst.extend_from_slice(&last_stream_id.map_or(0, StreamId::get).to_be_bytes());
    dst.extend_from_slice(&error_code.to_be_bytes());
    dst.extend_from_slice(debug);
}

pub fn parse_settings_payload(payload: &[u8]) -> Result<PeerSettings, H2CornError> {
    if !payload.len().is_multiple_of(SETTING_ENTRY_LEN) {
        return H2Error::SettingsPayloadLengthInvalid.err();
    }

    // SAFETY: the length check above guarantees `payload` is an exact multiple of
    // `size_of::<WireSetting>()`, `WireSetting` is `Unaligned`, and its zerocopy
    // traits guarantee the byte representation can be viewed as `WireSetting`
    // values.
    let entries = unsafe {
        slice::from_raw_parts(
            payload.as_ptr().cast::<WireSetting>(),
            payload.len() / SETTING_ENTRY_LEN,
        )
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
            .chunks_exact(SETTING_ENTRY_LEN)
            .map(|entry| {
                (
                    u16::from_be_bytes([entry[0], entry[1]]),
                    u32::from_be_bytes([entry[2], entry[3], entry[4], entry[5]]),
                )
            })
            .collect()
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
    fn encode_frame_header_uses_usize_length() {
        let encoded = encode_frame_header(FrameHeader {
            len: 0x4000,
            frame_type: FrameType::DATA,
            flags: FrameFlags::EMPTY,
            stream_id: Some(StreamId::new(1).unwrap()),
        });
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
}
