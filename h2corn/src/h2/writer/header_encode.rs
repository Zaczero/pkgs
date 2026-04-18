use bytes::{Bytes, BytesMut};
use tokio::io::AsyncWrite;

use crate::error::{ErrorExt, H2CornError, HttpResponseError};
use crate::frame::{FrameFlags, FrameHeader, FrameType, StreamId};
use crate::hpack::Encoder;
use crate::http::types::{HttpStatusCode, ResponseHeaders, status_code};

use super::ENCODED_HEADER_BLOCK_CAPACITY;
use super::flush::write_frame;

pub(super) struct HeaderEncodeState {
    encoder: Encoder,
    block: BytesMut,
}

impl HeaderEncodeState {
    pub(super) fn new() -> Self {
        Self {
            encoder: Encoder::new(),
            block: BytesMut::with_capacity(ENCODED_HEADER_BLOCK_CAPACITY),
        }
    }

    pub(super) fn encode_response(
        &mut self,
        status: HttpStatusCode,
        headers: &ResponseHeaders,
    ) -> Result<Bytes, H2CornError> {
        self.block.clear();
        self.encoder.begin_block(&mut self.block);
        encode_header_block(&mut self.encoder, &mut self.block, Some(status), headers)
    }

    pub(super) fn encode_trailers(
        &mut self,
        headers: &ResponseHeaders,
    ) -> Result<Bytes, H2CornError> {
        self.block.clear();
        self.encoder.begin_block(&mut self.block);
        encode_header_block(&mut self.encoder, &mut self.block, None, headers)
    }

    pub(super) fn update_max_size(&mut self, size: usize) {
        self.encoder.update_max_size(size);
    }
}

pub(super) async fn write_header_block<W>(
    writer: &mut W,
    stream_id: StreamId,
    end_stream: bool,
    header_block: &[u8],
    max_frame_size: usize,
) -> Result<(), H2CornError>
where
    W: AsyncWrite + Unpin,
{
    if header_block.len() <= max_frame_size {
        return write_frame(
            writer,
            FrameHeader {
                len: header_block.len(),
                frame_type: FrameType::HEADERS,
                flags: FrameFlags::END_HEADERS
                    | if end_stream {
                        FrameFlags::END_STREAM
                    } else {
                        FrameFlags::EMPTY
                    },
                stream_id: Some(stream_id),
            },
            header_block,
        )
        .await;
    }

    let mut first = true;
    let mut cursor = 0;

    loop {
        let remaining = &header_block[cursor..];
        let chunk_len = remaining.len().min(max_frame_size);
        let chunk = &remaining[..chunk_len];
        cursor += chunk_len;

        let is_last = cursor == header_block.len();
        let mut flags = if is_last {
            FrameFlags::END_HEADERS
        } else {
            FrameFlags::EMPTY
        };
        if first && end_stream {
            flags |= FrameFlags::END_STREAM;
        }

        let frame_type = if first {
            FrameType::HEADERS
        } else {
            FrameType::CONTINUATION
        };
        write_frame(
            writer,
            FrameHeader {
                len: chunk.len(),
                frame_type,
                flags,
                stream_id: Some(stream_id),
            },
            chunk,
        )
        .await?;

        if is_last {
            return Ok(());
        }
        first = false;
    }
}

fn encode_header_block(
    encoder: &mut Encoder,
    out: &mut BytesMut,
    status: Option<u16>,
    headers: &ResponseHeaders,
) -> Result<Bytes, H2CornError> {
    if let Some(status) = status {
        encode_status_header(encoder, out, status)?;
    }
    encode_header_fields(encoder, out, headers)?;
    Ok(out.split().freeze())
}

fn encode_status_header(
    encoder: &Encoder,
    out: &mut BytesMut,
    status: HttpStatusCode,
) -> Result<(), H2CornError> {
    match status {
        status_code::OK => encoder.encode_indexed(8, out),
        status_code::NO_CONTENT => encoder.encode_indexed(9, out),
        status_code::PARTIAL_CONTENT => encoder.encode_indexed(10, out),
        status_code::NOT_MODIFIED => encoder.encode_indexed(11, out),
        status_code::BAD_REQUEST => encoder.encode_indexed(12, out),
        status_code::NOT_FOUND => encoder.encode_indexed(13, out),
        status_code::INTERNAL_SERVER_ERROR => encoder.encode_indexed(14, out),
        100..=999 => encoder.encode_indexed_name_bytes(8, &status_to_bytes(status), out),
        _ => {
            return HttpResponseError::StatusMustBeThreeDigitCode.err();
        }
    }
    Ok(())
}

fn status_to_bytes(status: HttpStatusCode) -> [u8; 3] {
    [
        b'0' + ((status / 100) % 10) as u8,
        b'0' + ((status / 10) % 10) as u8,
        b'0' + (status % 10) as u8,
    ]
}

fn encode_header_fields(
    encoder: &mut Encoder,
    out: &mut BytesMut,
    headers: &ResponseHeaders,
) -> Result<(), H2CornError> {
    for (name, value) in headers {
        encoder.encode_field_bytes(name.as_bytes(), value.as_bytes(), out);
    }
    Ok(())
}
