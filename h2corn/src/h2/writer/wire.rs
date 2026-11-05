//! Prepared frame ownership and resumable transport progress for the HTTP/2 writer.

use std::io::{self, IoSlice};
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::BytesMut;
use smallvec::SmallVec;
use tokio::io::{AsyncWrite, BufWriter};

use crate::h2::writer::WindowTarget;
use crate::h2::writer::stream_state::{BodyItem, PendingBody};
use crate::h2_frame::{
    self, FRAME_HEADER_LEN, FrameFlags, FrameHeader, FramePayloadLen, FrameType, StreamId,
    WindowIncrement,
};
use crate::sendfile::WriteTarget;

pub(super) const FRAME_QUANTUM: usize = 64;
pub(super) const BYTE_QUANTUM: usize = 64 * 1024;

pub(super) struct WriteProgress {
    pub(super) bytes: usize,
    pub(super) complete: bool,
    pub(super) flow_bytes: usize,
}

pub(super) struct DataFrame {
    header: [u8; FRAME_HEADER_LEN],
    source: usize,
    offset: usize,
    len: FramePayloadLen,
}

impl DataFrame {
    const fn wire_len(&self) -> usize {
        FRAME_HEADER_LEN
            .checked_add(self.len.as_usize())
            .expect("HTTP/2 frame extent fits usize")
    }

    pub(super) fn new(
        stream_id: StreamId,
        source: usize,
        offset: usize,
        len: FramePayloadLen,
        end: bool,
    ) -> Self {
        Self {
            header: h2_frame::encode_frame_header(
                FrameHeader {
                    frame_type: FrameType::DATA,
                    flags: if end {
                        FrameFlags::END_STREAM
                    } else {
                        FrameFlags::EMPTY
                    },
                    stream_id: Some(stream_id),
                },
                len,
            ),
            source,
            offset,
            len,
        }
    }
}

pub(super) struct DataCursor {
    pub(super) stream_id: StreamId,
    pub(super) body: PendingBody,
    pub(super) frames: SmallVec<[DataFrame; 9]>,
    written: usize,
    pub(super) end_stream: bool,
    pub(super) retired: bool,
    transport: DataTransport,
}

enum DataTransport {
    Buffered,
    Sendfile { header_flushed: bool },
}

impl DataCursor {
    pub(super) fn new(stream_id: StreamId, body: PendingBody, sendfile: bool) -> Self {
        Self {
            stream_id,
            body,
            frames: SmallVec::new(),
            written: 0,
            end_stream: false,
            retired: false,
            transport: if sendfile {
                DataTransport::Sendfile {
                    header_flushed: false,
                }
            } else {
                DataTransport::Buffered
            },
        }
    }

    fn wire_len(&self) -> usize {
        self.frame_ranges().last().map_or(0, |(_, range)| range.end)
    }

    /// Each retained frame paired with its extent in this ordered transport batch.
    fn frame_ranges(&self) -> impl Iterator<Item = (&DataFrame, Range<usize>)> {
        self.frames.iter().scan(0_usize, |start, frame| {
            let end = start
                .checked_add(frame.wire_len())
                .expect("prepared frame batch fits usize");
            let range = *start..end;
            *start = end;
            Some((frame, range))
        })
    }

    pub(super) fn retire(&mut self) {
        self.retired = true;
        let active = self
            .frame_ranges()
            .enumerate()
            .find_map(|(index, (frame, range))| {
                (self.written > range.start && self.written < range.end).then_some((
                    index,
                    frame.source,
                    range.start,
                ))
            });
        let Some((current, source, frame_start)) = active else {
            self.frames.clear();
            self.body = PendingBody::new();
            self.written = 0;
            return;
        };
        for _ in 0..source {
            self.body
                .pop_front()
                .expect("preceding frame source is retained");
        }
        let retained = self
            .body
            .pop_front()
            .expect("active DATA frame retains its payload owner");
        self.body.clear();
        self.body.push_back(retained);
        self.frames.drain(..current);
        self.frames.truncate(1);
        self.frames
            .first_mut()
            .expect("active frame retained")
            .source = 0;
        self.written = self
            .written
            .checked_sub(frame_start)
            .expect("active frame begins before transport cursor");
    }

    pub(super) fn constrain(&mut self, available: i64, maximum: FramePayloadLen) -> bool {
        if self.retired {
            return false;
        }
        let mut budget = available.max(0) as usize;
        let truncate = self
            .frame_ranges()
            .enumerate()
            .find_map(|(index, (frame, range))| {
                if self.written <= range.start {
                    if frame.len.as_usize() > budget || frame.len > maximum {
                        return Some(index);
                    }
                    budget = budget
                        .checked_sub(frame.len.as_usize())
                        .expect("frame fits available peer credit");
                }
                None
            });
        if let Some(index) = truncate {
            if self.written == 0 {
                return true;
            }
            self.frames.truncate(index);
            self.end_stream = self
                .frames
                .last()
                .is_some_and(|frame| frame.header[4] & 1 != 0);
        }
        false
    }

    fn record_written(&mut self, bytes: usize) -> WriteProgress {
        let after = self
            .written
            .checked_add(bytes)
            .expect("transport progress fits prepared batch");
        let mut flow_bytes = 0_usize;
        for (frame, range) in self.frame_ranges() {
            // A prepared frame has not spent peer credit. Commit its full
            // payload once its first byte enters the ordered transport.
            if self.written <= range.start && after > range.start {
                flow_bytes = flow_bytes
                    .checked_add(frame.len.as_usize())
                    .expect("flow bytes fit prepared batch");
            }
        }
        self.written = after;
        WriteProgress {
            bytes,
            complete: after == self.wire_len(),
            flow_bytes,
        }
    }

    pub(super) fn poll_write<W: WriteTarget>(
        &mut self,
        writer: &mut BufWriter<W>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<WriteProgress>> {
        if self.written == self.wire_len() {
            return Poll::Ready(Ok(WriteProgress {
                bytes: 0,
                complete: true,
                flow_bytes: 0,
            }));
        }
        if matches!(self.transport, DataTransport::Sendfile { .. }) {
            return self.poll_sendfile(writer, cx);
        }
        let mut slices = SmallVec::<[IoSlice<'_>; 27]>::new();
        let mut sources = self.body.iter().enumerate();
        let (mut source_index, mut source) = sources.next().expect("DATA batch has a source");
        for frame in &self.frames {
            while source_index < frame.source {
                (source_index, source) = sources.next().expect("frame source is retained");
            }
            slices.push(IoSlice::new(&frame.header));
            match source {
                BodyItem::Chunk(chunk) => {
                    let (first, second) =
                        chunk.remaining_slices(frame.offset, frame.len.as_usize());
                    if !first.is_empty() {
                        slices.push(IoSlice::new(first));
                    }
                    if !second.is_empty() {
                        slices.push(IoSlice::new(second));
                    }
                },
                BodyItem::File(file) => {
                    slices.push(IoSlice::new(
                        file.remaining()
                            .get(frame.offset..)
                            .and_then(|remaining| remaining.get(..frame.len.as_usize()))
                            .expect("prepared DATA fits its retained file buffer"),
                    ));
                },
            }
        }
        let written = ready!(poll_slices(writer, cx, &mut slices, self.written))?;
        drop(slices);
        drop(sources);
        Poll::Ready(Ok(self.record_written(written)))
    }

    fn poll_sendfile<W: WriteTarget>(
        &mut self,
        writer: &mut BufWriter<W>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<WriteProgress>> {
        let frame = self.frames.first().expect("sendfile has one DATA frame");
        if self.written < FRAME_HEADER_LEN {
            let written = ready!(
                Pin::new(&mut *writer).poll_write(
                    cx,
                    frame
                        .header
                        .get(self.written..)
                        .expect("header cursor is bounded")
                )
            )?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            return Poll::Ready(Ok(self.record_written(written)));
        }
        let DataTransport::Sendfile { header_flushed } = &mut self.transport else {
            unreachable!("sendfile cursor uses the sendfile transport")
        };
        if !*header_flushed {
            ready!(Pin::new(&mut *writer).poll_flush(cx))?;
            *header_flushed = true;
        }
        let Some(BodyItem::File(file)) = self.body.front_mut() else {
            unreachable!("sendfile retains its file owner")
        };
        let mut cursor = file
            .sendfile_cursor()
            .expect("sendfile remains in its selected mode");
        let (file, offset) = cursor.parts();
        let remaining = frame
            .wire_len()
            .checked_sub(self.written)
            .expect("sendfile progress stays within frame");
        let written = ready!(W::poll_send_file(writer, file, offset, remaining, cx))?;
        if written == 0 {
            return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
        }
        drop(cursor);
        Poll::Ready(Ok(self.record_written(written)))
    }

    pub(super) fn consume(&mut self) {
        if self.retired || self.end_stream {
            return;
        }
        if matches!(self.transport, DataTransport::Sendfile { .. }) {
            if matches!(self.body.front(), Some(BodyItem::File(file)) if file.is_drained()) {
                self.body.pop_front();
            }
            return;
        }
        for frame in &self.frames {
            // Completed sources leave the head; all offsets were relative to
            // the original queue, so consumption itself is strictly sequential.
            let Some(item) = self.body.front_mut() else {
                unreachable!("frame source exists")
            };
            let drained = match item {
                BodyItem::Chunk(chunk) => {
                    chunk.consume(frame.len.as_usize());
                    chunk.remaining_len() == 0
                },
                BodyItem::File(file) => {
                    file.consume(frame.len.as_usize());
                    file.is_drained() || file.needs_fill()
                },
            };
            if drained {
                let remove = match item {
                    BodyItem::Chunk(_) => true,
                    BodyItem::File(file) => file.is_drained(),
                };
                if remove {
                    self.body.pop_front();
                }
            }
        }
    }
}

pub(super) struct HeaderCursor {
    pub(super) stream_id: StreamId,
    pub(super) end_stream: bool,
    pub(super) retired: bool,
    maximum: FramePayloadLen,
    block_offset: usize,
    written: usize,
}

impl HeaderCursor {
    pub(super) const fn new(
        stream_id: StreamId,
        end_stream: bool,
        maximum: FramePayloadLen,
    ) -> Self {
        Self {
            stream_id,
            end_stream,
            retired: false,
            maximum: maximum.min_usize(BYTE_QUANTUM),
            block_offset: 0,
            written: 0,
        }
    }

    pub(super) fn poll_write<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut BufWriter<W>,
        block: &[u8],
        maximum: FramePayloadLen,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<WriteProgress>> {
        if self.written == 0 {
            self.maximum = maximum.min_usize(BYTE_QUANTUM);
        }
        let remaining = block
            .get(self.block_offset..)
            .expect("header cursor fits encoded block");
        let len = self.maximum.min_usize(remaining.len());
        let (payload, suffix) = remaining.split_at(len.as_usize());
        let last = suffix.is_empty();
        let first = self.block_offset == 0;
        let mut flags = if last {
            FrameFlags::END_HEADERS
        } else {
            FrameFlags::EMPTY
        };
        if first && self.end_stream {
            flags |= FrameFlags::END_STREAM;
        }
        let header = h2_frame::encode_frame_header(
            FrameHeader {
                frame_type: if first {
                    FrameType::HEADERS
                } else {
                    FrameType::CONTINUATION
                },
                flags,
                stream_id: Some(self.stream_id),
            },
            len,
        );
        let mut slices = [IoSlice::new(&header), IoSlice::new(payload)];
        let written = ready!(poll_slices(writer, cx, &mut slices, self.written))?;
        self.written = self
            .written
            .checked_add(written)
            .expect("header transport cursor fits frame");
        if self.written
            == FRAME_HEADER_LEN
                .checked_add(payload.len())
                .expect("header frame extent fits usize")
        {
            self.block_offset = block
                .len()
                .checked_sub(suffix.len())
                .expect("fragment suffix belongs to encoded block");
            self.written = 0;
            Poll::Ready(Ok(WriteProgress {
                bytes: written,
                complete: last,
                flow_bytes: 0,
            }))
        } else {
            Poll::Ready(Ok(WriteProgress {
                bytes: written,
                complete: false,
                flow_bytes: 0,
            }))
        }
    }
}

pub(super) fn poll_slices<W: AsyncWrite + Unpin>(
    writer: &mut W,
    cx: &mut Context<'_>,
    slices: &mut [IoSlice<'_>],
    offset: usize,
) -> Poll<io::Result<usize>> {
    let mut remaining = slices;
    IoSlice::advance_slices(&mut remaining, offset);
    let written = ready!(Pin::new(writer).poll_write_vectored(cx, remaining))?;
    if written == 0 {
        return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
    }
    Poll::Ready(Ok(written))
}

#[expect(
    clippy::large_enum_variant,
    reason = "one inline cursor per connection avoids allocating each bounded DATA batch"
)]
pub(super) enum ActiveOutput {
    Headers(HeaderCursor),
    Data(DataCursor),
    Control {
        written: usize,
        input_credit: Option<(WindowTarget, WindowIncrement)>,
    },
}

impl ActiveOutput {
    pub(super) fn retire_stream(&mut self, stream_id: StreamId) {
        match self {
            Self::Headers(headers) if headers.stream_id == stream_id => {
                headers.retired = true;
            },
            Self::Data(data) if data.stream_id == stream_id => data.retire(),
            Self::Headers(_) | Self::Data(_) | Self::Control { .. } => {},
        }
    }
}

pub(super) fn poll_control<W: AsyncWrite + Unpin>(
    writer: &mut BufWriter<W>,
    frame: &BytesMut,
    written: &mut usize,
    cx: &mut Context<'_>,
) -> Poll<io::Result<WriteProgress>> {
    let count = ready!(Pin::new(writer).poll_write(
        cx,
        frame.get(*written..).expect("control cursor is bounded")
    ))?;
    if count == 0 {
        return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
    }
    *written = written
        .checked_add(count)
        .expect("control transport cursor fits frame");
    Poll::Ready(Ok(WriteProgress {
        bytes: count,
        complete: *written == frame.len(),
        flow_bytes: 0,
    }))
}
