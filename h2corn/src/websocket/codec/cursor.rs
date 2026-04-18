use std::cmp::min;

use bytes::{Bytes, BytesMut};

use crate::smallvec_deque::SmallVecDeque;

use super::mask::copy_masked_into;

#[derive(Debug, Default)]
pub(super) struct SegmentCursor<const N: usize> {
    segments: SmallVecDeque<Bytes, N>,
    offset: usize,
    len: usize,
}

impl<const N: usize> SegmentCursor<N> {
    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn push(&mut self, segment: Bytes) {
        if segment.is_empty() {
            return;
        }

        self.len += segment.len();
        self.segments.push_back(segment);
    }

    pub(super) fn peek_prefix(&self, len: usize, out: &mut [u8]) {
        debug_assert!(len <= self.len);
        debug_assert!(len <= out.len());

        let mut copied = 0;
        for (index, segment) in self.segments.iter().enumerate() {
            let start = if index == 0 { self.offset } else { 0 };
            let remaining = len - copied;
            let chunk_len = min(segment.len() - start, remaining);
            out[copied..copied + chunk_len].copy_from_slice(&segment[start..start + chunk_len]);
            copied += chunk_len;
            if copied == len {
                return;
            }
        }

        debug_assert_eq!(copied, len);
    }

    pub(super) fn skip(&mut self, mut len: usize) {
        debug_assert!(len <= self.len);
        self.len -= len;

        while len != 0 {
            let available = {
                let front = self
                    .segments
                    .front()
                    .expect("segmented websocket input remains available while consuming");
                front.len() - self.offset
            };
            if len < available {
                self.offset += len;
                return;
            }

            len -= available;
            self.segments.pop_front();
            self.offset = 0;
        }
    }

    pub(super) fn take_masked_payload(&mut self, len: usize, mask: [u8; 4]) -> Bytes {
        debug_assert!(len <= self.len);

        let mut out = BytesMut::with_capacity(len);
        let mut remaining = len;
        let mut phase = 0;
        while remaining != 0 {
            let take = {
                let front = self
                    .segments
                    .front()
                    .expect("segmented websocket input remains available while draining");
                let available = &front[self.offset..];
                let take = min(available.len(), remaining);
                let start = out.len();
                phase = copy_masked_into(
                    &mut out.spare_capacity_mut()[..take],
                    &available[..take],
                    mask,
                    phase,
                );
                // SAFETY: `copy_masked_into` initializes exactly `take` bytes.
                unsafe {
                    out.set_len(start + take);
                }
                take
            };
            self.skip(take);
            remaining -= take;
        }
        out.freeze()
    }
}
