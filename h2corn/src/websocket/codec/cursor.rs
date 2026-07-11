use std::cmp::min;

use bytes::{Buf, Bytes, BytesMut};

use super::mask::{MaskKey, apply_websocket_mask_phase};
use crate::smallvec_deque::SmallVecDeque;

#[derive(Debug, Default)]
pub(super) struct SegmentCursor<const N: usize> {
    segments: SmallVecDeque<Bytes, N>,
    offset: usize,
    len: usize,
}

impl<const N: usize> SegmentCursor<N> {
    pub(super) const fn len(&self) -> usize {
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
        let key = MaskKey::new(mask);

        // A transport segment commonly contains exactly one complete WebSocket
        // frame. When its backing allocation is no longer shared with the
        // transport read buffer, take that allocation and unmask the payload in
        // place. `is_unique` is stable here: the cursor has exclusive access to
        // the only handle that another thread could clone.
        if self
            .segments
            .front()
            .is_some_and(|segment| segment.len() - self.offset == len && segment.is_unique())
        {
            let segment = self
                .segments
                .pop_front()
                .expect("unique payload segment is present");
            let mut payload = segment
                .try_into_mut()
                .expect("uniquely owned Bytes converts without copying");
            payload.advance(self.offset);
            self.offset = 0;
            self.len -= len;
            apply_websocket_mask_phase(payload.as_mut(), key, 0);
            return payload.freeze();
        }

        let mut out = BytesMut::with_capacity(len);
        let mut remaining = len;
        let mut phase = 0;
        self.len -= len;
        while remaining != 0 {
            let front = self
                .segments
                .front()
                .expect("segmented websocket input remains available while draining");
            let available = &front[self.offset..];
            let take = min(available.len(), remaining);
            let start = out.len();
            out.extend_from_slice(&available[..take]);
            phase = apply_websocket_mask_phase(&mut out[start..start + take], key, phase);
            remaining -= take;
            if take == available.len() {
                self.segments.pop_front();
                self.offset = 0;
            } else {
                self.offset += take;
            }
        }
        out.freeze()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::SegmentCursor;

    #[test]
    fn complete_unique_segment_is_unmasked_in_place() {
        let mask = [1_u8, 2, 3, 4];
        let prefix_len = 6;
        let mut wire = vec![0xAA; prefix_len];
        wire.extend(
            b"payload"
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index & 3]),
        );
        let segment = Bytes::from(wire);
        let payload_ptr = segment[prefix_len..].as_ptr();
        let mut cursor = SegmentCursor::<2>::default();
        cursor.push(segment);
        cursor.skip(prefix_len);

        let payload = cursor.take_masked_payload(7, mask);

        assert_eq!(payload.as_ref(), b"payload");
        assert_eq!(
            payload.as_ptr(),
            payload_ptr,
            "payload allocation was copied"
        );
        assert_eq!(cursor.len(), 0);
    }

    #[test]
    fn shared_segment_falls_back_without_mutating_another_owner() {
        let mask = [1_u8, 2, 3, 4];
        let masked = b"payload"
            .iter()
            .enumerate()
            .map(|(index, byte)| byte ^ mask[index & 3])
            .collect::<Vec<_>>();
        let segment = Bytes::from(masked);
        let other_owner = segment.clone();
        let mut cursor = SegmentCursor::<2>::default();
        cursor.push(segment);

        let payload = cursor.take_masked_payload(7, mask);

        assert_eq!(payload.as_ref(), b"payload");
        assert_ne!(other_owner.as_ref(), b"payload");
    }
}
