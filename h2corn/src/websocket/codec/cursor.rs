use std::cmp::min;

use bytes::{Buf as _, Bytes, BytesMut};

use crate::inline_fifo::InlineFifo;
use crate::websocket::codec::mask::{MaskKey, apply_websocket_mask_phase};

#[derive(Debug, Default)]
pub(super) struct SegmentCursor<const N: usize> {
    segments: InlineFifo<Bytes, N>,
    len: usize,
}

impl<const N: usize> SegmentCursor<N> {
    pub(super) const fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(super) fn front_is_unique(&self) -> Option<bool> {
        self.segments.front().map(Bytes::is_unique)
    }

    pub(super) fn push(&mut self, segment: Bytes) {
        if segment.is_empty() {
            return;
        }

        self.len = self.len.wrapping_add(segment.len());
        self.segments.push_back(segment);
    }

    pub(super) fn peek_prefix(&self, len: usize, out: &mut [u8]) {
        debug_assert!(len <= self.len);
        debug_assert!(len <= out.len());

        let mut remaining = out.get_mut(..len).expect("prefix fits destination");
        for segment in self.segments.iter() {
            let count = min(segment.len(), remaining.len());
            let (destination, rest) = remaining.split_at_mut(count);
            destination.copy_from_slice(segment.get(..count).expect("prefix fits segment"));
            remaining = rest;
            if remaining.is_empty() {
                return;
            }
        }

        debug_assert!(remaining.is_empty());
    }

    pub(super) fn skip(&mut self, mut len: usize) {
        debug_assert!(len <= self.len);
        self.len = self.len.wrapping_sub(len);

        while len != 0 {
            let available = {
                let front = self
                    .segments
                    .front_mut()
                    .expect("segmented websocket input remains available while consuming");
                if len < front.len() {
                    front.advance(len);
                    return;
                }
                front.len()
            };

            len = len.wrapping_sub(available);
            self.segments.pop_front();
        }
    }

    /// Consume one available masked region. Callers retain the returned mask
    /// phase across regions, so compressed input can be inflated and released
    /// one transport segment at a time instead of materializing a whole frame.
    pub(super) fn take_masked_chunk(
        &mut self,
        len: usize,
        key: MaskKey,
        phase: usize,
    ) -> (BytesMut, usize) {
        debug_assert!(len <= self.len);

        // A transport segment commonly contains exactly one complete WebSocket
        // frame. When its backing allocation is no longer shared with the
        // transport read buffer, take that allocation and unmask the payload in
        // place. `is_unique` is stable here: the cursor has exclusive access to
        // the only handle that another thread could clone.
        if self
            .segments
            .front()
            .is_some_and(|segment| segment.len() == len && segment.is_unique())
        {
            let segment = self
                .segments
                .pop_front()
                .expect("unique payload segment is present");
            let mut payload = segment
                .try_into_mut()
                .expect("uniquely owned Bytes converts without copying");
            self.len = self.len.wrapping_sub(len);
            let phase = apply_websocket_mask_phase(payload.as_mut(), key, phase);
            return (payload, phase);
        }

        let mut out = BytesMut::with_capacity(len);
        let mut remaining = len;
        let mut phase = phase;
        self.len = self.len.wrapping_sub(len);
        while remaining != 0 {
            let front = self
                .segments
                .front_mut()
                .expect("segmented websocket input remains available while draining");
            let take = min(front.len(), remaining);
            let start = out.len();
            out.extend_from_slice(front.get(..take).expect("chunk fits front segment"));
            front.advance(take);
            let consumed_front = front.is_empty();
            phase = apply_websocket_mask_phase(
                out.get_mut(start..)
                    .expect("appended chunk starts within output"),
                key,
                phase,
            );
            remaining = remaining.wrapping_sub(take);
            if consumed_front {
                self.segments.pop_front();
            }
        }
        (out, phase)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{MaskKey, SegmentCursor};

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

        let (payload, _) = cursor.take_masked_chunk(7, MaskKey::new(mask), 0);

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

        let (payload, _) = cursor.take_masked_chunk(7, MaskKey::new(mask), 0);

        assert_eq!(payload.as_ref(), b"payload");
        assert_ne!(other_owner.as_ref(), b"payload");
    }
}
