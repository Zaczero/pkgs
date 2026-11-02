use std::num::NonZeroU64;

use crate::bridge::RequestBodyCounter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestBodyProgress {
    Continue,
    SizeLimitExceeded,
    ContentLengthExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestBodyFinish {
    Complete,
    ContentLengthMismatch,
}

#[derive(Debug)]
pub(crate) struct RequestBodyState {
    expected_length: Option<u64>,
    // Chunked parsing previews size before recording it. Reusing the optional
    // shared access-log counter would add an atomic read per preview.
    received_length: u64,
    access_log_bytes: Option<RequestBodyCounter>,
    max_body_size: Option<NonZeroU64>,
    deliver_to_app: bool,
}

impl RequestBodyState {
    pub(crate) const fn new(
        expected_length: Option<u64>,
        access_log_bytes: Option<RequestBodyCounter>,
        max_body_size: Option<NonZeroU64>,
    ) -> Self {
        Self {
            expected_length,
            received_length: 0,
            access_log_bytes,
            max_body_size,
            deliver_to_app: true,
        }
    }

    pub(crate) fn record_chunk(&mut self, chunk_len: u64) -> RequestBodyProgress {
        let (received_length, overflow) = self.received_length.overflowing_add(chunk_len);
        self.received_length = if overflow { u64::MAX } else { received_length };
        if let Some(access_log_bytes) = &self.access_log_bytes {
            access_log_bytes.add(chunk_len);
        }
        self.classify_received_length(self.received_length, overflow)
    }

    pub(crate) fn preview_chunk(&self, chunk_len: u64) -> RequestBodyProgress {
        let (received_length, overflow) = self.received_length.overflowing_add(chunk_len);
        self.classify_received_length(received_length, overflow)
    }

    pub(crate) fn finish(&self) -> RequestBodyFinish {
        if self
            .expected_length
            .is_some_and(|expected_length| expected_length != self.received_length)
        {
            RequestBodyFinish::ContentLengthMismatch
        } else {
            RequestBodyFinish::Complete
        }
    }

    pub(crate) const fn stop_delivering(&mut self) {
        self.deliver_to_app = false;
    }

    pub(crate) const fn should_deliver(&self) -> bool {
        self.deliver_to_app
    }

    fn classify_received_length(
        &self,
        received_length: u64,
        overflow: bool,
    ) -> RequestBodyProgress {
        if self
            .max_body_size
            .is_some_and(|max_body_size| overflow || received_length > max_body_size.get())
        {
            return RequestBodyProgress::SizeLimitExceeded;
        }
        if self
            .expected_length
            .is_some_and(|expected_length| overflow || received_length > expected_length)
        {
            return RequestBodyProgress::ContentLengthExceeded;
        }
        RequestBodyProgress::Continue
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::{RequestBodyFinish, RequestBodyProgress, RequestBodyState};
    use crate::bridge::RequestBodyCounter;

    fn body_counter() -> RequestBodyCounter {
        RequestBodyCounter::default()
    }

    #[test]
    fn body_state_tracks_access_log_bytes_and_size_limit() {
        let access_log_bytes = body_counter();
        let mut state =
            RequestBodyState::new(None, Some(access_log_bytes.clone()), NonZeroU64::new(5));

        assert_eq!(state.record_chunk(3), RequestBodyProgress::Continue);
        assert_eq!(
            state.record_chunk(3),
            RequestBodyProgress::SizeLimitExceeded
        );
        assert_eq!(access_log_bytes.load(), 6);
        assert_eq!(state.finish(), RequestBodyFinish::Complete);
    }

    #[test]
    fn body_state_detects_content_length_overrun_and_mismatch() {
        let mut state = RequestBodyState::new(Some(5), None, None);

        assert_eq!(state.record_chunk(3), RequestBodyProgress::Continue);
        assert_eq!(state.finish(), RequestBodyFinish::ContentLengthMismatch);
        assert_eq!(
            state.record_chunk(3),
            RequestBodyProgress::ContentLengthExceeded
        );
    }

    #[test]
    fn stopping_delivery_does_not_stop_accounting() {
        let access_log_bytes = body_counter();
        let mut state = RequestBodyState::new(Some(4), Some(access_log_bytes.clone()), None);

        state.stop_delivering();
        assert!(!state.should_deliver());
        assert_eq!(state.record_chunk(4), RequestBodyProgress::Continue);
        assert_eq!(state.finish(), RequestBodyFinish::Complete);
        assert_eq!(access_log_bytes.load(), 4);
    }

    #[test]
    fn preview_chunk_checks_limits_without_mutating_state() {
        let mut state = RequestBodyState::new(Some(5), None, NonZeroU64::new(6));

        assert_eq!(state.record_chunk(3), RequestBodyProgress::Continue);
        assert_eq!(state.preview_chunk(2), RequestBodyProgress::Continue);
        assert_eq!(
            state.preview_chunk(3),
            RequestBodyProgress::ContentLengthExceeded
        );
        assert_eq!(state.record_chunk(2), RequestBodyProgress::Continue);
        assert_eq!(state.finish(), RequestBodyFinish::Complete);
    }

    #[test]
    fn body_counter_overflow_preserves_limit_precedence() {
        let mut limited = RequestBodyState::new(Some(u64::MAX), None, NonZeroU64::new(u64::MAX));
        assert_eq!(
            limited.record_chunk(u64::MAX),
            RequestBodyProgress::Continue
        );
        assert_eq!(
            limited.preview_chunk(1),
            RequestBodyProgress::SizeLimitExceeded
        );
        assert_eq!(
            limited.record_chunk(1),
            RequestBodyProgress::SizeLimitExceeded
        );

        let mut declared = RequestBodyState::new(Some(u64::MAX), None, None);
        assert_eq!(
            declared.record_chunk(u64::MAX),
            RequestBodyProgress::Continue
        );
        assert_eq!(
            declared.preview_chunk(1),
            RequestBodyProgress::ContentLengthExceeded
        );
        assert_eq!(declared.finish(), RequestBodyFinish::Complete);
        assert_eq!(
            declared.record_chunk(1),
            RequestBodyProgress::ContentLengthExceeded
        );

        let mut unlimited = RequestBodyState::new(None, None, None);
        assert_eq!(
            unlimited.record_chunk(u64::MAX),
            RequestBodyProgress::Continue
        );
        assert_eq!(unlimited.preview_chunk(1), RequestBodyProgress::Continue);
        assert_eq!(unlimited.record_chunk(1), RequestBodyProgress::Continue);
    }
}
