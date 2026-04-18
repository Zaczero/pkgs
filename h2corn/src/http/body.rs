use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

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
    received_length: u64,
    access_log_bytes: Option<Arc<AtomicU64>>,
    max_body_size: Option<u64>,
    deliver_to_app: bool,
}

impl RequestBodyState {
    pub(crate) fn new(
        expected_length: Option<u64>,
        access_log_bytes: Option<Arc<AtomicU64>>,
        max_body_size: Option<u64>,
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
        self.received_length = self.received_length.saturating_add(chunk_len);
        if let Some(access_log_bytes) = &self.access_log_bytes {
            access_log_bytes.fetch_add(chunk_len, Ordering::Relaxed);
        }
        if self
            .max_body_size
            .is_some_and(|max_body_size| self.received_length > max_body_size)
        {
            return RequestBodyProgress::SizeLimitExceeded;
        }
        if self
            .expected_length
            .is_some_and(|expected_length| self.received_length > expected_length)
        {
            return RequestBodyProgress::ContentLengthExceeded;
        }
        RequestBodyProgress::Continue
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

    pub(crate) fn stop_delivering(&mut self) {
        self.deliver_to_app = false;
    }

    pub(crate) fn should_deliver(&self) -> bool {
        self.deliver_to_app
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use super::{RequestBodyFinish, RequestBodyProgress, RequestBodyState};

    #[test]
    fn body_state_tracks_access_log_bytes_and_size_limit() {
        let access_log_bytes = Arc::new(AtomicU64::new(0));
        let mut state = RequestBodyState::new(None, Some(access_log_bytes.clone()), Some(5));

        assert_eq!(state.record_chunk(3), RequestBodyProgress::Continue);
        assert_eq!(
            state.record_chunk(3),
            RequestBodyProgress::SizeLimitExceeded
        );
        assert_eq!(access_log_bytes.load(Ordering::Relaxed), 6);
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
        let access_log_bytes = Arc::new(AtomicU64::new(0));
        let mut state = RequestBodyState::new(Some(4), Some(access_log_bytes.clone()), None);

        state.stop_delivering();
        assert!(!state.should_deliver());
        assert_eq!(state.record_chunk(4), RequestBodyProgress::Continue);
        assert_eq!(state.finish(), RequestBodyFinish::Complete);
        assert_eq!(access_log_bytes.load(Ordering::Relaxed), 4);
    }
}
