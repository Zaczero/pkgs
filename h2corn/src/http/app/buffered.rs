use std::sync::Arc;

use tokio::sync::mpsc;

use crate::bridge::{ASGI_QUEUE_CAPACITY, HttpOutboundEvent};
use crate::buffered_events::BufferedState;

enum HttpSendMode {
    Buffering,
    Streaming { tx: mpsc::Sender<HttpOutboundEvent> },
}

#[derive(Clone)]
pub struct HttpSendState {
    shared: Arc<BufferedState<HttpSendMode, HttpOutboundEvent, 2>>,
}

pub struct HttpSendBuffer {
    shared: Arc<BufferedState<HttpSendMode, HttpOutboundEvent, 2>>,
    stream_rx: Option<mpsc::Receiver<HttpOutboundEvent>>,
}

impl HttpSendState {
    pub(crate) fn new() -> (Self, HttpSendBuffer) {
        let send_state = Self {
            shared: Arc::new(BufferedState::new(HttpSendMode::Buffering)),
        };
        let send_buffer = HttpSendBuffer {
            shared: Arc::clone(&send_state.shared),
            stream_rx: None,
        };
        (send_state, send_buffer)
    }

    pub(crate) fn push_or_forward(
        &self,
        event: HttpOutboundEvent,
    ) -> Option<(mpsc::Sender<HttpOutboundEvent>, HttpOutboundEvent)> {
        let mut inner = self.shared.lock();
        match &inner.state {
            HttpSendMode::Buffering => {
                inner.queue.push_back(event);
                drop(inner);
                self.shared.notify_ready();
                None
            },
            HttpSendMode::Streaming { tx } => Some((tx.clone(), event)),
        }
    }
}

impl HttpSendBuffer {
    pub(super) fn take_ready(&mut self, streaming: bool) -> Option<HttpOutboundEvent> {
        let mut inner = self.shared.lock();
        if let Some(event) = inner.queue.pop_front() {
            return Some(event);
        }
        if !streaming {
            return None;
        }

        if self.stream_rx.is_none() && matches!(inner.state, HttpSendMode::Buffering) {
            let (tx, rx) = mpsc::channel(ASGI_QUEUE_CAPACITY);
            self.stream_rx = Some(rx);
            inner.state = HttpSendMode::Streaming { tx };
        }
        drop(inner);
        self.stream_rx.as_mut().and_then(|rx| rx.try_recv().ok())
    }

    pub(super) async fn wait_ready(&mut self, streaming: bool) -> Option<HttpOutboundEvent> {
        loop {
            if let Some(event) = self.take_ready(streaming) {
                return Some(event);
            }
            if streaming {
                return self
                    .stream_rx
                    .as_mut()
                    .expect("streaming is enabled before waiting for streaming events")
                    .recv()
                    .await;
            }
            self.shared.wait_ready().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::HttpSendState;
    use crate::bridge::{HttpOutboundEvent, PayloadBytes};

    fn body_event(body: &'static [u8]) -> HttpOutboundEvent {
        HttpOutboundEvent::Body {
            body: PayloadBytes::from(Bytes::from_static(body)),
            more_body: true,
        }
    }

    fn assert_body_event(event: HttpOutboundEvent, expected: &[u8]) {
        match event {
            HttpOutboundEvent::Body { body, more_body } => {
                assert_eq!(body.as_ref(), expected);
                assert!(more_body);
            },
            other => panic!("expected buffered HTTP body event, got {other:?}"),
        }
    }

    #[test]
    fn buffered_events_drain_before_streaming_mode_forwards_new_events() {
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(send_state.push_or_forward(body_event(b"first")).is_none());
        assert!(send_state.push_or_forward(body_event(b"second")).is_none());

        assert_body_event(
            send_buffer
                .take_ready(true)
                .expect("first buffered event is available"),
            b"first",
        );
        assert_body_event(
            send_buffer
                .take_ready(true)
                .expect("second buffered event is available"),
            b"second",
        );
        assert!(
            send_buffer.take_ready(true).is_none(),
            "draining buffered events transitions the send state into streaming mode"
        );

        let (_tx, event) = send_state
            .push_or_forward(body_event(b"third"))
            .expect("new events are forwarded once streaming begins");
        assert_body_event(event, b"third");
    }

    #[test]
    fn enabling_streaming_without_buffered_events_forwards_immediately() {
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(
            send_buffer.take_ready(true).is_none(),
            "empty buffer flips directly into streaming mode"
        );

        let (_tx, event) = send_state
            .push_or_forward(body_event(b"live"))
            .expect("streaming mode forwards live events immediately");
        assert_body_event(event, b"live");
    }
}
