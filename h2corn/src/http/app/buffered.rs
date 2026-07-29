use std::sync::Arc;

use tokio::sync::mpsc;

use crate::async_util::{TryPush, try_push};
use crate::bridge::{HTTP_ASGI_QUEUE_CAPACITY, HttpOutboundEvent};
use crate::buffered_events::BufferedState;

enum HttpSendMode {
    Inline {
        accepted: u8,
    },
    Streaming {
        tx: mpsc::Sender<HttpOutboundEvent>,
        rx: Option<mpsc::Receiver<HttpOutboundEvent>>,
    },
    Closed,
}

/// Result of handing an ASGI event to the response driver. The common
/// buffered and sent states carry no channel owner; only a channel proven full
/// transfers one sender clone into a backpressure waiter.
pub(crate) enum HttpSendDisposition {
    Buffered,
    Sent,
    Backpressured {
        tx: mpsc::Sender<HttpOutboundEvent>,
        event: HttpOutboundEvent,
    },
    Closed,
}

#[derive(Clone)]
pub(crate) struct HttpSendState {
    shared: Arc<BufferedState<HttpSendMode, HttpOutboundEvent, 2>>,
}

pub(crate) struct HttpSendBuffer {
    shared: Arc<BufferedState<HttpSendMode, HttpOutboundEvent, 2>>,
    stream_rx: Option<mpsc::Receiver<HttpOutboundEvent>>,
}

impl HttpSendState {
    pub(crate) fn new() -> (Self, HttpSendBuffer) {
        let send_state = Self {
            shared: Arc::new(BufferedState::new(HttpSendMode::Inline { accepted: 0 })),
        };
        let send_buffer = HttpSendBuffer {
            shared: Arc::clone(&send_state.shared),
            stream_rx: None,
        };
        (send_state, send_buffer)
    }

    pub(crate) fn push_or_forward(&self, event: HttpOutboundEvent) -> HttpSendDisposition {
        let mut inner = self.shared.lock();
        let should_buffer = matches!(
            &inner.state,
            HttpSendMode::Inline { accepted } if *accepted < 2
        );
        if should_buffer {
            inner.queue.push_back(event);
            let HttpSendMode::Inline { accepted } = &mut inner.state else {
                unreachable!("inline admission cannot change state while locked")
            };
            *accepted += 1;
            drop(inner);
            self.shared.notify_ready();
            return HttpSendDisposition::Buffered;
        }
        if matches!(&inner.state, HttpSendMode::Inline { .. }) {
            {
                let (tx, rx) = mpsc::channel(HTTP_ASGI_QUEUE_CAPACITY);
                // A fresh bounded channel cannot be full. Keeping the first
                // two values in the inline FIFO preserves its cheap fast path;
                // this third accepted event is the first queue admission.
                tx.try_send(event)
                    .expect("a newly created outbound channel has capacity");
                inner.state = HttpSendMode::Streaming { tx, rx: Some(rx) };
                drop(inner);
                self.shared.notify_ready();
                return HttpSendDisposition::Sent;
            }
        }
        match &inner.state {
            HttpSendMode::Streaming { tx, .. } => match try_push(tx, event) {
                TryPush::Sent => HttpSendDisposition::Sent,
                TryPush::Full(event) => HttpSendDisposition::Backpressured {
                    tx: tx.clone(),
                    event,
                },
                TryPush::Closed(_) => HttpSendDisposition::Closed,
            },
            HttpSendMode::Inline { .. } => unreachable!("inline mode returned above"),
            HttpSendMode::Closed => HttpSendDisposition::Closed,
        }
    }
}

impl HttpSendBuffer {
    /// Reject new app sends and wake any sender currently waiting on a full
    /// streaming channel. Events accepted before closure remain available so
    /// the response driver can report their original ASGI contract error.
    pub(super) fn close_outbound(&mut self) {
        let mut inner = self.shared.lock();
        let state = std::mem::replace(&mut inner.state, HttpSendMode::Closed);
        if let HttpSendMode::Streaming { rx, .. } = state
            && self.stream_rx.is_none()
        {
            self.stream_rx = rx;
        }
        drop(inner);
        if let Some(rx) = &mut self.stream_rx {
            rx.close();
        }
        self.shared.notify_ready();
    }

    pub(super) fn take_ready(&mut self) -> Option<HttpOutboundEvent> {
        let mut inner = self.shared.lock();
        if let Some(event) = inner.queue.pop_front() {
            return Some(event);
        }

        if self.stream_rx.is_none()
            && let HttpSendMode::Streaming { rx, .. } = &mut inner.state
        {
            self.stream_rx = rx.take();
        }
        drop(inner);
        self.stream_rx.as_mut().and_then(|rx| rx.try_recv().ok())
    }

    pub(super) async fn wait_ready(&mut self) -> Option<HttpOutboundEvent> {
        loop {
            if let Some(event) = self.take_ready() {
                return Some(event);
            }
            if let Some(rx) = &mut self.stream_rx {
                return rx.recv().await;
            }
            self.shared.wait_ready().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{HttpSendDisposition, HttpSendState};
    use crate::bridge::{HTTP_ASGI_QUEUE_CAPACITY, HttpOutboundEvent, PayloadBytes};

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
    fn http_send_buffer_two_inline_then_bounded_channel() {
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(matches!(
            send_state.push_or_forward(body_event(b"first")),
            HttpSendDisposition::Buffered
        ));
        assert!(matches!(
            send_state.push_or_forward(body_event(b"second")),
            HttpSendDisposition::Buffered
        ));

        assert_body_event(
            send_buffer
                .take_ready()
                .expect("first buffered event is available"),
            b"first",
        );
        assert_body_event(
            send_buffer
                .take_ready()
                .expect("second buffered event is available"),
            b"second",
        );
        assert!(
            send_buffer.take_ready().is_none(),
            "only the inline FIFO is populated before the third send"
        );

        assert!(matches!(
            send_state.push_or_forward(body_event(b"third")),
            HttpSendDisposition::Sent
        ));
        for _ in 0..HTTP_ASGI_QUEUE_CAPACITY - 1 {
            assert!(matches!(
                send_state.push_or_forward(body_event(b"queued")),
                HttpSendDisposition::Sent
            ));
        }
        assert!(matches!(
            send_state.push_or_forward(body_event(b"backpressured")),
            HttpSendDisposition::Backpressured { .. }
        ));
        assert_body_event(
            send_buffer
                .take_ready()
                .expect("the streaming receiver owns the directly sent event"),
            b"third",
        );
    }

    #[test]
    fn a_third_event_starts_streaming_after_two_inline_events() {
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(matches!(
            send_state.push_or_forward(body_event(b"one")),
            HttpSendDisposition::Buffered
        ));
        assert!(matches!(
            send_state.push_or_forward(body_event(b"two")),
            HttpSendDisposition::Buffered
        ));
        assert!(matches!(
            send_state.push_or_forward(body_event(b"live")),
            HttpSendDisposition::Sent
        ));
        assert_body_event(
            send_buffer.take_ready().expect("first inline event"),
            b"one",
        );
        assert_body_event(
            send_buffer.take_ready().expect("second inline event"),
            b"two",
        );
        assert_body_event(
            send_buffer
                .take_ready()
                .expect("the streaming receiver owns the directly sent event"),
            b"live",
        );
    }

    #[test]
    fn streaming_sender_clones_only_after_the_channel_is_full() {
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(matches!(
            send_state.push_or_forward(body_event(b"one")),
            HttpSendDisposition::Buffered
        ));
        assert!(matches!(
            send_state.push_or_forward(body_event(b"two")),
            HttpSendDisposition::Buffered
        ));
        assert!(matches!(
            send_state.push_or_forward(body_event(b"three")),
            HttpSendDisposition::Sent
        ));
        assert_body_event(
            send_buffer.take_ready().expect("first inline event"),
            b"one",
        );
        assert_body_event(
            send_buffer.take_ready().expect("second inline event"),
            b"two",
        );
        assert_body_event(
            send_buffer.take_ready().expect("first streaming event"),
            b"three",
        );

        let internal_count = || {
            let inner = send_state.shared.lock();
            match &inner.state {
                super::HttpSendMode::Streaming { tx, .. } => tx.strong_count(),
                super::HttpSendMode::Inline { .. } | super::HttpSendMode::Closed => {
                    panic!("streaming mode was enabled")
                },
            }
        };
        assert_eq!(internal_count(), 1);
        for _ in 0..HTTP_ASGI_QUEUE_CAPACITY {
            assert!(matches!(
                send_state.push_or_forward(body_event(b"queued")),
                HttpSendDisposition::Sent
            ));
            assert_eq!(internal_count(), 1, "uncontended sends must not clone");
        }

        let HttpSendDisposition::Backpressured { tx, .. } =
            send_state.push_or_forward(body_event(b"waiting"))
        else {
            panic!("a full channel transfers one sender to the waiter")
        };
        assert_eq!(tx.strong_count(), 2);
        drop(tx);
        assert_eq!(internal_count(), 1);
    }

    #[tokio::test]
    async fn closing_streaming_output_wakes_a_blocked_sender_and_retains_accepted_events() {
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(matches!(
            send_state.push_or_forward(body_event(b"one")),
            HttpSendDisposition::Buffered
        ));
        assert!(matches!(
            send_state.push_or_forward(body_event(b"two")),
            HttpSendDisposition::Buffered
        ));
        assert!(matches!(
            send_state.push_or_forward(body_event(b"three")),
            HttpSendDisposition::Sent
        ));
        for expected in [b"one".as_slice(), b"two", b"three"] {
            assert_body_event(send_buffer.take_ready().expect("initial event"), expected);
        }

        for _ in 0..HTTP_ASGI_QUEUE_CAPACITY {
            assert!(matches!(
                send_state.push_or_forward(body_event(b"accepted")),
                HttpSendDisposition::Sent
            ));
        }
        let HttpSendDisposition::Backpressured { tx, event } =
            send_state.push_or_forward(body_event(b"blocked"))
        else {
            panic!("the full streaming channel backpressures one sender")
        };
        let blocked = tokio::spawn(async move { tx.send(event).await });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished());

        send_buffer.close_outbound();
        assert!(
            blocked.await.expect("blocked send task completes").is_err(),
            "receiver closure wakes the blocked send with SendAfterClose"
        );
        assert!(matches!(
            send_state.push_or_forward(body_event(b"late")),
            HttpSendDisposition::Closed
        ));

        for _ in 0..HTTP_ASGI_QUEUE_CAPACITY {
            assert_body_event(
                send_buffer
                    .take_ready()
                    .expect("an event accepted before close remains visible"),
                b"accepted",
            );
        }
        assert!(send_buffer.take_ready().is_none());
    }

    #[test]
    fn closing_buffered_output_retains_events_and_rejects_new_sends() {
        let (send_state, mut send_buffer) = HttpSendState::new();
        assert!(matches!(
            send_state.push_or_forward(body_event(b"first")),
            HttpSendDisposition::Buffered
        ));
        assert!(matches!(
            send_state.push_or_forward(body_event(b"second")),
            HttpSendDisposition::Buffered
        ));

        send_buffer.close_outbound();
        assert!(matches!(
            send_state.push_or_forward(body_event(b"late")),
            HttpSendDisposition::Closed
        ));
        assert_body_event(
            send_buffer.take_ready().expect("first retained event"),
            b"first",
        );
        assert_body_event(
            send_buffer.take_ready().expect("second retained event"),
            b"second",
        );
        assert!(send_buffer.take_ready().is_none());
    }
}
