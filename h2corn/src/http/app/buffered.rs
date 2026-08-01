use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::async_util::{TryPush, try_push};
use smallvec::SmallVec;

use crate::bridge::{HTTP_ASGI_QUEUE_CAPACITY, HttpOutboundEvent};
use crate::buffered_events::BufferedState;
use crate::http::response::{ResponseByteBudget, ResponseBytePermit};

enum HttpSendMode {
    Inline {
        accepted: u8,
    },
    Streaming {
        tx: mpsc::Sender<AdmittedHttpOutboundEvent>,
        rx: Option<mpsc::Receiver<AdmittedHttpOutboundEvent>>,
    },
    Closed,
}

/// Run once when the request finishes. Opaque so this layer stays free of
/// Python types; the bridge registers one that resolves an awaitable.
type FinishedWaiter = Box<dyn FnOnce() + Send>;

struct HttpSendControl {
    mode: HttpSendMode,
    close_signal: Option<watch::Sender<()>>,
    finished_waiters: SmallVec<[FinishedWaiter; 1]>,
}

/// An ASGI event that has already paid its connection-wide body credit. The
/// credit stays attached to the event through response lowering and into the
/// writer's pending DATA chunk.
#[derive(Debug)]
pub(crate) struct AdmittedHttpOutboundEvent {
    pub(crate) event: HttpOutboundEvent,
    pub(crate) credit: Option<ResponseBytePermit>,
}

impl AdmittedHttpOutboundEvent {
    pub(crate) const fn unbounded(event: HttpOutboundEvent) -> Self {
        Self {
            event,
            credit: None,
        }
    }

    pub(crate) const fn with_credit(
        event: HttpOutboundEvent,
        credit: Option<ResponseBytePermit>,
    ) -> Self {
        Self { event, credit }
    }

    fn into_event(self) -> HttpOutboundEvent {
        self.event
    }
}

/// Result of handing an ASGI event to the response driver. Only a full event
/// queue or byte ledger creates a waiter; uncontended sends keep no extra
/// sender or byte-accounting owner alive.
pub(crate) enum HttpSendDisposition {
    Buffered,
    Sent,
    Backpressured(HttpSendWaiter),
    Closed,
}

/// How an admitted event entered the shared state, decided under one lock.
enum Admission {
    /// Held in the inline FIFO; the consumer has been notified.
    Buffered,
    /// The first value of a channel this admission installed; notified.
    Started,
    /// Pushed into a channel that was already streaming.
    Sent,
    /// Streaming, but the channel was full. Only this case clones the sender:
    /// an uncontended send finishes under the lock, so the common path pays no
    /// refcount traffic. The caller decides whether to back-pressure or await.
    Full(
        mpsc::Sender<AdmittedHttpOutboundEvent>,
        AdmittedHttpOutboundEvent,
    ),
    Closed,
}

/// A send that could not enter the bounded event/byte admission path. It
/// acquires byte credit before awaiting a channel slot, so its body remains
/// accounted while the sender is suspended.
pub(crate) struct HttpSendWaiter {
    state: HttpSendState,
    event: HttpOutboundEvent,
}

#[derive(Clone)]
pub(crate) struct HttpSendState {
    shared: Arc<BufferedState<HttpSendControl, AdmittedHttpOutboundEvent, 2>>,
    budget: Option<ResponseByteBudget>,
}

pub(crate) struct HttpSendBuffer {
    shared: Arc<BufferedState<HttpSendControl, AdmittedHttpOutboundEvent, 2>>,
    stream_rx: Option<mpsc::Receiver<AdmittedHttpOutboundEvent>>,
}

impl HttpSendState {
    #[cfg(test)]
    pub(crate) fn new() -> (Self, HttpSendBuffer) {
        Self::unbounded()
    }

    pub(crate) fn unbounded() -> (Self, HttpSendBuffer) {
        Self::with_optional_response_budget(None)
    }

    pub(crate) fn with_response_budget(
        budget: ResponseByteBudget,
    ) -> (Self, HttpSendBuffer) {
        Self::with_optional_response_budget(Some(budget))
    }

    /// The state and its buffer are minted together and the buffer cannot be
    /// minted again: they share one inline queue and one streaming receiver,
    /// and a second buffer would both compete for those and close the outbound
    /// side out from under the first when it dropped.
    fn with_optional_response_budget(
        budget: Option<ResponseByteBudget>,
    ) -> (Self, HttpSendBuffer) {
        let shared = Arc::new(BufferedState::new(HttpSendControl {
            mode: HttpSendMode::Inline { accepted: 0 },
            close_signal: None,
            finished_waiters: SmallVec::new(),
        }));
        let buffer = HttpSendBuffer {
            shared: Arc::clone(&shared),
            stream_rx: None,
        };
        (Self { shared, budget }, buffer)
    }

    pub(crate) fn push_or_forward(&self, event: HttpOutboundEvent) -> HttpSendDisposition {
        let admitted = match self.try_admit(event) {
            Ok(event) => event,
            Err(event) => {
                return HttpSendDisposition::Backpressured(HttpSendWaiter {
                    state: self.clone(),
                    event,
                });
            },
        };
        self.try_forward(admitted)
    }

    /// Resolve once this request is finished — the response has been fully
    /// sent, or the transport tore the exchange down. The driver closes the
    /// outbound side at exactly that point.
    ///
    /// This is what ASGI's `http.disconnect` names. Running out of *request
    /// body* is a different event and does not belong here: a bodyless GET
    /// whose application calls `receive()` a second time is watching for the
    /// peer to leave, and answering immediately told it the peer had.
    pub(crate) fn on_request_finished(&self, waiter: impl FnOnce() + Send + 'static) {
        let mut inner = self.shared.lock();
        if matches!(inner.state.mode, HttpSendMode::Closed) {
            drop(inner);
            waiter();
            return;
        }
        inner.state.finished_waiters.push(Box::new(waiter));
    }

    fn try_admit(
        &self,
        event: HttpOutboundEvent,
    ) -> Result<AdmittedHttpOutboundEvent, HttpOutboundEvent> {
        let credit = match (&event, &self.budget) {
            (HttpOutboundEvent::Body { body, .. }, Some(budget)) => {
                match budget.try_acquire(body.len()) {
                    Ok(credit) => credit,
                    Err(_) => return Err(event),
                }
            },
            _ => None,
        };
        Ok(AdmittedHttpOutboundEvent { event, credit })
    }

    /// Admit an event into the shared state under one lock.
    ///
    /// The inline-to-streaming transition lives only here. Installing the
    /// channel and publishing the wakeup that hands its receiver to the
    /// consumer are one step, so no caller can perform one without the other —
    /// a blocking send that installed the channel silently and never notified
    /// left the consumer parked in `wait_ready` with the response half-written.
    fn admit(&self, event: AdmittedHttpOutboundEvent) -> Admission {
        let mut inner = self.shared.lock();
        let accepted = match &inner.state.mode {
            HttpSendMode::Inline { accepted } => *accepted,
            HttpSendMode::Streaming { tx, .. } => {
                return match try_push(tx, event) {
                    TryPush::Sent => Admission::Sent,
                    TryPush::Full(event) => Admission::Full(tx.clone(), event),
                    TryPush::Closed(_) => Admission::Closed,
                };
            },
            HttpSendMode::Closed => return Admission::Closed,
        };
        let admission = if accepted < 2 {
            inner.queue.push_back(event);
            inner.state.mode = HttpSendMode::Inline {
                accepted: accepted + 1,
            };
            Admission::Buffered
        } else {
            let (tx, rx) = mpsc::channel(HTTP_ASGI_QUEUE_CAPACITY);
            // A fresh bounded channel cannot be full. Keeping the first two
            // values in the inline FIFO preserves its cheap fast path; this
            // third accepted event is the first queue admission.
            tx.try_send(event)
                .expect("a newly created outbound channel has capacity");
            inner.state.mode = HttpSendMode::Streaming { tx, rx: Some(rx) };
            Admission::Started
        };
        drop(inner);
        self.shared.notify_ready();
        admission
    }

    fn try_forward(&self, event: AdmittedHttpOutboundEvent) -> HttpSendDisposition {
        match self.admit(event) {
            Admission::Buffered => HttpSendDisposition::Buffered,
            Admission::Started | Admission::Sent => HttpSendDisposition::Sent,
            Admission::Full(_, event) => HttpSendDisposition::Backpressured(HttpSendWaiter {
                state: self.clone(),
                event: event.into_event(),
            }),
            Admission::Closed => HttpSendDisposition::Closed,
        }
    }

    async fn admit_after_backpressure(
        &self,
        event: HttpOutboundEvent,
    ) -> Option<AdmittedHttpOutboundEvent> {
        let credit = match (&event, &self.budget) {
            (HttpOutboundEvent::Body { body, .. }, Some(budget)) => {
                if let Ok(credit) = budget.try_acquire(body.len()) {
                    credit
                } else {
                    let mut closed = {
                        let mut inner = self.shared.lock();
                        if matches!(&inner.state.mode, HttpSendMode::Closed) {
                            return None;
                        }
                        inner
                            .state
                            .close_signal
                            .get_or_insert_with(|| watch::channel(()).0)
                            .subscribe()
                    };
                    tokio::select! {
                        credit = budget.acquire(body.len()) => credit.ok()?,
                        changed = closed.changed() => {
                            let _ = changed;
                            return None;
                        }
                    }
                }
            },
            _ => None,
        };
        Some(AdmittedHttpOutboundEvent { event, credit })
    }

    async fn forward_after_admission(&self, event: AdmittedHttpOutboundEvent) -> bool {
        match self.admit(event) {
            Admission::Buffered | Admission::Started | Admission::Sent => true,
            Admission::Full(tx, event) => tx.send(event).await.is_ok(),
            Admission::Closed => false,
        }
    }
}

impl HttpSendWaiter {
    pub(crate) async fn send(self) -> bool {
        let Some(event) = self.state.admit_after_backpressure(self.event).await else {
            return false;
        };
        self.state.forward_after_admission(event).await
    }
}

impl HttpSendBuffer {
    /// Reject new app sends and wake any sender currently waiting on a full
    /// streaming channel. Events accepted before closure remain available so
    /// the response driver can report their original ASGI contract error.
    pub(super) fn close_outbound(&mut self) {
        let mut inner = self.shared.lock();
        let state = std::mem::replace(&mut inner.state.mode, HttpSendMode::Closed);
        let close_signal = inner.state.close_signal.take();
        let finished_waiters = std::mem::take(&mut inner.state.finished_waiters);
        if let HttpSendMode::Streaming { rx, .. } = state
            && self.stream_rx.is_none()
        {
            self.stream_rx = rx;
        }
        drop(inner);
        if let Some(rx) = &mut self.stream_rx {
            rx.close();
        }
        if let Some(close_signal) = close_signal {
            close_signal.send_replace(());
        }
        for waiter in finished_waiters {
            waiter();
        }
        self.shared.notify_ready();
    }

    pub(super) fn take_ready(&mut self) -> Option<AdmittedHttpOutboundEvent> {
        let mut inner = self.shared.lock();
        if let Some(event) = inner.queue.pop_front() {
            return Some(event);
        }

        if self.stream_rx.is_none()
            && let HttpSendMode::Streaming { rx, .. } = &mut inner.state.mode
        {
            self.stream_rx = rx.take();
        }
        drop(inner);
        self.stream_rx.as_mut().and_then(|rx| rx.try_recv().ok())
    }

    pub(super) async fn wait_ready(&mut self) -> Option<AdmittedHttpOutboundEvent> {
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

impl Drop for HttpSendBuffer {
    fn drop(&mut self) {
        self.close_outbound();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::time::{Duration, timeout};

    use super::{HttpSendDisposition, HttpSendState};
    use crate::bridge::{HTTP_ASGI_QUEUE_CAPACITY, HttpOutboundEvent, PayloadBytes};
    use crate::http::response::ResponseByteBudget;

    fn body_event(body: &'static [u8]) -> HttpOutboundEvent {
        HttpOutboundEvent::Body {
            body: PayloadBytes::from(Bytes::from_static(body)),
            more_body: true,
        }
    }

    fn assert_body_event(event: super::AdmittedHttpOutboundEvent, expected: &[u8]) {
        match event.event {
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
            HttpSendDisposition::Backpressured(_)
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

    /// No send retains a sender clone — not an uncontended one, and not a
    /// backpressure waiter.
    ///
    /// This counts what is *held*, which a clone made and dropped inside one
    /// call would not show. That the uncontended path makes no clone at all is
    /// structural instead: `admit` pushes under the lock it already holds and
    /// clones only on the `Full` arm.
    #[test]
    fn no_send_retains_a_streaming_sender_clone() {
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
            match &inner.state.mode {
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
            assert_eq!(internal_count(), 1, "an uncontended send retains no clone");
        }

        let HttpSendDisposition::Backpressured(waiter) =
            send_state.push_or_forward(body_event(b"waiting"))
        else {
            panic!("a full channel creates a backpressure waiter")
        };
        assert_eq!(internal_count(), 1, "the waiter owns no sender clone");
        drop(waiter);
        assert_eq!(internal_count(), 1);
    }

    #[tokio::test]
    async fn a_byte_blocked_send_wakes_the_consumer_when_it_starts_streaming() {
        // The inline FIFO holds two events, so the third installs the
        // streaming channel. Reaching that transition from the byte-budget
        // waiter — rather than from an uncontended send — is the case that
        // used to install the channel without publishing a wakeup, parking
        // the consumer in `wait_ready` forever while the app stayed alive.
        let budget = ResponseByteBudget::new(4);
        let (send_state, mut send_buffer) = HttpSendState::with_response_budget(budget);
        for body in [b"aa", b"bb"] {
            assert!(matches!(
                send_state.push_or_forward(body_event(body)),
                HttpSendDisposition::Buffered
            ));
        }

        let HttpSendDisposition::Backpressured(waiter) =
            send_state.push_or_forward(body_event(b"cc"))
        else {
            panic!("the third body waits for the exhausted byte budget")
        };
        let blocked = tokio::spawn(waiter.send());

        // Draining returns the credit the waiter needs, so it proceeds into
        // the inline-to-streaming transition.
        let first = send_buffer.take_ready().expect("the first inline event");
        drop(first);
        let second = send_buffer.take_ready().expect("the second inline event");
        drop(second);

        let delivered = timeout(Duration::from_secs(5), send_buffer.wait_ready())
            .await
            .expect("installing the streaming channel wakes the waiting consumer");
        assert!(delivered.is_some(), "the third event reaches the consumer");
        assert!(
            timeout(Duration::from_secs(5), blocked)
                .await
                .expect("the byte waiter completes")
                .expect("the byte waiter task does not panic"),
            "the send succeeds once credit is available"
        );
    }

    #[test]
    fn unbounded_sends_never_arm_a_close_signal() {
        let (send_state, _send_buffer) = HttpSendState::new();
        assert!(
            send_state.shared.lock().state.close_signal.is_none(),
            "unbounded HTTP/1 sends have no byte waiter to wake"
        );
    }

    #[tokio::test]
    async fn closing_before_a_byte_waiter_arms_observes_closed() {
        let budget = ResponseByteBudget::new(1);
        let (send_state, mut send_buffer) = HttpSendState::with_response_budget(budget);
        assert!(matches!(
            send_state.push_or_forward(body_event(b"a")),
            HttpSendDisposition::Buffered
        ));
        let _retained = send_buffer
            .take_ready()
            .expect("the budget-filling body is admitted");
        let HttpSendDisposition::Backpressured(waiter) =
            send_state.push_or_forward(body_event(b"b"))
        else {
            panic!("the second body waits for byte credit")
        };

        send_buffer.close_outbound();
        assert!(
            send_state.shared.lock().state.close_signal.is_none(),
            "closing before the wait leaves no signal to allocate"
        );
        assert!(
            !timeout(Duration::from_secs(1), waiter.send())
                .await
                .expect("a waiter started after close returns immediately"),
            "a closed request does not acquire a byte permit"
        );
    }

    #[tokio::test]
    async fn closing_after_arming_wakes_all_byte_waiters() {
        let budget = ResponseByteBudget::new(1);
        let (send_state, mut send_buffer) = HttpSendState::with_response_budget(budget);
        assert!(matches!(
            send_state.push_or_forward(body_event(b"a")),
            HttpSendDisposition::Buffered
        ));
        let _retained = send_buffer
            .take_ready()
            .expect("the budget-filling body is admitted");

        let mut blocked = Vec::new();
        for body in [b"b", b"c", b"d"] {
            let HttpSendDisposition::Backpressured(waiter) =
                send_state.push_or_forward(body_event(body))
            else {
                panic!("every body waits for the exhausted byte budget")
            };
            blocked.push(tokio::spawn(waiter.send()));
        }
        tokio::task::yield_now().await;
        assert!(
            send_state.shared.lock().state.close_signal.is_some(),
            "the first blocked byte acquisition arms the shared close signal"
        );

        send_buffer.close_outbound();
        for waiter in blocked {
            assert!(
                !timeout(Duration::from_secs(1), waiter)
                    .await
                    .expect("closure wakes every byte waiter")
                    .expect("waiter task joins"),
                "a byte waiter cannot send after close"
            );
        }
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
        let HttpSendDisposition::Backpressured(waiter) =
            send_state.push_or_forward(body_event(b"blocked"))
        else {
            panic!("the full streaming channel backpressures one sender")
        };
        let blocked = tokio::spawn(async move { waiter.send().await });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished());

        send_buffer.close_outbound();
        assert!(
            !timeout(Duration::from_secs(1), blocked)
                .await
                .expect("closing a full streaming output wakes the blocked sender")
                .expect("blocked send task completes"),
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

    #[tokio::test]
    async fn response_byte_credit_blocks_until_written_bytes_are_released() {
        let budget = ResponseByteBudget::new(64 * 1024);
        let (send_state, mut send_buffer) = HttpSendState::with_response_budget(budget);

        assert!(matches!(
            send_state.push_or_forward(HttpOutboundEvent::Body {
                body: PayloadBytes::from(Bytes::from(vec![b'a'; 64 * 1024])),
                more_body: true,
            }),
            HttpSendDisposition::Buffered
        ));
        let mut first = send_buffer
            .take_ready()
            .expect("the budget-filling body is admitted");
        let HttpSendDisposition::Backpressured(waiter) =
            send_state.push_or_forward(HttpOutboundEvent::Body {
                body: PayloadBytes::from(Bytes::from(vec![b'b'; 64 * 1024])),
                more_body: true,
            })
        else {
            panic!("the next body must wait behind a full byte budget")
        };

        let blocked = tokio::spawn(waiter.send());
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished(), "no byte credit is available yet");
        assert!(
            send_state.shared.lock().state.close_signal.is_some(),
            "a blocked byte acquisition arms the close signal"
        );

        first
            .credit
            .as_mut()
            .expect("body carries its response byte credit")
            .release_written(64 * 1024);
        assert!(
            timeout(Duration::from_secs(1), blocked)
                .await
                .expect("one released DATA frame wakes one sender")
                .expect("sender task joins"),
            "the released credit admits exactly the waiting send"
        );
    }

    #[tokio::test]
    async fn one_oversized_body_occupies_the_full_response_budget() {
        let budget = ResponseByteBudget::new(64 * 1024);
        let (send_state, mut send_buffer) = HttpSendState::with_response_budget(budget);

        assert!(matches!(
            send_state.push_or_forward(HttpOutboundEvent::Body {
                body: PayloadBytes::from(Bytes::from(vec![b'a'; 128 * 1024])),
                more_body: true,
            }),
            HttpSendDisposition::Buffered
        ));
        let first = send_buffer
            .take_ready()
            .expect("one oversized event is admitted");
        assert!(matches!(
            send_state.push_or_forward(body_event(b"later")),
            HttpSendDisposition::Backpressured(_)
        ));
        drop(first);
        assert_eq!(
            send_state
                .budget
                .as_ref()
                .expect("bounded state retains its byte budget")
                .available(),
            64 * 1024
        );
    }

    #[tokio::test]
    async fn dropping_a_request_buffer_wakes_a_byte_blocked_send() {
        let budget = ResponseByteBudget::new(64);
        let (send_state, mut send_buffer) = HttpSendState::with_response_budget(budget);
        assert!(matches!(
            send_state.push_or_forward(HttpOutboundEvent::Body {
                body: PayloadBytes::from(Bytes::from(vec![b'a'; 64])),
                more_body: true,
            }),
            HttpSendDisposition::Buffered
        ));
        let _retained = send_buffer
            .take_ready()
            .expect("the first body keeps the full budget");
        let HttpSendDisposition::Backpressured(waiter) =
            send_state.push_or_forward(HttpOutboundEvent::Body {
                body: PayloadBytes::from(Bytes::from_static(b"next")),
                more_body: true,
            })
        else {
            panic!("the second body waits for byte credit")
        };
        let blocked = tokio::spawn(waiter.send());
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished());
        assert!(send_state.shared.lock().state.close_signal.is_some());

        drop(send_buffer);
        assert!(
            !timeout(Duration::from_secs(1), blocked)
                .await
                .expect("connection teardown wakes byte waiter")
                .expect("waiter task joins"),
            "a torn-down request must not leave an ASGI send waiting for credit"
        );
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
