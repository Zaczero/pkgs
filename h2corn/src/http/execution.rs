use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tokio::sync::mpsc;

use crate::bridge::ASGI_QUEUE_CAPACITY;
use crate::http::planner::{RequestInputPlan, RequestLaunchPlan};
use crate::runtime::{AppState, RequestAdmission, StreamInput, try_acquire_request_admission};

/// Allocated input channels for a request whose body streams to the app.
pub(crate) struct StreamRequestInput {
    pub(crate) tx: mpsc::Sender<StreamInput>,
    pub(crate) rx: mpsc::Receiver<StreamInput>,
    pub(crate) body_bytes_read: Option<Arc<AtomicU64>>,
}

impl StreamRequestInput {
    pub(crate) fn new(count_body_bytes: bool) -> Self {
        let (tx, rx) = mpsc::channel(ASGI_QUEUE_CAPACITY);
        Self {
            tx,
            rx,
            body_bytes_read: count_body_bytes.then(|| Arc::new(AtomicU64::new(0))),
        }
    }
}

pub(crate) enum PreparedRequestInput {
    None,
    Stream(StreamRequestInput),
}

impl PreparedRequestInput {
    fn prepare(plan: RequestInputPlan) -> Self {
        match plan {
            RequestInputPlan::None => Self::None,
            RequestInputPlan::Stream { count_body_bytes } => {
                Self::Stream(StreamRequestInput::new(count_body_bytes))
            },
        }
    }

    /// Split into the sender that drives the inbound stream and the receiver
    /// the app reads. The sender is the channel's sole producer, so dropping
    /// the stream closes the app's `receive()` (delivering disconnect); the
    /// app side never holds a sender clone.
    pub(crate) fn split(self) -> (Option<mpsc::Sender<StreamInput>>, AppRequestInput) {
        match self {
            Self::None => (None, AppRequestInput::None),
            Self::Stream(StreamRequestInput {
                tx,
                rx,
                body_bytes_read,
            }) => (Some(tx), AppRequestInput::Stream {
                rx,
                body_bytes_read,
            }),
        }
    }
}

/// The app-side half of a prepared request input: receiver plus body counter,
/// never a sender.
pub(crate) enum AppRequestInput {
    None,
    Stream {
        rx: mpsc::Receiver<StreamInput>,
        body_bytes_read: Option<Arc<AtomicU64>>,
    },
}

impl AppRequestInput {
    /// The shared body-byte counter the inbound stream increments and the
    /// access log reads (a refcount clone, not a sender).
    pub(crate) fn body_bytes_read(&self) -> Option<Arc<AtomicU64>> {
        match self {
            Self::None => None,
            Self::Stream {
                body_bytes_read, ..
            } => body_bytes_read.clone(),
        }
    }
}

/// Admitted request, ready to launch; the route carries exactly the input
/// shape it requires, so launch sites never re-check channel presence.
pub(crate) enum RequestExecution<WebSocketMeta> {
    Http {
        admission: RequestAdmission,
        input: PreparedRequestInput,
    },
    WebSocket {
        meta: WebSocketMeta,
        admission: RequestAdmission,
        input: StreamRequestInput,
    },
}

pub(crate) fn prepare_request_execution<WebSocketMeta>(
    app: AppState,
    plan: RequestLaunchPlan<WebSocketMeta>,
) -> Option<RequestExecution<WebSocketMeta>> {
    let admission = try_acquire_request_admission(app)?;
    Some(match plan {
        RequestLaunchPlan::Http { input } => RequestExecution::Http {
            admission,
            input: PreparedRequestInput::prepare(input),
        },
        RequestLaunchPlan::WebSocket { meta } => RequestExecution::WebSocket {
            meta,
            admission,
            input: StreamRequestInput::new(false),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{AppRequestInput, PreparedRequestInput, StreamRequestInput};
    use crate::http::planner::RequestInputPlan;

    #[test]
    fn no_body_http_requests_do_not_allocate_channels() {
        let input = PreparedRequestInput::prepare(RequestInputPlan::None);
        let (tx, app_input) = input.split();
        assert!(tx.is_none());
        assert!(matches!(app_input, AppRequestInput::None));
        assert!(app_input.body_bytes_read().is_none());
    }

    #[test]
    fn streaming_split_yields_one_sender_and_app_receiver() {
        let input = PreparedRequestInput::prepare(RequestInputPlan::Stream {
            count_body_bytes: true,
        });
        let (tx, app_input) = input.split();
        let tx = tx.expect("streaming input keeps the sole sender for the stream");
        assert_eq!(
            tx.strong_count(),
            1,
            "app side must not hold a sender clone"
        );
        assert!(matches!(app_input, AppRequestInput::Stream { .. }));
        assert!(app_input.body_bytes_read().is_some());
    }

    #[test]
    fn streaming_http_requests_allocate_channels_and_optional_counter() {
        let input = StreamRequestInput::new(true);
        assert!(input.body_bytes_read.is_some());
    }

    #[test]
    fn websocket_requests_allocate_channels_without_counter() {
        let input = StreamRequestInput::new(false);
        assert!(input.body_bytes_read.is_none());
    }
}
