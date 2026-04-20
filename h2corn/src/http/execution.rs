use std::sync::{Arc, atomic::AtomicU64};

use tokio::sync::mpsc;

use crate::bridge::ASGI_QUEUE_CAPACITY;
use crate::http::planner::{RequestInputPlan, RequestLaunchPlan, RequestRoute};
use crate::runtime::{AppState, RequestAdmission, StreamInput, try_acquire_request_admission};

pub(crate) struct RequestInputChannels {
    pub(crate) tx: Option<mpsc::Sender<StreamInput>>,
    pub(crate) rx: Option<mpsc::Receiver<StreamInput>>,
    pub(crate) body_bytes_read: Option<Arc<AtomicU64>>,
}

pub(crate) struct RequestExecution<WebSocketMeta> {
    pub(crate) route: RequestRoute<WebSocketMeta>,
    pub(crate) admission: RequestAdmission,
    pub(crate) input: RequestInputChannels,
}

pub(crate) fn prepare_request_input(input: RequestInputPlan) -> RequestInputChannels {
    match input {
        RequestInputPlan::None => RequestInputChannels {
            tx: None,
            rx: None,
            body_bytes_read: None,
        },
        RequestInputPlan::Stream { count_body_bytes } => {
            let (tx, rx) = mpsc::channel(ASGI_QUEUE_CAPACITY);
            RequestInputChannels {
                tx: Some(tx),
                rx: Some(rx),
                body_bytes_read: count_body_bytes.then(|| Arc::new(AtomicU64::new(0))),
            }
        }
    }
}

pub(crate) fn prepare_request_execution<WebSocketMeta>(
    app: &AppState,
    plan: RequestLaunchPlan<WebSocketMeta>,
) -> Option<RequestExecution<WebSocketMeta>> {
    let RequestLaunchPlan { route, input } = plan;
    let admission = try_acquire_request_admission(app)?;
    let input = prepare_request_input(input);
    Some(RequestExecution {
        route,
        admission,
        input,
    })
}

#[cfg(test)]
mod tests {
    use super::{RequestInputChannels, prepare_request_input};
    use crate::http::planner::RequestInputPlan;

    fn assert_empty_input(input: RequestInputChannels) {
        assert!(input.tx.is_none());
        assert!(input.rx.is_none());
        assert!(input.body_bytes_read.is_none());
    }

    #[test]
    fn no_body_http_requests_do_not_allocate_channels() {
        assert_empty_input(prepare_request_input(RequestInputPlan::None));
    }

    #[test]
    fn streaming_http_requests_allocate_channels_and_optional_counter() {
        let input = prepare_request_input(RequestInputPlan::Stream {
            count_body_bytes: true,
        });

        assert!(input.tx.is_some());
        assert!(input.rx.is_some());
        assert!(input.body_bytes_read.is_some());
    }

    #[test]
    fn websocket_requests_allocate_channels_without_counter() {
        let input = prepare_request_input(RequestInputPlan::Stream {
            count_body_bytes: false,
        });

        assert!(input.tx.is_some());
        assert!(input.rx.is_some());
        assert!(input.body_bytes_read.is_none());
    }
}
