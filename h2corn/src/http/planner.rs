use std::num::NonZeroU64;

use crate::http::types::{HttpStatusCode, RequestHead, ResponseHeaders, status_code};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RequestRoute<WebSocketMeta> {
    Http,
    WebSocket(WebSocketMeta),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestInputPlan {
    None,
    Stream { count_body_bytes: bool },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestLaunchPlan<WebSocketMeta> {
    pub(crate) route: RequestRoute<WebSocketMeta>,
    pub(crate) input: RequestInputPlan,
}

#[derive(Debug)]
pub(crate) struct RequestRejection {
    pub(crate) status: HttpStatusCode,
    pub(crate) headers: ResponseHeaders,
}

impl RequestRejection {
    pub(crate) fn payload_too_large() -> Self {
        Self {
            status: status_code::PAYLOAD_TOO_LARGE,
            headers: ResponseHeaders::new(),
        }
    }

    pub(crate) fn not_implemented() -> Self {
        Self {
            status: status_code::NOT_IMPLEMENTED,
            headers: ResponseHeaders::new(),
        }
    }
}

pub(crate) fn reject_oversized_request(
    request: &RequestHead,
    max_request_body_size: Option<NonZeroU64>,
) -> Result<(), RequestRejection> {
    if max_request_body_size.is_some_and(|limit| {
        request
            .content_length()
            .is_some_and(|len| len > limit.get())
    }) {
        Err(RequestRejection::payload_too_large())
    } else {
        Ok(())
    }
}

pub(crate) fn plan_request_input<WebSocketMeta>(
    route: &RequestRoute<WebSocketMeta>,
    input_finished: bool,
    access_log: bool,
) -> RequestInputPlan {
    match route {
        RequestRoute::WebSocket(_) => RequestInputPlan::Stream {
            count_body_bytes: false,
        },
        RequestRoute::Http if input_finished => RequestInputPlan::None,
        RequestRoute::Http => RequestInputPlan::Stream {
            count_body_bytes: access_log,
        },
    }
}

pub(crate) fn plan_request<WebSocketMeta>(
    request: &RequestHead,
    websocket: Option<Result<WebSocketMeta, RequestRejection>>,
    input_finished: bool,
    access_log: bool,
    max_request_body_size: Option<NonZeroU64>,
) -> Result<RequestLaunchPlan<WebSocketMeta>, RequestRejection> {
    reject_oversized_request(request, max_request_body_size)?;

    let route = match websocket {
        Some(Ok(meta)) => RequestRoute::WebSocket(meta),
        Some(Err(rejection)) => return Err(rejection),
        None if request.is_connect() => return Err(RequestRejection::not_implemented()),
        None => RequestRoute::Http,
    };

    Ok(RequestLaunchPlan {
        input: plan_request_input(&route, input_finished, access_log),
        route,
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use http::Method;

    use super::{
        RequestInputPlan, RequestRoute, plan_request, plan_request_input, reject_oversized_request,
    };
    use crate::ext::Protocol;
    use crate::hpack::BytesStr;
    use crate::http::header_meta::RequestHeaderMeta;
    use crate::http::types::{
        HttpVersion, RequestAuthority, RequestHead, RequestTarget, status_code,
    };

    fn http_request(content_length: Option<u64>) -> RequestHead {
        RequestHead {
            http_version: HttpVersion::Http1_1,
            method: Method::GET,
            target: RequestTarget::normal(
                BytesStr::from_static("http"),
                BytesStr::from_static("/demo"),
            ),
            headers: Vec::new(),
            header_meta: RequestHeaderMeta {
                content_length,
                ..RequestHeaderMeta::default()
            },
        }
    }

    fn websocket_request() -> RequestHead {
        RequestHead {
            http_version: HttpVersion::Http2,
            method: Method::CONNECT,
            target: RequestTarget::extended_connect(
                RequestAuthority::new(BytesStr::from_static("example.com:443")),
                Protocol::from("websocket"),
                BytesStr::from_static("https"),
                BytesStr::from_static("/chat"),
            ),
            headers: Vec::new(),
            header_meta: RequestHeaderMeta::default(),
        }
    }

    fn connect_request() -> RequestHead {
        RequestHead {
            http_version: HttpVersion::Http2,
            method: Method::CONNECT,
            target: RequestTarget::connect(RequestAuthority::new(BytesStr::from_static(
                "example.com:443",
            ))),
            headers: Vec::new(),
            header_meta: RequestHeaderMeta::default(),
        }
    }

    #[test]
    fn oversized_requests_are_rejected() {
        let request = http_request(Some(11));
        let rejection = reject_oversized_request(&request, NonZeroU64::new(10))
            .expect_err("oversized requests are rejected");

        assert_eq!(rejection.status, status_code::PAYLOAD_TOO_LARGE);
        assert!(rejection.headers.is_empty());
    }

    #[test]
    fn http_input_without_body_is_none() {
        assert_eq!(
            plan_request_input(&RequestRoute::<()>::Http, true, true),
            RequestInputPlan::None
        );
    }

    #[test]
    fn http_stream_input_uses_access_log_policy() {
        assert_eq!(
            plan_request_input(&RequestRoute::<()>::Http, false, true),
            RequestInputPlan::Stream {
                count_body_bytes: true
            }
        );
        assert_eq!(
            plan_request_input(&RequestRoute::<()>::Http, false, false),
            RequestInputPlan::Stream {
                count_body_bytes: false
            }
        );
    }

    #[test]
    fn websocket_requests_always_expect_stream_input() {
        let request = websocket_request();
        let plan = plan_request(&request, Some(Ok("meta")), true, true, None)
            .expect("websocket request is accepted");

        assert_eq!(plan.route, RequestRoute::WebSocket("meta"));
        assert_eq!(
            plan.input,
            RequestInputPlan::Stream {
                count_body_bytes: false
            }
        );
    }

    #[test]
    fn plain_connect_is_rejected() {
        let request = connect_request();
        let rejection = plan_request::<()>(&request, None, true, false, None)
            .expect_err("plain CONNECT is not implemented");

        assert_eq!(rejection.status, status_code::NOT_IMPLEMENTED);
        assert!(rejection.headers.is_empty());
    }
}
