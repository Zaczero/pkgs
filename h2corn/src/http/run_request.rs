use crate::error::H2CornError;
use crate::http::app::{HttpRequestBody, run_asgi_http_request};
use crate::http::response::HttpResponseTransport;
use crate::log::{HttpAccessLogState, ResponseLogState};
use crate::runtime::{RequestAdmission, RequestContext};

pub(crate) async fn run_http_request<T, F>(
    ctx: Box<RequestContext>,
    request_body: HttpRequestBody,
    admission: RequestAdmission,
    transport: &mut T,
    read_body_bytes: F,
) -> Result<(), H2CornError>
where
    T: HttpResponseTransport,
    F: FnOnce() -> u64,
{
    let access_log = HttpAccessLogState::new(&ctx);
    let mut response_log = ResponseLogState::default();
    let result =
        run_asgi_http_request(ctx, request_body, admission, transport, &mut response_log).await;
    access_log.emit_http_response(response_log, read_body_bytes);
    result
}
