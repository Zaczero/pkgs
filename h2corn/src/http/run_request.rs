use crate::access_log::HttpAccessLogState;
use crate::error::H2CornError;
use crate::http::app::{HttpRequestBody, run_asgi_http_request};
use crate::http::response::HttpResponseTransport;
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
    let result = run_asgi_http_request(ctx, request_body, admission, transport).await;
    access_log.emit_http_response(transport.response_log_state(), read_body_bytes);
    result
}
