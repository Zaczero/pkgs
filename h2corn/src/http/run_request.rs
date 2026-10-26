use crate::error::H2CornError;
use crate::http::app::{HttpRequestBody, run_asgi_http_request};
use crate::http::response::HttpResponseTransport;
use crate::log::HttpAccessLogState;
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
    F: Fn() -> u64,
{
    let mut access_log = HttpAccessLogState::new(&ctx, read_body_bytes);
    run_asgi_http_request(
        ctx,
        request_body,
        admission,
        transport,
        &mut access_log.response,
    )
    .await
}
