use std::num::{NonZeroU32, NonZeroU64};
use std::sync::LazyLock;
use std::time::Duration;

use bytes::BytesMut;
use http::Method;
use tokio::io::{AsyncWriteExt as _, BufWriter, duplex, sink};
use tokio::spawn;
use tokio::sync::mpsc;

use super::{
    ChunkDeliveryBatch, HeadLineCursor, HeadParseOutcome, MAX_CHUNK_SIZE_LINE_BYTES,
    MAX_TRAILER_SECTION_BYTES, drain_chunked_trailers, parse_chunk_size, parse_http2_settings,
    read_chunk_size_line, read_chunked_body, read_request, read_request_head,
};
use crate::config::{
    BindTarget, ForwardedFields, Http1Config, Http2Config, LogFormat, ProxyConfig,
    ResponseHeaderConfig, ServerConfig, WebSocketConfig,
};
use crate::error::{ErrorKind, H2CornError, Http1Error};
use crate::h1::{ConnectionPersistence, RequestBodyKind, RequestRoute, UpgradeRequest};
use crate::h2_frame;
use crate::http::body::RequestBodyState;
use crate::http::types::RequestAuthority;
use crate::proxy_protocol::ProxyProtocolMode;
use crate::runtime::StreamInput;

fn test_server_config() -> &'static ServerConfig {
    static CONFIG: LazyLock<ServerConfig> = LazyLock::new(|| ServerConfig {
        binds: Box::new([BindTarget::Tcp {
            host: Box::from("127.0.0.1"),
            port: 8000,
        }]),
        access_log: false,
        log_format: LogFormat::Text,
        root_path: Box::from(""),
        root_path_scope: crate::python::PyOnceLock::new(),
        limit_request_fields: None,
        http1: Http1Config {
            enabled: true,
            ..Default::default()
        },
        http2: Http2Config {
            max_concurrent_streams: NonZeroU32::new(8).expect("non-zero"),
            max_header_list_size: None,
            max_header_block_size: None,
            max_inbound_frame_size: NonZeroU32::new(h2_frame::DEFAULT_MAX_FRAME_SIZE as u32)
                .expect("default HTTP/2 frame size is non-zero"),
            initial_stream_window_size: NonZeroU32::new(1 << 20).expect("non-zero"),
            initial_connection_window_size: NonZeroU32::new(2 << 20).expect("non-zero"),
            timeout_response_stall: None,
        },
        max_request_body_size: None,
        timeout_graceful_shutdown: Duration::from_secs(30),
        timeout_keep_alive: None,
        timeout_request_header: None,
        timeout_request_body_idle: None,
        limit_concurrency: None,
        limit_connections: None,
        max_requests: None,
        runtime_threads: 2,
        loop_threads: 1,
        websocket: WebSocketConfig::default(),
        proxy: ProxyConfig {
            trust_headers: false,
            trusted_peers: Box::new([]),
            forwarded_fields: ForwardedFields::default(),
            protocol: ProxyProtocolMode::Off,
        },
        tls: None,
        timeout_handshake: Some(Duration::from_secs(5)),
        response_headers: ResponseHeaderConfig::default(),
    });
    &CONFIG
}

async fn read_test_request(request: &[u8]) -> Result<Option<super::ParsedRequest>, H2CornError> {
    let (mut client, mut server) = duplex(512);
    let mut writer = BufWriter::new(sink());
    let request = request.to_vec();
    let write_task = spawn(async move {
        client
            .write_all(&request)
            .await
            .expect("request write succeeds");
        client.shutdown().await.expect("request shutdown succeeds");
    });

    let parsed = read_request(
        &mut server,
        &mut BytesMut::new(),
        &mut writer,
        test_server_config(),
        "http",
        None,
        true,
        false,
    )
    .await;
    write_task.await.expect("writer task finishes");
    parsed
}

async fn parse_test_request(request: &[u8]) -> super::ParsedRequest {
    read_test_request(request)
        .await
        .expect("request parse succeeds")
        .expect("request is present")
}

#[tokio::test]
async fn untrusted_proxy_header_reaches_the_asgi_header_list_without_metadata() {
    let parsed = parse_test_request(
        b"GET / HTTP/1.1\r\nHost: example.com\r\nX-Forwarded-For: 198.51.100.9\r\n\r\n",
    )
    .await;

    assert!(
        parsed
            .request
            .headers
            .iter()
            .any(|header| header.value() == b"198.51.100.9")
    );
    assert!(parsed.request.header_meta.proxy_headers().is_none());
}

const fn unlimited_fields() -> super::FieldLimits {
    super::FieldLimits {
        max_fields: None,
        max_field_size: None,
    }
}

fn base64url_encode(src: &[u8]) -> Vec<u8> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    let mut out = Vec::with_capacity(src.len().div_ceil(3).saturating_mul(4));
    let (chunks, remainder) = src.as_chunks::<3>();
    for &[c0, c1, c2] in chunks {
        out.push(TABLE[usize::from(c0 >> 2)]);
        out.push(TABLE[usize::from(((c0 & 0x03) << 4) | (c1 >> 4))]);
        out.push(TABLE[usize::from(((c1 & 0x0F) << 2) | (c2 >> 6))]);
        out.push(TABLE[usize::from(c2 & 0x3F)]);
    }

    match remainder {
        &[a] => {
            out.push(TABLE[usize::from(a >> 2)]);
            out.push(TABLE[usize::from((a & 0x03) << 4)]);
        },
        &[a, b] => {
            out.push(TABLE[usize::from(a >> 2)]);
            out.push(TABLE[usize::from(((a & 0x03) << 4) | (b >> 4))]);
            out.push(TABLE[usize::from((b & 0x0F) << 2)]);
        },
        [] => {},
        _ => unreachable!("remainder from as_chunks::<3>() is at most 2 bytes"),
    }

    out
}

#[test]
fn parse_http2_settings_rejects_zero_max_frame_size() {
    let encoded = base64url_encode(&[0x00, 0x05, 0x00, 0x00, 0x00, 0x00]);
    let err = parse_http2_settings(&encoded).unwrap_err();
    assert_eq!(err.to_string(), "invalid SETTINGS_MAX_FRAME_SIZE value");
}

#[tokio::test]
async fn request_head_with_an_empty_field_section_is_accepted() {
    let (mut client, mut server) = duplex(64);
    client
        .write_all(b"GET / HTTP/1.1\r\n\r\n")
        .await
        .expect("request write succeeds");
    client.shutdown().await.expect("request shutdown succeeds");
    let mut writer = BufWriter::new(sink());
    let head = read_request_head(
        &mut server,
        &mut BytesMut::new(),
        &mut writer,
        test_server_config(),
        None,
        true,
        None,
    )
    .await
    .expect("request head reads")
    .expect("request head is present");

    let mut lines = HeadLineCursor::new(&head);
    let HeadParseOutcome::Parsed(Some(request_line)) = lines.next_line() else {
        panic!("request line is available");
    };
    assert_eq!(request_line, b"GET / HTTP/1.1");
    assert!(matches!(lines.next_line(), HeadParseOutcome::Parsed(None)));
}

#[tokio::test]
async fn read_request_accepts_a_head_split_mid_crlf() {
    let (mut client, mut server) = duplex(1);
    let write_task = spawn(async move {
        for chunk in [
            b"GET / HTTP/1.1\r".as_slice(),
            b"\nHost: example.com\r".as_slice(),
            b"\n\r".as_slice(),
            b"\n".as_slice(),
        ] {
            client
                .write_all(chunk)
                .await
                .expect("request write succeeds");
        }
        client.shutdown().await.expect("request shutdown succeeds");
    });

    let mut writer = BufWriter::new(sink());
    let parsed = read_request(
        &mut server,
        &mut BytesMut::new(),
        &mut writer,
        test_server_config(),
        "http",
        None,
        true,
        false,
    )
    .await;
    write_task.await.expect("writer task finishes");

    assert!(parsed.expect("request parse succeeds").is_some());
}

#[tokio::test]
async fn request_head_timeouts_distinguish_idle_keep_alive_from_partial_headers() {
    let (_client, mut server) = duplex(64);
    let mut writer = BufWriter::new(sink());
    let mut buffer = BytesMut::new();
    let Err(idle) = read_request(
        &mut server,
        &mut buffer,
        &mut writer,
        test_server_config(),
        "http",
        Some(Duration::ZERO),
        false,
        false,
    )
    .await
    else {
        panic!("an idle keep-alive connection times out");
    };
    assert_eq!(
        idle.to_string(),
        "keep-alive connection did not receive the next request within timeout_keep_alive"
    );

    let (_client, mut server) = duplex(64);
    let mut writer = BufWriter::new(sink());
    let mut buffer = BytesMut::from(&b"GET"[..]);
    let Err(partial) = read_request(
        &mut server,
        &mut buffer,
        &mut writer,
        test_server_config(),
        "http",
        Some(Duration::ZERO),
        false,
        false,
    )
    .await
    else {
        panic!("a partial request head times out");
    };
    assert_eq!(
        partial.to_string(),
        "HTTP/1.1 request head did not arrive within timeout_request_header"
    );
}

#[tokio::test]
async fn read_request_accepts_extension_method() {
    let parsed = parse_test_request(b"PROPFIND /items HTTP/1.1\r\nHost: example.com\r\n\r\n").await;

    assert_eq!(
        parsed.request.method,
        Method::from_bytes(b"PROPFIND").expect("extension method is valid")
    );
}

#[tokio::test]
async fn read_request_accepts_options_asterisk() {
    // RFC 9112 §3.2: authority-form belongs to CONNECT, asterisk-form to
    // OPTIONS, and the other two to neither.

    assert!(
        parse_test_request(b"OPTIONS * HTTP/1.1\r\nHost: example.com\r\n\r\n")
            .await
            .request
            .target
            .path_and_query()
            .is_some_and(|target| target.as_str() == "*")
    );
}

#[rstest::rstest]
#[case::connect_tunnel_http_1_1_r_nhost_example_com_r_n_r_n(b"CONNECT /tunnel HTTP/1.1\r\nHost: example.com\r\n\r\n".as_slice())]
#[case::connect_http_1_1_r_nhost_example_com_r_n_r_n(
    b"CONNECT * HTTP/1.1\r\nHost: example.com\r\n\r\n"
)]
#[case::connect_http_example_com_p_http_1_1_r_nhost_example_com_r_n_r_n(
    b"CONNECT http://example.com/p HTTP/1.1\r\nHost: example.com\r\n\r\n"
)]
#[case::get_http_1_1_r_nhost_example_com_r_n_r_n(b"GET * HTTP/1.1\r\nHost: example.com\r\n\r\n")]
#[case::get_example_com_443_http_1_1_r_nhost_example_com_r_n_r_n(
    b"GET example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
)]
#[case::get_p_q_http_1_1_r_nhost_example_com_r_n_r_n(
    b"GET p/q HTTP/1.1\r\nHost: example.com\r\n\r\n"
)]
#[tokio::test]
async fn read_request_rejects_target_forms_for_incompatible_methods(#[case] request: &[u8]) {
    assert!(
        read_test_request(request)
            .await
            .expect("a malformed target is answered, not fatal")
            .is_none(),
        "expected rejection of {:?}",
        str::from_utf8(request).expect("test input is UTF-8")
    );
}

#[tokio::test]
async fn read_request_parses_connect_as_a_tunnel_to_its_authority() {
    // Without an authority form the target parsed as an ordinary `/`, and
    // the tunnel request reached the application instead of a 501.
    let parsed =
        parse_test_request(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
            .await;

    assert_eq!(parsed.request.method, Method::CONNECT);
    assert!(parsed.request.is_connect());
    assert_eq!(
        parsed
            .request
            .target
            .authority()
            .map(RequestAuthority::as_str),
        Some("example.com:443")
    );
}

#[tokio::test]
async fn read_request_maps_portless_connect_to_a_bad_request_response() {
    let parsed = read_test_request(b"CONNECT example.com HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .expect("an invalid tunnel authority receives a response");

    assert!(parsed.is_none());
}

#[tokio::test]
async fn read_request_keeps_content_length_body_shape() {
    let parsed = parse_test_request(
        b"POST /upload HTTP/1.1\r\nHost: example.com\r\nContent-Length: 7\r\n\r\npayload",
    )
    .await;

    assert!(matches!(
        parsed.route,
        RequestRoute::Http(RequestBodyKind::ContentLength(value)) if value.get() == 7
    ));
    assert_eq!(parsed.persistence, ConnectionPersistence::KeepAlive);
}

#[tokio::test]
async fn read_request_keeps_chunked_body_shape() {
    let parsed = parse_test_request(
        b"POST /upload HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\n\r\n",
    )
    .await;

    assert!(matches!(
        parsed.route,
        RequestRoute::Http(RequestBodyKind::Chunked)
    ));
    assert_eq!(parsed.persistence, ConnectionPersistence::KeepAlive);
}

#[tokio::test]
async fn read_request_keeps_connection_close_disposition() {
    let parsed =
        parse_test_request(b"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
            .await;

    assert!(matches!(
        parsed.route,
        RequestRoute::Http(RequestBodyKind::None)
    ));
    assert_eq!(parsed.persistence, ConnectionPersistence::Close);
}

#[tokio::test]
async fn read_request_classifies_valid_websocket_upgrade() {
    let parsed = parse_test_request(
        concat!(
            "GET /ws HTTP/1.1\r\n",
            "Host: example.com\r\n",
            "Connection: Upgrade\r\n",
            "Upgrade: websocket\r\n",
            "Sec-WebSocket-Version: 13\r\n",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            "\r\n",
        )
        .as_bytes(),
    )
    .await;

    assert!(matches!(
        parsed.route,
        RequestRoute::Upgrade(UpgradeRequest::WebSocket { .. })
    ));
}

#[tokio::test]
async fn read_request_classifies_unsupported_websocket_version() {
    let parsed = parse_test_request(
        concat!(
            "GET /ws HTTP/1.1\r\n",
            "Host: example.com\r\n",
            "Connection: Upgrade\r\n",
            "Upgrade: websocket\r\n",
            "Sec-WebSocket-Version: 12\r\n",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            "\r\n",
        )
        .as_bytes(),
    )
    .await;

    assert!(matches!(
        parsed.route,
        RequestRoute::Upgrade(UpgradeRequest::WebSocketUnsupportedVersion)
    ));
}

#[tokio::test]
async fn read_request_classifies_bad_websocket_handshake() {
    let parsed = parse_test_request(
        concat!(
            "POST /ws HTTP/1.1\r\n",
            "Host: example.com\r\n",
            "Connection: Upgrade\r\n",
            "Upgrade: websocket\r\n",
            "Sec-WebSocket-Version: 13\r\n",
            "\r\n",
        )
        .as_bytes(),
    )
    .await;

    assert!(matches!(
        parsed.route,
        RequestRoute::Upgrade(UpgradeRequest::WebSocketBadRequest)
    ));
}

#[tokio::test]
async fn read_request_rejects_whitespace_before_header_colon() {
    let parsed = read_test_request(b"GET / HTTP/1.1\r\nHost : example.com\r\n\r\n")
        .await
        .expect("request parse succeeds");

    assert!(parsed.is_none());
}

#[tokio::test]
async fn read_request_rejects_raw_header_value_controls_after_ows_trimming() {
    let parsed =
        read_test_request(b"GET / HTTP/1.1\r\nHost: example.com\r\nX-Demo: \x0cvalue\r\n\r\n")
            .await
            .expect("malformed request receives a response");

    assert!(parsed.is_none());
}

#[tokio::test]
async fn read_request_rejects_a_second_cr_before_the_line_terminator() {
    let parsed =
        read_test_request(b"GET / HTTP/1.1\r\nHost: example.com\r\nX-Demo: value\r\r\n\r\n")
            .await
            .expect("malformed request receives a response");

    assert!(parsed.is_none());
}

#[tokio::test]
async fn read_request_rejects_duplicate_host_even_when_identical() {
    let parsed = read_test_request(
        concat!(
            "GET / HTTP/1.1\r\n",
            "Host: example.com\r\n",
            "Host: example.com\r\n",
            "\r\n",
        )
        .as_bytes(),
    )
    .await
    .expect("request parse succeeds");

    assert!(parsed.is_none());
}

#[tokio::test]
async fn read_request_rejects_duplicate_websocket_key() {
    let parsed = parse_test_request(
        concat!(
            "GET /ws HTTP/1.1\r\n",
            "Host: example.com\r\n",
            "Connection: Upgrade\r\n",
            "Upgrade: websocket\r\n",
            "Sec-WebSocket-Version: 13\r\n",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            "\r\n",
        )
        .as_bytes(),
    )
    .await;

    assert!(matches!(
        parsed.route,
        RequestRoute::Upgrade(UpgradeRequest::WebSocketBadRequest)
    ));
}

#[tokio::test]
async fn read_request_classifies_h2c_upgrade() {
    let parsed = parse_test_request(
        concat!(
            "GET / HTTP/1.1\r\n",
            "Host: example.com\r\n",
            "Connection: Upgrade, HTTP2-Settings\r\n",
            "Upgrade: h2c\r\n",
            "HTTP2-Settings:\r\n",
            "\r\n",
        )
        .as_bytes(),
    )
    .await;

    assert!(matches!(
        parsed.route,
        RequestRoute::Upgrade(UpgradeRequest::H2c { .. })
    ));
}

#[tokio::test]
async fn read_request_classifies_h2c_bodies() {
    let content_length = parse_test_request(
        concat!(
            "POST / HTTP/1.1\r\n",
            "Host: example.com\r\n",
            "Connection: Upgrade, HTTP2-Settings\r\n",
            "Upgrade: h2c\r\n",
            "HTTP2-Settings:\r\n",
            "Content-Length: 1\r\n",
            "\r\n",
            "x",
        )
        .as_bytes(),
    )
    .await;
    assert!(matches!(
        content_length.route,
        RequestRoute::Http(RequestBodyKind::ContentLength(_))
    ));

    let chunked = parse_test_request(
        concat!(
            "POST / HTTP/1.1\r\n",
            "Host: example.com\r\n",
            "Connection: Upgrade, HTTP2-Settings\r\n",
            "Upgrade: h2c\r\n",
            "HTTP2-Settings:\r\n",
            "Transfer-Encoding: chunked\r\n",
            "\r\n",
        )
        .as_bytes(),
    )
    .await;
    assert!(matches!(
        chunked.route,
        RequestRoute::Http(RequestBodyKind::Chunked)
    ));

    let no_body = parse_test_request(
        concat!(
            "GET / HTTP/1.1\r\n",
            "Host: example.com\r\n",
            "Connection: Upgrade, HTTP2-Settings\r\n",
            "Upgrade: h2c\r\n",
            "HTTP2-Settings:\r\n",
            "Content-Length: 0\r\n",
            "\r\n",
        )
        .as_bytes(),
    )
    .await;
    assert!(matches!(
        no_body.route,
        RequestRoute::Upgrade(UpgradeRequest::H2c { .. })
    ));
}

#[rstest::rstest]
#[case::settings_empty("")]
#[case::aaeaaaab("AAEAAAAB")]
#[tokio::test]
async fn read_request_rejects_duplicate_h2c_settings(#[case] settings: &str) {
    let request = format!(
        concat!(
            "GET / HTTP/1.1\r\n",
            "Host: example.com\r\n",
            "Connection: Upgrade, HTTP2-Settings\r\n",
            "Upgrade: h2c\r\n",
            "HTTP2-Settings:\r\n",
            "HTTP2-Settings: {settings}\r\n",
            "\r\n",
        ),
        settings = settings,
    );
    assert!(
        read_test_request(request.as_bytes())
            .await
            .expect("malformed upgrade is a client error")
            .is_none(),
        "a duplicate HTTP2-Settings header must be rejected: {settings:?}"
    );
}

#[tokio::test]
async fn read_chunked_body_accepts_empty_trailer_block() {
    let (mut client, mut server) = duplex(64);
    let writer = spawn(async move {
        client
            .write_all(b"3\r\nabc\r\n0\r\n\r\n")
            .await
            .expect("duplex write succeeds");
    });
    let mut buffer = BytesMut::new();
    let (tx, mut rx) = mpsc::channel(4);

    let mut body = RequestBodyState::new(None, None, None);
    read_chunked_body(
        &mut server,
        &mut buffer,
        &tx,
        &mut body,
        None,
        unlimited_fields(),
    )
    .await
    .expect("empty trailer block is accepted");
    writer.await.expect("writer task finishes");

    let StreamInput::Data { body, credit: None } = rx.try_recv().expect("body chunk is forwarded")
    else {
        panic!("expected body data event");
    };
    assert_eq!(body.as_ref(), b"abc");
    rx.try_recv().unwrap_err();
}

#[tokio::test]
async fn chunk_delivery_batch_allocates_only_for_small_decoded_fragments() {
    let (tx, mut rx) = mpsc::channel(4);
    let mut body = RequestBodyState::new(None, None, None);

    let empty = ChunkDeliveryBatch::new();
    assert_eq!(
        empty.pending.capacity(),
        0,
        "an empty body has no batch allocation"
    );
    assert!(
        rx.try_recv().is_err(),
        "an empty body makes no delivery call"
    );

    let mut small = ChunkDeliveryBatch::new();
    let mut small_input = BytesMut::from(&[b'a'; 1024][..]);
    small
        .push(&mut small_input, 1024, &tx, &mut body)
        .await
        .expect("small fragment is accepted");
    assert!(
        small.pending.capacity() >= super::CHUNK_DELIVERY_BATCH_BYTES,
        "the first small copy reserves one full batch"
    );
    small.flush(&tx, &mut body).await;
    assert!(matches!(rx.try_recv(), Ok(StreamInput::Data { body, .. }) if body.len() == 1024));
    assert!(
        rx.try_recv().is_err(),
        "one small fragment makes one delivery call"
    );

    let mut direct = ChunkDeliveryBatch::new();
    let mut direct_input = BytesMut::from(vec![b'b'; super::CHUNK_DELIVERY_BATCH_BYTES].as_slice());
    direct
        .push(
            &mut direct_input,
            super::CHUNK_DELIVERY_BATCH_BYTES,
            &tx,
            &mut body,
        )
        .await
        .expect("full batch is accepted");
    assert_eq!(
        direct.pending.capacity(),
        0,
        "a contiguous full batch stays zero-copy"
    );
    assert!(
        matches!(rx.try_recv(), Ok(StreamInput::Data { body, .. }) if body.len() == super::CHUNK_DELIVERY_BATCH_BYTES)
    );
    assert!(
        rx.try_recv().is_err(),
        "one full fragment makes one delivery call"
    );

    let mut tiny = ChunkDeliveryBatch::new();
    let prefix_len = super::CHUNK_DELIVERY_BATCH_BYTES - 3;
    let mut prefix = BytesMut::from(vec![b'c'; prefix_len].as_slice());
    tiny.push(&mut prefix, prefix_len, &tx, &mut body)
        .await
        .expect("partial batch is accepted");
    for _ in 0..2 {
        let mut input = BytesMut::from(&b"c"[..]);
        tiny.push(&mut input, 1, &tx, &mut body)
            .await
            .expect("one-byte fragment is accepted");
    }
    assert!(
        tiny.pending.capacity() >= super::CHUNK_DELIVERY_BATCH_BYTES,
        "many tiny fragments reuse the one full batch allocation"
    );
    let mut input = BytesMut::from(&b"c"[..]);
    tiny.push(&mut input, 1, &tx, &mut body)
        .await
        .expect("the final one-byte fragment is accepted");
    assert!(
        matches!(rx.try_recv(), Ok(StreamInput::Data { body, .. }) if body.len() == super::CHUNK_DELIVERY_BATCH_BYTES)
    );
    assert!(
        rx.try_recv().is_err(),
        "many tiny fragments retain one delivery call"
    );
}

#[tokio::test]
async fn read_chunked_body_accepts_extensions_and_ignores_trailers() {
    let (mut client, mut server) = duplex(128);
    let writer = spawn(async move {
        client
            .write_all(b"3;foo=bar\r\nabc\r\n4;baz=qux\r\ndefg\r\n0\r\nX-Test: yes\r\n\r\n")
            .await
            .expect("duplex write succeeds");
    });
    let mut buffer = BytesMut::new();
    let (tx, mut rx) = mpsc::channel(8);

    let mut body = RequestBodyState::new(None, None, None);
    read_chunked_body(
        &mut server,
        &mut buffer,
        &tx,
        &mut body,
        None,
        unlimited_fields(),
    )
    .await
    .expect("chunk extensions and trailers are accepted");
    writer.await.expect("writer task finishes");

    let StreamInput::Data {
        body: chunk,
        credit: None,
    } = rx.try_recv().expect("coalesced body chunk exists")
    else {
        panic!("expected coalesced body chunk");
    };
    assert_eq!(chunk.as_ref(), b"abcdefg");
    rx.try_recv().unwrap_err();
}

#[tokio::test]
async fn read_chunked_body_batches_tiny_wire_chunks_by_decoded_size() {
    const CHUNKS: usize = 17;

    let mut request = Vec::with_capacity(CHUNKS * 6 + 5);
    for _ in 0..CHUNKS {
        request.extend_from_slice(b"1\r\na\r\n");
    }
    request.extend_from_slice(b"0\r\n\r\n");

    let (mut client, mut server) = duplex(1024);
    let writer = spawn(async move {
        client
            .write_all(&request)
            .await
            .expect("duplex write succeeds");
    });
    let mut buffer = BytesMut::new();
    let (tx, mut rx) = mpsc::channel(2);
    let mut body = RequestBodyState::new(None, None, None);

    read_chunked_body(
        &mut server,
        &mut buffer,
        &tx,
        &mut body,
        None,
        unlimited_fields(),
    )
    .await
    .expect("tiny chunks are accepted");
    writer.await.expect("writer task finishes");

    let StreamInput::Data {
        body: chunk,
        credit: None,
    } = rx.try_recv().expect("one batched body event exists")
    else {
        panic!("expected body data event");
    };
    assert_eq!(chunk.as_ref(), vec![b'a'; CHUNKS]);
    rx.try_recv().unwrap_err();
}

#[tokio::test]
async fn read_chunk_size_line_rejects_overlong_buffered_line() {
    let (_client, mut server) = duplex(1);
    let mut buffer =
        BytesMut::from(format!("{}\r\n", "f".repeat(MAX_CHUNK_SIZE_LINE_BYTES + 1)).as_bytes());

    let err = read_chunk_size_line(&mut server, &mut buffer, None)
        .await
        .expect_err("overlong buffered chunk size line is rejected");

    assert!(matches!(
        err.kind(),
        ErrorKind::Http1(Http1Error::InvalidChunkSize)
    ));
}

#[tokio::test]
async fn read_chunk_size_line_accepts_the_exact_wire_boundary() {
    let line = format!("1;{}\r\n", "a".repeat(MAX_CHUNK_SIZE_LINE_BYTES - 2));
    assert_eq!(line.len(), MAX_CHUNK_SIZE_LINE_BYTES + 2);

    let (_client, mut server) = duplex(1);
    let mut buffered = BytesMut::from(line.as_bytes());
    assert_eq!(
        read_chunk_size_line(&mut server, &mut buffered, None)
            .await
            .expect("the exact limit is accepted"),
        1
    );

    let (prefix, boundary) = line.as_bytes().split_at(line.len() - 3);
    let mut split = BytesMut::from(prefix);
    let boundary = boundary.to_owned();
    let (mut client, mut server) = duplex(1);
    let writer = spawn(async move {
        for byte in boundary {
            client
                .write_all(&[byte])
                .await
                .expect("byte-split line writes");
        }
    });
    assert_eq!(
        read_chunk_size_line(&mut server, &mut split, None)
            .await
            .expect("the byte-split exact limit is accepted"),
        1
    );
    writer.await.expect("writer task completes");
}

#[tokio::test]
async fn drain_chunked_trailers_rejects_oversized_total_section() {
    let (_client, mut server) = duplex(1);
    let mut payload = Vec::new();
    while payload.len() <= MAX_TRAILER_SECTION_BYTES {
        payload.extend_from_slice(b"X-Test: abcdefghijklmnop\r\n");
    }
    payload.extend_from_slice(b"\r\n");
    let mut buffer = BytesMut::from(payload.as_slice());

    let err = drain_chunked_trailers(&mut server, &mut buffer, None, unlimited_fields())
        .await
        .expect_err("oversized trailer section is rejected");

    assert!(matches!(
        err.kind(),
        ErrorKind::Http1(Http1Error::MalformedHeaderLine)
    ));
}

#[tokio::test]
async fn read_chunked_body_rejects_announced_chunk_over_limit_before_consuming_data() {
    let (mut client, mut server) = duplex(64);
    let writer = spawn(async move {
        client
            .write_all(b"5\r\nhello\r\n")
            .await
            .expect("duplex write succeeds");
    });
    let mut buffer = BytesMut::new();
    let (tx, mut rx) = mpsc::channel(4);

    let mut body = RequestBodyState::new(None, None, NonZeroU64::new(4));
    let err = read_chunked_body(
        &mut server,
        &mut buffer,
        &tx,
        &mut body,
        None,
        unlimited_fields(),
    )
    .await
    .expect_err("announced chunk beyond configured limit is rejected");
    writer.await.expect("writer task finishes");

    assert!(matches!(
        err.kind(),
        ErrorKind::Http1(Http1Error::RequestBodyLimitExceeded)
    ));
    rx.try_recv().unwrap_err();
    assert_eq!(&buffer[..], b"hello\r\n");
}

#[test]
fn parse_chunk_size_accepts_hex_extensions_and_rejects_whitespace() {
    assert_eq!(parse_chunk_size(b"1a").unwrap(), 0x1A);
    assert_eq!(parse_chunk_size(b"1A;foo=bar").unwrap(), 0x1A);
    parse_chunk_size(b" \t1A").unwrap_err();
}

#[rstest::rstest]
#[case::line_1_foo_bar_token_baz_quoted_value(b"1;foo; bar = token;baz=\"quoted \\\"value\\\"\"".as_slice())]
#[case::f_t_x_t_t_quoted(b"F \t; X\t=\t\"quoted\"")]
fn parse_chunk_size_accepts_extension_grammar(#[case] line: &[u8]) {
    assert!(parse_chunk_size(line).is_ok(), "{line:?} is legal");
}

#[rstest::rstest]
#[case::line_1(b" 1".as_slice())]
#[case::line_0x1(b"0x1")]
#[case::line_1_7dfe9bd4(b"1;")]
#[case::line_1_value(b"1;=value")]
#[case::line_1_0(b"1;\0")]
#[case::line_1_foo_unterminated(b"1;foo=\"unterminated")]
#[case::line_1_foo(b"1;foo ")]
#[case::line_1_foo_bar(b"1;foo=bar ")]
fn parse_chunk_size_rejects_malformed_extensions(#[case] line: &[u8]) {
    assert!(parse_chunk_size(line).is_err(), "{line:?} is illegal");
}

#[test]
fn parse_chunk_size_rejects_empty_and_invalid_values() {
    assert!(matches!(
        parse_chunk_size(b"").unwrap_err().kind(),
        ErrorKind::Http1(Http1Error::InvalidChunkSize)
    ));
    assert!(matches!(
        parse_chunk_size(b";foo=bar").unwrap_err().kind(),
        ErrorKind::Http1(Http1Error::InvalidChunkSize)
    ));
    assert!(matches!(
        parse_chunk_size(b"1 g").unwrap_err().kind(),
        ErrorKind::Http1(Http1Error::InvalidChunkSize)
    ));
}

#[test]
fn parse_chunk_size_accepts_the_maximum_native_size() {
    let maximum = format!("{:x}", usize::MAX);
    assert_eq!(parse_chunk_size(maximum.as_bytes()).unwrap(), usize::MAX);
}

#[rstest::rstest]
#[case::line_10000000000000000(b"10000000000000000".as_slice())]
#[case::line_10000000000000001_extension_value(b"10000000000000001;extension=value")]
#[case::fffffffffffffffff(b"fffffffffffffffff")]
fn parse_chunk_size_rejects_hexadecimal_overflow(#[case] line: &[u8]) {
    assert!(matches!(
        parse_chunk_size(line).unwrap_err().kind(),
        ErrorKind::Http1(Http1Error::InvalidChunkSize)
    ));
}
