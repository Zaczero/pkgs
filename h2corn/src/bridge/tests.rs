use std::ffi::CString;
use std::future::pending;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use pyo3::ffi::c_str;
use pyo3::types::{
    PyAnyMethods as _, PyBytes, PyDict, PyDictMethods as _, PyList, PyString, PyTuple,
};
use pyo3::{PyResult, Python};
use tokio::sync::{Mutex, mpsc, oneshot};

use super::{
    AsgiMessage, EventSource, HttpInboundEvent, HttpOutboundType, ReceiveResolve, Requeueable,
    ResolveOp, WebSocketOutboundEvent, WebSocketOutboundType, build_http_inbound_event,
    parse_http_outbound_event, parse_websocket_outbound_event,
};
use crate::config::{ConfiguredResponseHeader, ResponseHeaderConfig};
use crate::error::{AsgiContainer, AsgiError, ErrorKind, HttpResponseError, WebSocketError};
use crate::h2_frame::StreamId;
use crate::http::header::{
    ResponseConnectionDirective, inspect_response_headers,
    prepare_fixed_length_response_headers_with_scan,
};
use crate::http::types::status_code;
use crate::python::py_dict;
use crate::runtime::H2InputCreditQueue;

#[derive(Debug)]
struct NeverEventSource;

impl EventSource for NeverEventSource {
    type Event = HttpInboundEvent;

    fn try_pull(&mut self) -> Option<Self::Event> {
        None
    }

    async fn pull(&mut self) -> Self::Event {
        pending().await
    }
}

#[derive(Debug)]
struct CountingEventSource {
    rx: mpsc::Receiver<HttpInboundEvent>,
    pulls: Arc<AtomicUsize>,
}

impl EventSource for CountingEventSource {
    type Event = HttpInboundEvent;

    fn try_pull(&mut self) -> Option<Self::Event> {
        let event = self.rx.try_recv().ok()?;
        self.pulls.fetch_add(1, Ordering::Relaxed);
        Some(event)
    }

    async fn pull(&mut self) -> Self::Event {
        let event = self.rx.recv().await.expect("test input remains open");
        self.pulls.fetch_add(1, Ordering::Relaxed);
        event
    }
}

fn init_python() {
    Python::initialize();
}

#[test]
fn http_response_start_requires_status() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let message = py_dict!(py, {
            "type" => "http.response.start",
        });

        let err = parse_http_outbound_event(&message).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::Asgi(AsgiError::MissingField {
                container: AsgiContainer::HttpResponseStart,
                field: "status",
            })
        ));
        Ok(())
    })
    .unwrap();
}

#[test]
fn asgi_message_boundaries_name_the_event_and_bad_field() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let message = py_dict!(py, { "type" => 1 });
        assert_eq!(
            parse_http_outbound_event(&message).unwrap_err().to_string(),
            "ASGI message type must be a str, got int"
        );

        let message = py_dict!(py, {
            "type" => "http.response.start",
            "status" => "200",
        });
        assert_eq!(
            parse_http_outbound_event(&message).unwrap_err().to_string(),
            "http.response.start status must be an int, got str"
        );

        let message = py_dict!(py, {
            "type" => "http.response.pathsend",
            "path" => 1,
        });
        assert_eq!(
            parse_http_outbound_event(&message).unwrap_err().to_string(),
            "http.response.pathsend path must be a str, got int"
        );

        let message = py_dict!(py, {
            "type" => "http.response.body",
            "more_body" => "yes",
        });
        assert_eq!(
            parse_http_outbound_event(&message).unwrap_err().to_string(),
            "http.response.body more_body must be a bool, got str"
        );

        let message = py_dict!(py, {
            "type" => "http.response.start",
            "status" => 200,
            "headers" => [1],
        });
        assert_eq!(
            parse_http_outbound_event(&message).unwrap_err().to_string(),
            "http.response.start headers must be two-item (bytes, bytes) pairs, got int"
        );

        let message = py_dict!(py, {
            "type" => "websocket.close",
            "code" => "1000",
        });
        assert_eq!(
            parse_websocket_outbound_event(&message)
                .unwrap_err()
                .to_string(),
            "websocket.close code must be an int, got str"
        );

        let message = py_dict!(py, {
            "type" => "websocket.close",
            "code" => 0,
        });
        assert_eq!(
            parse_websocket_outbound_event(&message)
                .unwrap_err()
                .to_string(),
            "websocket close code is invalid"
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn asgi_outbound_type_dispatches_interned_values_and_falls_back_to_strings() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        // Driven from the same table the dispatch is generated from, so
        // a new message type is covered the moment it is declared. The
        // property that matters is that every one of them takes the
        // interned pointer path: a miss there is only slower, never wrong,
        // so nothing else would report it.
        for (expected, text) in HttpOutboundType::WIRE_NAMES {
            let message = PyDict::new(py);
            message.set_item("type", PyString::intern(py, text))?;
            let message = AsgiMessage::parse(&message).unwrap();
            assert_eq!(message.interned_http_outbound_type(), Some(*expected));
            assert_eq!(message.http_outbound_type().unwrap(), *expected);
        }

        for (expected, text) in WebSocketOutboundType::WIRE_NAMES {
            let message = PyDict::new(py);
            message.set_item("type", PyString::intern(py, text))?;
            let message = AsgiMessage::parse(&message).unwrap();
            assert_eq!(message.interned_websocket_outbound_type(), Some(*expected));
            assert_eq!(message.websocket_outbound_type().unwrap(), *expected);
        }

        let dynamic = PyString::new(py, "http.response.body");
        assert!(!dynamic.is(PyString::intern(py, "http.response.body")));
        let message = PyDict::new(py);
        message.set_item("type", dynamic)?;
        let message = AsgiMessage::parse(&message).unwrap();
        assert_eq!(message.interned_http_outbound_type(), None);
        assert_eq!(
            message.http_outbound_type().unwrap(),
            HttpOutboundType::Body
        );

        let subclass = py.eval(
            c_str!("type('MessageType', (str,), {} )('websocket.send')"),
            None,
            None,
        )?;
        let message = PyDict::new(py);
        message.set_item("type", subclass)?;
        let message = AsgiMessage::parse(&message).unwrap();
        assert_eq!(message.interned_websocket_outbound_type(), None);
        assert_eq!(
            message.websocket_outbound_type().unwrap(),
            WebSocketOutboundType::Send
        );

        let message = PyDict::new(py);
        message.set_item("type", PyString::new(py, "http.response.unknown"))?;
        assert_eq!(
            parse_http_outbound_event(&message).unwrap_err().to_string(),
            "unsupported http ASGI outbound message: http.response.unknown"
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn websocket_send_accepts_exactly_one_payload_variant() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let text_message = py_dict!(py, {
            "type" => "websocket.send",
            "text" => "hello",
        });
        let bytes_message = py_dict!(py, {
            "type" => "websocket.send",
            "bytes" => PyBytes::new(py, b"hello"),
        });

        let text_event = parse_websocket_outbound_event(&text_message).unwrap();
        let bytes_event = parse_websocket_outbound_event(&bytes_message).unwrap();

        assert!(matches!(
            text_event,
            WebSocketOutboundEvent::SendText(text) if &*text == "hello"
        ));
        assert!(matches!(
            bytes_event,
            WebSocketOutboundEvent::SendBytes(body) if body.as_ref() == b"hello"
        ));
        Ok(())
    })
    .unwrap();
}

#[test]
fn unexpected_websocket_event_reports_its_asgi_type_not_application_payload() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
            let message = py_dict!(py, {
                "type" => "websocket.send",
                "text" => "sk-live-SECRET-TOKEN-12345",
            });

            let event = parse_websocket_outbound_event(&message).unwrap();
            let error = WebSocketError::unexpected_initial_event(event.message_type());
            assert_eq!(
                error.to_string(),
                "unexpected websocket.send before handshake; the app must send websocket.accept or websocket.close first"
            );
            assert!(!error.to_string().contains("sk-live-SECRET-TOKEN-12345"));
            Ok(())
        })
        .unwrap();
}

#[test]
fn websocket_send_rejects_missing_or_ambiguous_payload() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let missing_payload = py_dict!(py, {
            "type" => "websocket.send",
        });
        let ambiguous_payload = py_dict!(py, {
            "type" => "websocket.send",
            "text" => "hello",
            "bytes" => PyBytes::new(py, b"hello"),
        });

        for message in [&missing_payload, &ambiguous_payload] {
            let err = parse_websocket_outbound_event(message).unwrap_err();
            assert!(matches!(
                err.kind(),
                ErrorKind::Asgi(AsgiError::WebSocketSendRequiresExactlyOnePayload)
            ));
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn websocket_accept_owns_handshake_headers_at_ingress() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let message = py_dict!(py, {
            "type" => "websocket.accept",
            "headers" => [
                (PyBytes::new(py, b"x-before"), PyBytes::new(py, b"one")),
                (PyBytes::new(py, b"connection"), PyBytes::new(py, b"upgrade")),
                (PyBytes::new(py, b"upgrade"), PyBytes::new(py, b"not-websocket")),
                (PyBytes::new(py, b"sec-websocket-accept"), PyBytes::new(py, b"bogus")),
                (PyBytes::new(py, b"x-after"), PyBytes::new(py, b"two")),
            ],
        });

        let WebSocketOutboundEvent::Accept { headers, .. } =
            parse_websocket_outbound_event(&message).expect("accept headers are parsed")
        else {
            panic!("websocket.accept must produce an accept event");
        };
        let fields = headers
            .iter()
            .map(|(name, value)| (name.as_bytes(), value.as_bytes()))
            .collect::<Vec<_>>();
        assert_eq!(fields, [
            (b"x-before".as_slice(), b"one".as_slice()),
            (b"x-after".as_slice(), b"two".as_slice())
        ]);

        for name in [
            b"sec-websocket-protocol".as_slice(),
            b"sec-websocket-extensions",
        ] {
            let message = py_dict!(py, {
                "type" => "websocket.accept",
                "headers" => [(PyBytes::new(py, name), PyBytes::new(py, b"value"))],
            });
            assert!(
                parse_websocket_outbound_event(&message).is_err(),
                "{name:?}"
            );
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn http_response_start_defaults_trailers_to_false() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let message = py_dict!(py, {
            "type" => "http.response.start",
            "status" => 200,
            "headers" => [
                (PyBytes::new(py, b"content-length"), PyBytes::new(py, b"2")),
            ],
        });

        let event = parse_http_outbound_event(&message).unwrap();
        assert!(matches!(
            event,
            super::HttpOutboundEvent::Start { status, trailers: false, headers, .. }
                if status.get() == status_code::OK && headers.len() == 1
                    && headers[0].0.as_ref() == b"content-length"
                    && headers[0].1.as_ref() == b"2"
        ));
        Ok(())
    })
    .unwrap();
}

#[test]
fn response_status_validation_handles_python_integer_boundaries() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
            for status in [-1_i64, 0, 99] {
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => status,
                });
                let err = parse_http_outbound_event(&message).unwrap_err();
                assert!(matches!(
                    err.kind(),
                    ErrorKind::HttpResponse(HttpResponseError::StatusMustBeThreeDigitCode { .. })
                ));
                assert_eq!(
                    err.to_string(),
                    format!("http.response.start status must be a three-digit code, got {status}")
                );
            }

            let too_large = py.eval(c_str!("2**63"), None, None)?;
            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => too_large,
            });
            assert_eq!(
                parse_http_outbound_event(&message).unwrap_err().to_string(),
                "http.response.start status must be a three-digit code, got an integer outside the signed 64-bit range"
            );

            for status in [100_u16, 199] {
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => status,
                });
                assert!(matches!(
                    parse_http_outbound_event(&message).unwrap_err().kind(),
                    ErrorKind::HttpResponse(HttpResponseError::InformationalStatusUnsupported {
                        status: actual,
                        ..
                    }) if *actual == status
                ));
            }

            {
                let status = 600_u16;
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => status,
                });
                assert!(matches!(
                    parse_http_outbound_event(&message),
                    Ok(super::HttpOutboundEvent::Start { status: actual, .. })
                        if actual.get().get() == status
                ));
            }

            let bool_message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => true,
            });
            assert_eq!(
                parse_http_outbound_event(&bool_message).unwrap_err().to_string(),
                "http.response.start status must be a three-digit code, got 1"
            );

            let subclass = py.eval(c_str!("type('Status', (int,), {})(201)"), None, None)?;
            let subclass_message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => subclass,
            });
            assert!(matches!(
                parse_http_outbound_event(&subclass_message),
                Ok(super::HttpOutboundEvent::Start { status, .. }) if status.get().get() == 201
            ));
            Ok(())
        })
        .unwrap();
}

#[test]
fn http_response_start_accepts_tuple_headers_via_generic_fallback() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let headers = PyTuple::new(py, [(PyBytes::new(py, b"x-demo"), PyBytes::new(py, b"1"))])?;
        let message = py_dict!(py, {
            "type" => "http.response.start",
            "status" => 200,
            "headers" => headers,
        });

        let event = parse_http_outbound_event(&message).unwrap();
        assert!(matches!(
            event,
            super::HttpOutboundEvent::Start { status, trailers: false, headers, .. }
                if status.get() == status_code::OK && headers.len() == 1
                    && headers[0].0.as_ref() == b"x-demo"
                    && headers[0].1.as_ref() == b"1"
        ));
        Ok(())
    })
    .unwrap();
}

#[test]
fn response_header_iterable_never_calls_len() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let locals = PyDict::new(py);
        py.run(
            c_str!(
                r#"
import sys
class Headers:
    def __init__(self, kind):
        self.kind = kind
    def __iter__(self):
        return iter(((b"x-test", b"ok"),))
    def __len__(self):
        if self.kind == 0:
            raise AssertionError("__len__ must not run")
        if self.kind == 1:
            return sys.maxsize
        return -1
headers = (Headers(0), Headers(1), Headers(2))
"#
            ),
            Some(&locals),
            Some(&locals),
        )?;
        let header_sets = locals
            .get_item("headers")?
            .expect("test created header iterables");
        for headers in header_sets.try_iter()? {
            let message = PyDict::new(py);
            message.set_item("type", "http.response.start")?;
            message.set_item("status", 200)?;
            message.set_item("headers", headers?)?;
            parse_http_outbound_event(&message).unwrap();
        }
        Ok(())
    })
    .unwrap();
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one Python fixture shares iterator state across the precedence matrix"
)]
fn generic_header_pairs_preserve_iterator_error_precedence() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let locals = PyDict::new(py);
        py.run(
            c_str!(
                r#"
class Pair:
    def __init__(self, values, error=None):
        self.values = values
        self.error = error
        self.index = 0
        self.next_calls = 0
    def __iter__(self):
        return self
    def __next__(self):
        self.next_calls += 1
        if self.index < len(self.values):
            value = self.values[self.index]
            self.index += 1
            return value
        if self.error:
            raise RuntimeError(self.error)
        raise StopIteration

class Headers:
    def __init__(self, pair, error=None):
        self.pair = pair
        self.error = error
    def __iter__(self):
        yield self.pair
        if self.error:
            raise RuntimeError(self.error)
"#
            ),
            Some(&locals),
            Some(&locals),
        )?;
        let cases = [
            (
                "Pair([], None)",
                Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair"),
            ),
            (
                "Pair([b'x'], None)",
                Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair"),
            ),
            ("Pair([b'x', b'y'], None)", None),
            (
                "Pair([b'x', b'y', b'z'], None)",
                Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair"),
            ),
            (
                "Pair([1, b'y', b'z'])",
                Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair"),
            ),
            (
                "Pair([b'x', 1, b'z'])",
                Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair"),
            ),
            (
                "Pair([1, b'y'], None)",
                Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair"),
            ),
            (
                "Pair([b'x', 1], None)",
                Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair"),
            ),
            (
                "Pair([b'x', b'y'], 'late')",
                Some("http.response.start headers must be two-item (bytes, bytes) pairs, got Pair"),
            ),
        ];
        for (pair, expected) in cases {
            let pair_code = CString::new(pair).unwrap();
            let pair = py.eval(pair_code.as_c_str(), Some(&locals), None)?;
            locals.set_item("pair", &pair)?;
            let headers = py.eval(c_str!("Headers(pair)"), Some(&locals), None)?;
            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => headers,
            });
            let result = parse_http_outbound_event(&message);
            if let Some(expected) = expected {
                assert_eq!(result.unwrap_err().to_string(), expected, "{pair}");
            } else {
                assert!(result.is_ok(), "{pair}");
            }
        }

        let pair = py.eval(
            c_str!("Pair([b'x', b'y', b'z'], 'late')"),
            Some(&locals),
            None,
        )?;
        locals.set_item("pair", &pair)?;
        let headers = py.eval(c_str!("Headers(pair)"), Some(&locals), None)?;
        let message = py_dict!(py, {
            "type" => "http.response.start",
            "status" => 200,
            "headers" => headers,
        });
        assert_eq!(pair.getattr("next_calls")?.extract::<usize>()?, 0);
        assert_eq!(
            parse_http_outbound_event(&message).unwrap_err().to_string(),
            "http.response.start headers must be two-item (bytes, bytes) pairs, got Pair"
        );
        assert_eq!(pair.getattr("next_calls")?.extract::<usize>()?, 4);

        let pair = py.eval(c_str!("Pair([b'x', b'y'], 'inner')"), Some(&locals), None)?;
        locals.set_item("pair", &pair)?;
        let headers = py.eval(c_str!("Headers(pair)"), Some(&locals), None)?;
        let message = py_dict!(py, {
            "type" => "http.response.start",
            "status" => 200,
            "headers" => headers,
        });
        assert!(
            parse_http_outbound_event(&message)
                .unwrap_err()
                .to_string()
                .contains("two-item (bytes, bytes) pairs")
        );

        let message = py_dict!(py, {
            "type" => "http.response.start",
            "status" => 200,
            "headers" => 1,
        });
        assert!(
            parse_http_outbound_event(&message)
                .unwrap_err()
                .to_string()
                .contains("an iterable of two-item (bytes, bytes) pairs")
        );

        let pair = py.eval(c_str!("Pair([b'x', b'y'], None)"), Some(&locals), None)?;
        locals.set_item("pair", &pair)?;
        let headers = py.eval(c_str!("Headers(pair, 'outer')"), Some(&locals), None)?;
        let message = py_dict!(py, {
            "type" => "http.response.start",
            "status" => 200,
            "headers" => headers,
        });
        assert!(
            parse_http_outbound_event(&message)
                .unwrap_err()
                .to_string()
                .contains("an iterable of two-item (bytes, bytes) pairs")
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn exact_header_list_reserves_all_builtin_default_slots() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        // The ingress reserve covers `server`, `date`, and
        // `content-length`; the default helper separately reserves any
        // configured extras because their count belongs to configuration.

        let one = PyList::new(py, [(PyBytes::new(py, b"x-app"), PyBytes::new(py, b"one"))])?;
        let mut headers = super::try_parse_exact_header_list(&one)
            .expect("response header list parses")
            .expect("exact list stays on the exact path");
        assert_eq!(headers.capacity(), headers.len() + 3);
        let allocation = headers.as_ptr();
        let defaults = ResponseHeaderConfig {
            server_header: Some(Bytes::from_static(b"h2corn")),
            date_header: true,
            extra_headers: Box::new([]),
        };
        let mut scan = inspect_response_headers(&headers);
        prepare_fixed_length_response_headers_with_scan(&mut headers, &mut scan, &defaults, 3);
        assert_eq!(headers.as_ptr(), allocation, "built-ins do not reallocate");
        assert_eq!(headers.capacity(), 4);
        assert_eq!(headers.len(), 4);

        // This is the old `list.len()` capacity. It has one 80-byte
        // field allocation, and default preparation grows it once to the
        // four-field (320-byte) allocation. The production path above
        // must remain the no-growth counterpart of this control.
        let mut old_capacity = super::try_parse_exact_header_list(&one)
            .expect("response header list parses")
            .expect("exact list stays on the exact path");
        old_capacity.shrink_to_fit();
        assert_eq!(old_capacity.capacity(), 1);
        let old_allocation = old_capacity.as_ptr();
        let mut scan = inspect_response_headers(&old_capacity);
        prepare_fixed_length_response_headers_with_scan(&mut old_capacity, &mut scan, &defaults, 3);
        assert_ne!(
            old_capacity.as_ptr(),
            old_allocation,
            "one field grows from 80 to 320 bytes"
        );
        assert_eq!(old_capacity.capacity(), 4);

        // Application-provided built-ins leave the three reserved slots
        // unused; disabled server/date therefore cannot grow this vector.
        let supplied = PyList::new(py, [
            (PyBytes::new(py, b"server"), PyBytes::new(py, b"app")),
            (PyBytes::new(py, b"date"), PyBytes::new(py, b"date")),
            (PyBytes::new(py, b"content-length"), PyBytes::new(py, b"3")),
        ])?;
        let mut supplied = super::try_parse_exact_header_list(&supplied)
            .expect("response header list parses")
            .expect("exact list stays on the exact path");
        let allocation = supplied.as_ptr();
        let defaults = ResponseHeaderConfig::default();
        let mut scan = inspect_response_headers(&supplied);
        prepare_fixed_length_response_headers_with_scan(&mut supplied, &mut scan, &defaults, 3);
        assert_eq!(supplied.as_ptr(), allocation);
        assert_eq!(supplied.len(), 3);

        // Configured extras retain their existing exact reserve in the
        // default helper and are appended once, after application fields.
        let empty = PyList::empty(py);
        let mut configured = super::try_parse_exact_header_list(&empty)
            .expect("response header list parses")
            .expect("exact list stays on the exact path");
        let defaults = ResponseHeaderConfig {
            server_header: None,
            date_header: false,
            extra_headers: Box::new([ConfiguredResponseHeader::new(
                Bytes::from_static(b"x-configured"),
                Bytes::from_static(b"yes"),
            )]),
        };
        let mut scan = inspect_response_headers(&configured);
        prepare_fixed_length_response_headers_with_scan(&mut configured, &mut scan, &defaults, 0);
        assert_eq!(
            configured
                .iter()
                .map(|(name, _)| name.as_bytes())
                .collect::<Vec<_>>(),
            [b"x-configured".as_slice(), b"content-length"],
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn response_headers_and_trailers_enforce_their_own_field_policies() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
            for name in [b"upgrade".as_slice(), b"connection"] {
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => 200,
                    "headers" => [(PyBytes::new(py, name), PyBytes::new(py, b"x"))],
                });
                assert!(parse_http_outbound_event(&message).is_err(), "{name:?}");
            }

            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => [
                    (PyBytes::new(py, b"Transfer-Encoding"), PyBytes::new(py, b"gzip")),
                    (PyBytes::new(py, b"TE"), PyBytes::new(py, b"trailers")),
                    (PyBytes::new(py, b"Content-Type"), PyBytes::new(py, b"text/plain")),
                ],
            });
            let event = parse_http_outbound_event(&message).expect("fixed fields are stripped");
            assert!(matches!(
                event,
                super::HttpOutboundEvent::Start { headers, .. }
                    if headers.len() == 1
                        && headers[0].0.as_ref() == b"content-type"
            ));

            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => [
                    (PyBytes::new(py, b"connection"), PyBytes::new(py, b"content-security-policy")),
                    (PyBytes::new(py, b"content-security-policy"), PyBytes::new(py, b"default-src 'self'")),
                ],
            });
            let _error = parse_http_outbound_event(&message).unwrap_err();

            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 103,
            });
            let _error = parse_http_outbound_event(&message).unwrap_err();

            for value in [b"".as_slice(), b"internal\t whitespace"] {
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => 200,
                    "headers" => [(PyBytes::new(py, b"x-test"), PyBytes::new(py, value))],
                });
                assert!(parse_http_outbound_event(&message).is_ok(), "{value:?}");
            }
            for value in [
                b" leading".as_slice(),
                b"trailing ",
                b"\tleading",
                b"trailing\t",
            ] {
                let message = py_dict!(py, {
                    "type" => "http.response.start",
                    "status" => 200,
                    "headers" => [(PyBytes::new(py, b"x-test"), PyBytes::new(py, value))],
                });
                assert!(parse_http_outbound_event(&message).is_err(), "{value:?}");
            }

            for name in [
                b"content-length".as_slice(),
                b"authorization",
                b"content-type",
            ] {
                let message = py_dict!(py, {
                    "type" => "http.response.trailers",
                    "headers" => [(PyBytes::new(py, name), PyBytes::new(py, b"x"))],
                });
                assert!(parse_http_outbound_event(&message).is_err(), "{name:?}");
            }
            for name in [b"x-checksum".as_slice(), b"content-digest"] {
                let message = py_dict!(py, {
                    "type" => "http.response.trailers",
                    "headers" => [(PyBytes::new(py, name), PyBytes::new(py, b"x"))],
                });
                assert!(parse_http_outbound_event(&message).is_ok(), "{name:?}");
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn response_connection_directive_has_exactly_one_meaning() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        for (connection, upgrade, expected) in [
            (
                b"close".as_slice(),
                false,
                ResponseConnectionDirective::Close,
            ),
            (b"upgrade", true, ResponseConnectionDirective::Upgrade),
            (b"keep-alive", false, ResponseConnectionDirective::None),
            (b"close, close", false, ResponseConnectionDirective::Close),
            (
                b"upgrade, upgrade",
                true,
                ResponseConnectionDirective::Upgrade,
            ),
        ] {
            let mut headers = vec![(
                PyBytes::new(py, b"connection"),
                PyBytes::new(py, connection),
            )];
            if upgrade {
                headers.push((PyBytes::new(py, b"upgrade"), PyBytes::new(py, b"h2c")));
            }
            let message = py_dict!(py, {
                "type" => "http.response.start",
                "status" => 200,
                "headers" => headers,
            });
            let event =
                parse_http_outbound_event(&message).expect("valid response connection directive");
            assert!(matches!(
                event,
                super::HttpOutboundEvent::Start { directive, .. }
                    if directive == expected
            ));
        }

        let contradictory = py_dict!(py, {
            "type" => "http.response.start",
            "status" => 200,
            "headers" => [
                (PyBytes::new(py, b"connection"), PyBytes::new(py, b"close, upgrade")),
                (PyBytes::new(py, b"upgrade"), PyBytes::new(py, b"h2c")),
            ],
        });
        let _ = parse_http_outbound_event(&contradictory)
            .expect_err("contradictory connection directives are rejected");
        Ok(())
    })
    .unwrap();
}

#[test]
fn http_request_event_omits_default_body_and_more_body() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let event = build_http_inbound_event(py, HttpInboundEvent::Request {
            body: Bytes::new(),
            more_body: false,
            credit: None,
        })?
        .bind(py)
        .clone()
        .cast_into::<PyDict>()?;

        assert!(event.get_item("type")?.is_some());
        assert!(event.get_item("body")?.is_none());
        assert!(event.get_item("more_body")?.is_none());
        Ok(())
    })
    .unwrap();
}

#[test]
fn batched_http_request_materializes_one_contiguous_python_body() {
    init_python();
    Python::attach(|py| -> PyResult<()> {
        let event = build_http_inbound_event(py, HttpInboundEvent::RequestBatch {
            bodies: vec![
                Bytes::from_static(b"segmented-"),
                Bytes::from_static(b"body"),
            ],
            body_bytes: b"segmented-body".len(),
            credit: None,
        })?
        .bind(py)
        .clone()
        .cast_into::<PyDict>()?;

        assert_eq!(
            event
                .get_item("body")?
                .expect("body is present")
                .extract::<Vec<u8>>()?,
            b"segmented-body"
        );
        assert!(
            event
                .get_item("more_body")?
                .expect("more_body is present")
                .extract::<bool>()?
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn cancelled_receive_requeue_retains_h2_credit_until_conversion() {
    init_python();
    let flow = Arc::new(H2InputCreditQueue::default());
    let stream_id = StreamId::new(1).expect("non-zero stream id");
    let mut receive = Requeueable::new(NeverEventSource);
    receive.requeue(HttpInboundEvent::Request {
        body: Bytes::from_static(b"body"),
        more_body: true,
        credit: Some(flow.credit(stream_id, NonZeroU32::new(4).unwrap())),
    });

    assert!(!flow.has_pending(), "requeue must retain receive credit");
    let event = receive
        .try_next()
        .expect("requeued event is returned first");
    assert!(
        !flow.has_pending(),
        "pulling alone must retain receive credit"
    );
    Python::attach(|py| build_http_inbound_event(py, event)).expect("event converts");

    assert!(flow.has_pending(), "conversion commits receive credit");
    let mut released = Vec::new();
    flow.drain_into(&mut released);
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].stream_id, stream_id);
    assert_eq!(released[0].bytes.get(), 4);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "the test deliberately retains the owned receive guard until cancellation resolution"
)]
async fn cancelled_in_flight_receive_requeues_before_a_concurrent_waiter() {
    init_python();
    let flow = Arc::new(H2InputCreditQueue::default());
    let stream_id = StreamId::new(1).expect("non-zero stream id");
    let pulls = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel(2);
    tx.send(HttpInboundEvent::Request {
        body: Bytes::from_static(b"first"),
        more_body: true,
        credit: Some(flow.credit(stream_id, NonZeroU32::new(5).unwrap())),
    })
    .await
    .expect("first input is queued");
    tx.send(HttpInboundEvent::Request {
        body: Bytes::from_static(b"second"),
        more_body: true,
        credit: None,
    })
    .await
    .expect("second input is queued");

    let state = Arc::new(Mutex::new(Requeueable::new(CountingEventSource {
        rx,
        pulls: Arc::clone(&pulls),
    })));
    let mut first_guard = Arc::clone(&state).lock_owned().await;
    let first = first_guard.next().await;
    // Model the interval after the Tokio waiter consumed the event but
    // before the Python-loop pump resolves its RustFuture.
    let resolution: Box<dyn ResolveOp + Send> = Box::new(ReceiveResolve {
        event: first,
        guard: first_guard,
        build_event: build_http_inbound_event,
    });
    assert_eq!(pulls.load(Ordering::Relaxed), 1);
    assert!(!flow.has_pending(), "consumption alone retains H2 credit");

    let second_state = Arc::clone(&state);
    let (attempted_tx, attempted_rx) = oneshot::channel();
    let second = tokio::spawn(async move {
        let _ = attempted_tx.send(());
        let mut guard = second_state.lock_owned().await;
        guard.next().await
    });
    attempted_rx
        .await
        .expect("the concurrent receive attempted the state lock");
    tokio::task::yield_now().await;
    assert!(
        !second.is_finished(),
        "one consumed event stays exclusively owned until pump resolution"
    );
    assert_eq!(
        pulls.load(Ordering::Relaxed),
        1,
        "the concurrent receive cannot consume the following source event"
    );

    // Cancellation wins at pump resolution. Requeue is synchronous while
    // the resolver still owns the mutex, so the next waiter must observe
    // the exact consumed event before touching the source again.
    resolution.requeue();
    let first_again = second.await.expect("concurrent receive completes");
    assert_eq!(pulls.load(Ordering::Relaxed), 1);
    let HttpInboundEvent::Request { body, .. } = &first_again else {
        panic!("the first request event is preserved")
    };
    assert_eq!(body.as_ref(), b"first");
    assert!(!flow.has_pending(), "requeue still retains H2 credit");

    Python::attach(|py| build_http_inbound_event(py, first_again))
        .expect("the requeued event converts");
    assert!(flow.has_pending(), "conversion releases H2 credit once");
    let mut released = Vec::new();
    flow.drain_into(&mut released);
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].stream_id, stream_id);
    assert_eq!(released[0].bytes.get(), 5);
    assert!(!flow.has_pending());

    let mut guard = state.lock().await;
    let second_event = guard.next().await;
    drop(guard);
    assert_eq!(pulls.load(Ordering::Relaxed), 2);
    let HttpInboundEvent::Request { body, .. } = second_event else {
        panic!("the following request event is preserved")
    };
    assert_eq!(body.as_ref(), b"second");
}
