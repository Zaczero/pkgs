use std::ffi::CStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::{ResolveOp, ResolvePayload, RustFuture, new_rust_future};
use crate::pyloop::ShardHandle;

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn conversion_gc_callback_can_reenter_future() {
    const CHILD: &str = "H2CORN_CONVERSION_GC_CHILD";
    Python::initialize();
    Python::attach(|py| {
        if std::env::var_os(CHILD).is_some() {
            let future = new_rust_future(py, &ShardHandle::test_stub(py)).expect("future");
            let locals = PyDict::new(py);
            locals.set_item("future", &future).expect("future local");
            py.run(
                c_str!(
                    r#"
import gc
observed = []
def on_gc(phase, info):
    if phase == "start":
        observed.append(future.done())
gc.callbacks.append(on_gc)
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .expect("GC callback");
            let locals_for_conversion = locals.clone().unbind();
            RustFuture::resolve(
                &future,
                py,
                ResolvePayload::Simple(Box::new(move |py| {
                    py.run(
                        c_str!("gc.collect()"),
                        Some(locals_for_conversion.bind(py)),
                        None,
                    )?;
                    Ok(py.None())
                })),
            );
            py.run(
                c_str!(
                    r#"
gc.callbacks.remove(on_gc)
assert observed == [False], observed
assert future.done()
assert future.result() is None
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .expect("reentrant collection");
            return;
        }
        let locals = PyDict::new(py);
        locals
            .set_item(
                "executable",
                std::env::current_exe()
                    .expect("test executable")
                    .to_string_lossy()
                    .as_ref(),
            )
            .expect("executable local");
        py.run(
            c_str!(
                r#"
import os
import subprocess
environment = os.environ.copy()
environment["H2CORN_CONVERSION_GC_CHILD"] = "1"
child = subprocess.run([executable, "conversion_gc_callback_can_reenter_future"],
                       env=environment, capture_output=True, text=True, timeout=3)
assert child.returncode == 0, (child.stdout, child.stderr)
"#
            ),
            Some(&locals),
            Some(&locals),
        )
        .expect("conversion must not deadlock in GC");
    });
}

#[rstest::rstest]
#[case::cancel_message(c_str!("assert future.cancel(msg=future)"))]
#[case::pending_callback(c_str!("future.add_done_callback(lambda _, captured=future: None)"))]
#[case::callback_context(c_str!(r#"
import contextvars
key = contextvars.ContextVar("future")
key.set(future)
future.add_done_callback(lambda _: None)
key.set(None)
"#))]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn python_cycles_are_collected(#[case] setup: &CStr) {
    Python::initialize();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("test runtime");
    let waiter = runtime.spawn(std::future::pending::<()>());
    Python::attach(|py| {
        let future = new_rust_future(py, &ShardHandle::test_stub(py)).expect("future");
        future.get().set_abort(waiter.abort_handle());
        let locals = PyDict::new(py);
        locals.set_item("future", &future).expect("future local");
        drop(future);
        py.run(
            c_str!("import gc, weakref\nreference = weakref.ref(future)"),
            Some(&locals),
            Some(&locals),
        )
        .expect("weak reference");
        py.run(setup, Some(&locals), Some(&locals)).expect("cycle");
        py.run(
            c_str!("del future\ngc.collect()\nassert reference() is None"),
            Some(&locals),
            Some(&locals),
        )
        .expect("future cycle collected");
    });
    let error = runtime
        .block_on(waiter)
        .expect_err("clearing a pending cycle aborts its waiter");
    assert!(error.is_cancelled());
}

#[rstest::rstest]
#[case::result(false)]
#[case::exception(true)]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn completed_value_cycles_are_collected(#[case] failure: bool) {
    Python::initialize();
    Python::attach(|py| {
        let future = new_rust_future(py, &ShardHandle::test_stub(py)).expect("future");
        let locals = PyDict::new(py);
        locals.set_item("future", &future).expect("future local");
        let captured = future.clone_ref(py);
        RustFuture::resolve(
            &future,
            py,
            ResolvePayload::Simple(Box::new(move |py| {
                if failure {
                    let error = pyo3::exceptions::PyValueError::new_err("resolution failed");
                    error.value(py).setattr("future", captured)?;
                    Err(error)
                } else {
                    Ok(captured.into_any())
                }
            })),
        );
        drop(future);
        py.run(
            c_str!(
                r#"
import gc, weakref
reference = weakref.ref(future)
del future
gc.collect()
assert reference() is None
"#
            ),
            Some(&locals),
            Some(&locals),
        )
        .expect("completed cycle collected");
    });
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn future_loop_cycle_does_not_retain_shard() {
    Python::initialize();
    Python::attach(|py| {
        let event_loop = py
            .import("asyncio")
            .expect("asyncio")
            .call_method0("new_event_loop")
            .expect("loop");
        let shard = ShardHandle::from_event_loop(py, &event_loop, crate::config::LogFormat::Text)
            .expect("shard");
        let weak_shard = Arc::downgrade(&shard);
        let future = new_rust_future(py, &shard).expect("future");
        drop(shard);
        assert!(weak_shard.upgrade().is_none());
        let locals = PyDict::new(py);
        locals.set_item("future", &future).expect("future local");
        locals
            .set_item("event_loop", &event_loop)
            .expect("loop local");
        drop(future);
        drop(event_loop);
        py.run(
            c_str!(
                r#"
import gc, weakref
event_loop.close()
event_loop.future = future
future_reference = weakref.ref(future)
loop_reference = weakref.ref(event_loop)
del event_loop, future
gc.collect()
assert future_reference() is None
assert loop_reference() is None
"#
            ),
            Some(&locals),
            Some(&locals),
        )
        .expect("loop cycle collected");
    });
}

struct ReentrantResolve {
    future: Py<RustFuture>,
    converted: Arc<AtomicUsize>,
    requeued: Arc<AtomicUsize>,
    cancel_during_conversion: bool,
}

impl ResolveOp for ReentrantResolve {
    #[expect(
        clippy::panic_in_result_fn,
        reason = "the test asserts the resolution contract from its conversion callback"
    )]
    fn convert(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.converted.fetch_add(1, Ordering::Relaxed);
        assert!(!self.future.get().done());
        if self.cancel_during_conversion {
            assert!(RustFuture::cancel(self.future.bind(py), None)?);
        }
        Ok(41_u32.into_pyobject(py)?.into_any().unbind())
    }

    fn requeue(self: Box<Self>) {
        self.requeued.fetch_add(1, Ordering::Relaxed);
    }
}

#[rstest::rstest]
#[case::before(true, false, 0, 1)]
#[case::during(false, true, 1, 1)]
#[case::after(false, false, 1, 0)]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn cancellation_owns_exactly_one_requeue(
    #[case] cancel_before: bool,
    #[case] cancel_during: bool,
    #[case] conversions: usize,
    #[case] requeues: usize,
) {
    Python::initialize();
    Python::attach(|py| {
        let event_loop = py
            .import("asyncio")
            .expect("asyncio")
            .call_method0("new_event_loop")
            .expect("loop");
        let shard = ShardHandle::from_event_loop(py, &event_loop, crate::config::LogFormat::Text)
            .expect("shard");
        let future = new_rust_future(py, &shard).expect("future");
        drop(shard);
        let locals = PyDict::new(py);
        locals.set_item("future", &future).expect("future local");
        locals
            .set_item("event_loop", &event_loop)
            .expect("loop local");
        py.run(c_str!("calls = []\nfuture.add_done_callback(lambda future: calls.append(future.cancelled()))"), Some(&locals), Some(&locals)).expect("callback");
        if cancel_before {
            assert!(RustFuture::cancel(future.bind(py), None).expect("cancel"));
        }
        let converted = Arc::new(AtomicUsize::new(0));
        let requeued = Arc::new(AtomicUsize::new(0));
        RustFuture::resolve(
            &future,
            py,
            ResolvePayload::Op(Box::new(ReentrantResolve {
                future: future.clone_ref(py),
                converted: Arc::clone(&converted),
                requeued: Arc::clone(&requeued),
                cancel_during_conversion: cancel_during,
            })),
        );
        assert_eq!(converted.load(Ordering::Relaxed), conversions);
        assert_eq!(requeued.load(Ordering::Relaxed), requeues);
        assert!(!RustFuture::cancel(future.bind(py), None).expect("cancel after resolution"));
        if !cancel_before && !cancel_during {
            assert_eq!(
                future
                    .get()
                    .result(py)
                    .expect("result")
                    .extract::<u32>(py)
                    .expect("integer result"),
                41
            );
        }
        py.run(
            c_str!(
                r#"
import asyncio
event_loop.run_until_complete(asyncio.sleep(0))
assert calls == [future.cancelled()], calls
event_loop.close()
"#
            ),
            Some(&locals),
            Some(&locals),
        )
        .expect("one completion callback");
    });
}
