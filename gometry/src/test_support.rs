use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::{Mutex, MutexGuard};

use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::prelude::*;

pub(crate) struct CountingAllocator;

thread_local! {
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static FAIL_AFTER: Cell<Option<usize>> = const { Cell::new(None) };
}

fn reject_allocation() -> bool {
    FAIL_AFTER.with(|remaining| match remaining.get() {
        None => false,
        Some(0) => true,
        Some(count) => {
            remaining.set(Some(count - 1));
            false
        },
    })
}

fn count_allocation() {
    COUNT_ALLOCATIONS.with(|enabled| {
        if enabled.get() {
            ALLOCATIONS.with(|allocations| allocations.set(allocations.get() + 1));
        }
    });
}

// SAFETY: requests either fail with null or delegate to System unchanged.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        if reject_allocation() {
            return std::ptr::null_mut();
        }
        // SAFETY: `layout` is supplied by the `GlobalAlloc` caller unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        if reject_allocation() {
            return std::ptr::null_mut();
        }
        // SAFETY: `layout` is supplied by the `GlobalAlloc` caller unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `GlobalAlloc` supplies the original pointer and layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation();
        if reject_allocation() {
            return std::ptr::null_mut();
        }
        // SAFETY: `GlobalAlloc` supplies the original allocation arguments.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

struct AllocationCountGuard;

impl Drop for AllocationCountGuard {
    fn drop(&mut self) {
        COUNT_ALLOCATIONS.with(|enabled| enabled.set(false));
    }
}

/// Count allocation requests from one test operation on the current thread.
pub(crate) fn allocation_count<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATIONS.with(|allocations| allocations.set(0));
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(true));
    let guard = AllocationCountGuard;
    let result = operation();
    drop(guard);
    (result, ALLOCATIONS.with(Cell::get))
}

/// Reject this thread's allocations persistently after the allowed prefix.
pub(crate) fn fail_allocations_after<T>(count: usize, operation: impl FnOnce() -> T) -> T {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            FAIL_AFTER.with(|remaining| remaining.set(self.0));
        }
    }
    let guard = Restore(FAIL_AFTER.with(|remaining| remaining.replace(Some(count))));
    let result = operation();
    drop(guard);
    result
}

#[pyclass(module = "gometry_test")]
pub(crate) struct NativeBoundaryIterator {
    item: Py<PyAny>,
    remaining: usize,
    calls: usize,
    interrupt_at: Option<usize>,
    error_at: Option<usize>,
    error: Option<Py<PyAny>>,
}

#[pymethods]
impl NativeBoundaryIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let call = self.calls;
        self.calls = self.calls.wrapping_add(1);
        if self.error_at == Some(call) {
            return Err(PyErr::from_value(
                self.error
                    .as_ref()
                    .expect("error_at requires an exception")
                    .bind(py)
                    .clone(),
            ));
        }
        self.remaining -= 1;
        if self.interrupt_at == Some(call) {
            // Queue the interrupt while this native protocol method still owns
            // control; the consumer's explicit checkpoint observes it later.
            unsafe { pyo3::ffi::PyErr_SetInterrupt() };
        }
        Ok(Some(self.item.clone_ref(py)))
    }
}

impl NativeBoundaryIterator {
    pub(crate) fn calls(&self) -> usize {
        self.calls
    }

    pub(crate) fn remaining(&self) -> usize {
        self.remaining
    }
}

pub(crate) fn native_boundary_iterator(
    py: Python<'_>,
    item: &Bound<'_, PyAny>,
    remaining: usize,
    interrupt_at: Option<usize>,
    sentinel: Option<(usize, Py<PyAny>)>,
) -> PyResult<Py<NativeBoundaryIterator>> {
    let (error_at, error) = sentinel.map_or((None, None), |(at, error)| (Some(at), Some(error)));
    Py::new(py, NativeBoundaryIterator {
        item: item.clone().unbind(),
        remaining,
        calls: 0,
        interrupt_at,
        error_at,
        error,
    })
}

static PYTHON_SIGNAL_TEST_LOCK: Mutex<()> = Mutex::new(());

pub(crate) struct PythonSignalTestGuard {
    _lock: MutexGuard<'static, ()>,
    previous_handler: Py<PyAny>,
}

impl PythonSignalTestGuard {
    pub(crate) fn new(py: Python<'_>) -> Self {
        let lock = PYTHON_SIGNAL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let signal = py.import("signal").expect("import signal for test");
        let sigint = signal.getattr("SIGINT").expect("read SIGINT");
        let default_handler = signal
            .getattr("default_int_handler")
            .expect("read default SIGINT handler");
        let previous_handler = signal
            .call_method1("signal", (sigint, default_handler))
            .expect("install default SIGINT handler")
            .unbind();

        let guard = Self {
            _lock: lock,
            previous_handler,
        };

        // Positive control: verify this interpreter/thread observes the native
        // pending-interrupt API before any owner-specific exercise begins.
        unsafe { pyo3::ffi::PyErr_SetInterrupt() };
        let error = py
            .check_signals()
            .expect_err("PyErr_SetInterrupt must reach the handler");
        assert!(error.is_instance_of::<PyKeyboardInterrupt>(py));
        guard
    }
}

impl Drop for PythonSignalTestGuard {
    fn drop(&mut self) {
        Python::attach(|py| {
            // Clear a signal left by a failing assertion before restoring the
            // caller's handler, so it cannot poison the next embedded test.
            let _ = py.check_signals();
            if let Ok(signal) = py.import("signal")
                && let Ok(sigint) = signal.getattr("SIGINT")
            {
                let _ = signal.call_method1("signal", (sigint, self.previous_handler.bind(py)));
            }
        });
    }
}

pub(crate) fn initialize_python() {
    Python::initialize();
    Python::attach(|py| {
        // Embedded interpreters do not inherit the launcher-configured module
        // search path, so install it before importing NumPy.
        if let Ok(paths) = std::env::var("PYTHONPATH") {
            let sys = PyModule::import(py, "sys").expect("import sys in embedded Python");
            let path = sys
                .getattr("path")
                .expect("read sys.path in embedded Python");
            for entry in paths.split(':').filter(|entry| !entry.is_empty()) {
                path.call_method1("append", (entry,))
                    .expect("append PYTHONPATH entry to embedded sys.path");
            }
        }
        py.import("numpy").unwrap_or_else(|error| {
            panic!(
                "gometry's Rust tests require NumPy in the embedded Python interpreter. \
                 Install NumPy into the interpreter selected by PYO3_PYTHON, for example: \
                 uv pip install --python \"$PYO3_PYTHON\" numpy. \
                 Original Python error: {error}"
            )
        });
    });
}
