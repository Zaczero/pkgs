use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::{Mutex, MutexGuard};

use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::prelude::*;
use pyo3::sync::MutexExt as _;

pub(crate) struct CountingAllocator;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AllocationPhase {
    Admission,
    SourceStaging,
    CandidatePairs,
    OverlapContacts,
    TopologyFaces,
    Publication,
}

impl AllocationPhase {
    const COUNT: usize = 6;

    const fn index(self) -> usize {
        match self {
            Self::Admission => 0,
            Self::SourceStaging => 1,
            Self::CandidatePairs => 2,
            Self::OverlapContacts => 3,
            Self::TopologyFaces => 4,
            Self::Publication => 5,
        }
    }
}

thread_local! {
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static ALLOCATION_PHASE: Cell<Option<AllocationPhase>> = const { Cell::new(None) };
    static PHASE_ALLOCATIONS: Cell<[usize; AllocationPhase::COUNT]> =
        const { Cell::new([0; AllocationPhase::COUNT]) };
}

fn count_allocation() {
    COUNT_ALLOCATIONS.with(|enabled| {
        if enabled.get() {
            ALLOCATIONS.with(|allocations| allocations.set(allocations.get().saturating_add(1)));
            ALLOCATION_PHASE.with(|phase| {
                if let Some(phase) = phase.get() {
                    PHASE_ALLOCATIONS.with(|allocations| {
                        let mut current = allocations.get();
                        let index = phase.index();
                        current[index] = current[index].saturating_add(1);
                        allocations.set(current);
                    });
                }
            });
        }
    });
}

// SAFETY: every request delegates to System with unchanged allocation arguments.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        // SAFETY: `layout` is supplied by the `GlobalAlloc` caller unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        // SAFETY: `layout` is supplied by the `GlobalAlloc` caller unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `GlobalAlloc` supplies the original pointer and layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation();
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
    PHASE_ALLOCATIONS.with(|allocations| allocations.set([0; AllocationPhase::COUNT]));
    ALLOCATION_PHASE.with(|phase| phase.set(None));
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(true));
    let guard = AllocationCountGuard;
    let result = operation();
    drop(guard);
    (result, ALLOCATIONS.with(Cell::get))
}

pub(crate) fn assert_no_allocations<T>(operation: impl FnOnce() -> T) -> T {
    let (result, allocations) = allocation_count(operation);
    assert_eq!(allocations, 0);
    result
}

/// Attribute subsequent counted allocations to one production phase.
pub(crate) fn allocation_phase(phase: AllocationPhase) -> AllocationPhaseGuard {
    AllocationPhaseGuard(ALLOCATION_PHASE.with(|current| current.replace(Some(phase))))
}

pub(crate) struct AllocationPhaseGuard(Option<AllocationPhase>);

impl Drop for AllocationPhaseGuard {
    fn drop(&mut self) {
        ALLOCATION_PHASE.with(|phase| phase.set(self.0));
    }
}

/// Return allocation requests attributed to the six coverage lifecycle phases.
pub(crate) fn allocation_phase_counts() -> [usize; AllocationPhase::COUNT] {
    PHASE_ALLOCATIONS.with(Cell::get)
}

#[cfg(debug_assertions)]
pub(crate) mod exact_operation_counts {
    use super::Cell;

    #[derive(Clone, Copy, Debug, Default)]
    pub(crate) struct Counts {
        pub(crate) conversions: usize,
        pub(crate) ratio_roundings: usize,
    }

    thread_local! {
        static COUNTS: Cell<Option<Counts>> = const { Cell::new(None) };
    }

    pub(crate) fn conversion() {
        COUNTS.with(|cell| {
            if let Some(mut counts) = cell.get() {
                counts.conversions = counts.conversions.wrapping_add(1);
                cell.set(Some(counts));
            }
        });
    }

    pub(crate) fn ratio_rounding() {
        COUNTS.with(|cell| {
            if let Some(mut counts) = cell.get() {
                counts.ratio_roundings = counts.ratio_roundings.wrapping_add(1);
                cell.set(Some(counts));
            }
        });
    }

    struct Guard(Option<Counts>);

    impl Drop for Guard {
        fn drop(&mut self) {
            COUNTS.with(|cell| cell.set(self.0));
        }
    }

    pub(crate) fn measure<T>(operation: impl FnOnce() -> T) -> (T, Counts) {
        let guard = Guard(COUNTS.with(|cell| cell.replace(Some(Counts::default()))));
        let result = operation();
        let counts = COUNTS.with(|cell| cell.get().expect("exact operation counting is active"));
        drop(guard);
        (result, counts)
    }
}

/// Counters for the binary admission contract. They are deliberately test-only:
/// production dispatch has no observable registry or diagnostic surface for
/// these implementation choices.
pub(crate) mod binary_dispatch_counters {
    use super::*;

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub(crate) struct Snapshot {
        pub(crate) classifications: usize,
        pub(crate) paired_array_validations: usize,
        pub(crate) frame_resolutions: usize,
        pub(crate) metric_context_acquisitions: usize,
        pub(crate) runtime_state_snapshots: usize,
        pub(crate) fast_path_attempts: usize,
        pub(crate) fast_path_taken: usize,
        pub(crate) fast_path_declines: usize,
        pub(crate) general_lane_runs: usize,
        #[cfg(debug_assertions)]
        pub(crate) point_predicate_dispatches: usize,
    }

    thread_local! {
        static COUNTERS: Cell<Snapshot> = const {
            Cell::new(Snapshot {
                classifications: 0,
                paired_array_validations: 0,
                frame_resolutions: 0,
                metric_context_acquisitions: 0,
                runtime_state_snapshots: 0,
                fast_path_attempts: 0,
                fast_path_taken: 0,
                fast_path_declines: 0,
                general_lane_runs: 0,
                #[cfg(debug_assertions)]
                point_predicate_dispatches: 0,
            })
        };
    }

    pub(crate) fn reset() {
        COUNTERS.with(|counters| counters.set(Snapshot::default()));
    }

    pub(crate) fn snapshot() -> Snapshot {
        COUNTERS.with(Cell::get)
    }

    fn update(update: impl FnOnce(&mut Snapshot)) {
        COUNTERS.with(|counters| {
            let mut snapshot = counters.get();
            update(&mut snapshot);
            counters.set(snapshot);
        });
    }

    pub(crate) fn classification() {
        update(|snapshot| snapshot.classifications = snapshot.classifications.saturating_add(1));
    }

    pub(crate) fn paired_array_validation() {
        update(|snapshot| {
            snapshot.paired_array_validations = snapshot.paired_array_validations.saturating_add(1);
        });
    }

    pub(crate) fn frame_resolution() {
        update(|snapshot| {
            snapshot.frame_resolutions = snapshot.frame_resolutions.saturating_add(1);
        });
    }

    pub(crate) fn metric_context_acquisition() {
        update(|snapshot| {
            snapshot.metric_context_acquisitions =
                snapshot.metric_context_acquisitions.saturating_add(1);
        });
    }

    pub(crate) fn runtime_state_snapshot() {
        update(|snapshot| {
            snapshot.runtime_state_snapshots = snapshot.runtime_state_snapshots.saturating_add(1);
        });
    }

    pub(crate) fn fast_path_attempt() {
        update(|snapshot| {
            snapshot.fast_path_attempts = snapshot.fast_path_attempts.saturating_add(1);
        });
    }

    pub(crate) fn fast_path_taken() {
        update(|snapshot| snapshot.fast_path_taken = snapshot.fast_path_taken.saturating_add(1));
    }

    pub(crate) fn fast_path_declined() {
        update(|snapshot| {
            snapshot.fast_path_declines = snapshot.fast_path_declines.saturating_add(1);
        });
    }

    pub(crate) fn general_lane() {
        update(|snapshot| {
            snapshot.general_lane_runs = snapshot.general_lane_runs.saturating_add(1);
        });
    }

    #[cfg(debug_assertions)]
    pub(crate) fn point_predicate_dispatch() {
        update(|snapshot| {
            snapshot.point_predicate_dispatches =
                snapshot.point_predicate_dispatches.saturating_add(1);
        });
    }
}

#[pyclass(module = "gometry_test")]
pub(crate) struct NativeBoundaryIterator {
    item: Py<PyAny>,
    remaining: usize,
    calls: usize,
    interrupt_at: Option<usize>,
    error: Option<(usize, Py<PyAny>)>,
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
        if let Some((error_at, error)) = self.error.as_ref()
            && *error_at == call
        {
            return Err(PyErr::from_value(error.bind(py).clone()));
        }
        self.remaining = self
            .remaining
            .checked_sub(1)
            .expect("remaining was checked nonzero");
        if self.interrupt_at == Some(call) {
            // Queue the interrupt while this native protocol method still owns
            // control; the consumer's explicit checkpoint observes it later.
            // SAFETY: `py` proves this thread holds a live interpreter; the
            // CPython pending-interrupt flag is process-global and does not
            // borrow this token beyond the call.
            unsafe {
                pyo3::ffi::PyErr_SetInterrupt();
            }
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
    Py::new(py, NativeBoundaryIterator {
        item: item.clone().unbind(),
        remaining,
        calls: 0,
        interrupt_at,
        error: sentinel,
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
            .lock_py_attached(py)
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
        // SAFETY: `py` proves this thread holds a live interpreter; the
        // CPython pending-interrupt flag is process-global and does not
        // borrow this token beyond the call.
        unsafe {
            pyo3::ffi::PyErr_SetInterrupt();
        }
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
            drop(py.check_signals());
            if let Ok(signal) = py.import("signal")
                && let Ok(sigint) = signal.getattr("SIGINT")
            {
                drop(signal.call_method1("signal", (sigint, self.previous_handler.bind(py))));
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
