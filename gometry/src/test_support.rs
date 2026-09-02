use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use pyo3::prelude::*;

pub(crate) struct CountingAllocator;

thread_local! {
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

fn count_allocation() {
    COUNT_ALLOCATIONS.with(|enabled| {
        if enabled.get() {
            ALLOCATIONS.with(|allocations| allocations.set(allocations.get() + 1));
        }
    });
}

// SAFETY: every allocator operation delegates to `System` with its original arguments.
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
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(true));
    let guard = AllocationCountGuard;
    let result = operation();
    drop(guard);
    (result, ALLOCATIONS.with(Cell::get))
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
