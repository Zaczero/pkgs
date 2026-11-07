//! Vendored allocation-cache regressions through ordinary pyclass construction.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pyo3::gc::{PyTraverseError, PyVisit};
use pyo3::impl_::freelist::FreeListStorage as _;
use pyo3::impl_::pyclass::{PyClassImpl as _, PyClassWithFreeList};
use pyo3::prelude::*;

struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[pyclass(freelist = 2, dict, weakref, subclass)]
struct Cached {
    #[pyo3(get, set)]
    link: Option<Py<PyAny>>,
    #[pyo3(get)]
    value: usize,
    _drop: DropProbe,
}

#[pymethods]
impl Cached {
    #[new]
    fn new(value: usize) -> Self {
        Self {
            link: None,
            value,
            _drop: DropProbe(Arc::new(AtomicUsize::new(0))),
        }
    }

    #[expect(
        clippy::same_name_method,
        reason = "PyO3's GC protocol fixes this name"
    )]
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.link)
    }

    fn __clear__(&mut self) {
        self.link = None;
    }
}

fn new_cached<'py>(py: Python<'py>, drops: &Arc<AtomicUsize>, value: usize) -> Bound<'py, Cached> {
    Bound::new(py, Cached {
        link: None,
        value,
        _drop: DropProbe(Arc::clone(drops)),
    })
    .expect("construct cached instance")
}

fn assert_cached<T: PyClassWithFreeList>(py: Python<'_>, address: *mut pyo3::ffi::PyObject) {
    let cache = T::get_free_list();
    if !T::HAS_FREELIST {
        assert!(cache.pop(py).is_none());
        return;
    }
    let allocation = cache
        .pop(py)
        .expect("destroyed allocation entered its class cache");
    assert_eq!(allocation.as_ptr(), address);
    // SAFETY: this allocation was just removed from this class's own cache.
    assert!(unsafe { cache.try_insert(py, allocation) }.is_none());
}

#[test]
#[cfg_attr(miri, ignore = "requires the CPython allocator and collector")]
fn reused_allocations_are_tracked_and_collect_cycles() {
    Python::initialize();
    Python::attach(|py| {
        let gc = py.import("gc").unwrap();
        let weakref = py.import("weakref").unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        for (lifetime, value) in [37, 91, 123].into_iter().enumerate() {
            let prime = new_cached(py, &drops, 456);
            let address = prime.as_ptr();
            prime.setattr("old_lifetime", 789).unwrap();
            let old_weak = weakref.call_method1("ref", (&prime,)).unwrap();
            drop(prime);
            assert_cached::<Cached>(py, address);
            assert!(old_weak.call0().unwrap().is_none());

            let reused = new_cached(py, &drops, value);
            if Cached::HAS_FREELIST {
                assert_eq!(reused.as_ptr(), address, "exercise a real freelist hit");
            }
            assert_eq!(reused.borrow().value, value);
            assert_eq!(reused.getattr("__dict__").unwrap().len().unwrap(), 0);
            assert!(
                gc.call_method1("is_tracked", (&reused,))
                    .unwrap()
                    .is_truthy()
                    .unwrap()
            );
            let weak = weakref.call_method1("ref", (&reused,)).unwrap();
            reused.borrow_mut().link = Some(reused.clone().into_any().unbind());
            drop(reused);
            gc.call_method0("collect").unwrap();
            assert!(weak.call0().unwrap().is_none());
            assert_eq!(drops.load(Ordering::Relaxed), 2 * (lifetime + 1));
        }
    });
}

#[test]
#[cfg(not(Py_GIL_DISABLED))]
#[cfg_attr(miri, ignore = "requires the CPython allocator")]
fn declared_capacity_one_reuses_the_only_slot() {
    #[pyclass(freelist = 1)]
    struct One;
    Python::initialize();
    Python::attach(|py| {
        let first = Bound::new(py, One).unwrap();
        let address = first.as_ptr();
        drop(first);
        assert_cached::<One>(py, address);
        let second = Bound::new(py, One).unwrap();
        if One::HAS_FREELIST {
            assert_eq!(second.as_ptr(), address);
        }
    });
}

#[test]
#[cfg_attr(miri, ignore = "requires the CPython allocator and collector")]
fn derived_reuse_initializes_every_rust_base() {
    #[pyclass(extends = Cached, freelist = 2)]
    struct Derived {
        marker: String,
    }

    #[pymethods]
    impl Derived {
        #[expect(
            clippy::unnecessary_wraps,
            clippy::panic_in_result_fn,
            clippy::same_name_method,
            reason = "PyO3 fixes the GC protocol signature; the test assertion detects traversal before initialization"
        )]
        fn __traverse__(&self, _visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
            assert_eq!(self.marker, "initialized");
            Ok(())
        }
    }

    Python::initialize();
    Python::attach(|py| {
        let gc = py.import("gc").unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut previous = None;
        for value in [37, 91, 123] {
            let object = Bound::new(
                py,
                PyClassInitializer::from(Cached {
                    link: None,
                    value,
                    _drop: DropProbe(Arc::clone(&drops)),
                })
                .add_subclass(Derived {
                    marker: "initialized".to_owned(),
                }),
            )
            .unwrap();
            if Derived::HAS_FREELIST
                && let Some(previous) = previous
            {
                assert_eq!(object.as_ptr(), previous);
            }
            previous = Some(object.as_ptr());
            assert_eq!(object.borrow().as_super().value, value);
            assert!(
                gc.call_method1("is_tracked", (&object,))
                    .unwrap()
                    .is_truthy()
                    .unwrap()
            );
            let address = object.as_ptr();
            drop(object);
            assert_cached::<Derived>(py, address);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    });
}

#[test]
#[cfg(any(not(Py_LIMITED_API), Py_3_12))]
#[cfg_attr(miri, ignore = "requires CPython type and instance allocation")]
fn native_exception_base_is_reinitialized_on_reuse() {
    #[pyclass(extends = pyo3::exceptions::PyException, freelist = 2)]
    struct CachedException {
        value: usize,
    }
    Python::initialize();
    Python::attach(|py| {
        let mut previous = None;
        for value in [37, 91, 123] {
            let object = Bound::new(py, CachedException { value }).unwrap();
            if CachedException::HAS_FREELIST
                && let Some(previous) = previous
            {
                assert_eq!(object.as_ptr(), previous);
            }
            previous = Some(object.as_ptr());
            assert_eq!(object.borrow().value, value);
            assert_eq!(object.getattr("args").unwrap().len().unwrap(), 0);
            assert_eq!(object.getattr("__dict__").unwrap().len().unwrap(), 0);
            object.setattr("old_lifetime", value).unwrap();
            let address = object.as_ptr();
            drop(object);
            assert_cached::<CachedException>(py, address);
        }
    });
}

#[test]
#[cfg_attr(miri, ignore = "requires CPython's live type slots")]
fn backend_selection_preserves_ordinary_allocator_hooks() {
    #[pyclass]
    struct Plain;
    #[pyclass(freelist = 0)]
    struct Disabled;
    #[pyclass(freelist = 8)]
    struct Enabled;

    fn slots<T: pyo3::PyClass>(py: Python<'_>) -> [*mut std::ffi::c_void; 2] {
        let ty = py.get_type::<T>();
        // SAFETY: `ty` retains this initialized type, and both are valid type slots.
        unsafe {
            [
                pyo3::ffi::PyType_GetSlot(ty.as_ptr().cast(), pyo3::ffi::Py_tp_alloc),
                pyo3::ffi::PyType_GetSlot(ty.as_ptr().cast(), pyo3::ffi::Py_tp_free),
            ]
        }
    }

    Python::initialize();
    Python::attach(|py| {
        assert_eq!(slots::<Disabled>(py), slots::<Plain>(py));
        if cfg!(Py_GIL_DISABLED) {
            assert_eq!(slots::<Enabled>(py), slots::<Plain>(py));
        } else {
            assert_ne!(slots::<Enabled>(py), slots::<Plain>(py));
        }
    });
}

#[test]
#[cfg_attr(miri, ignore = "requires CPython destruction and reentrant allocation")]
fn destructor_can_allocate_the_same_class() {
    #[pyclass(freelist = 1)]
    struct Reentrant {
        allocate: bool,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Reentrant {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
            if self.allocate {
                Python::attach(|py| {
                    drop(
                        Bound::new(py, Self {
                            allocate: false,
                            drops: Arc::clone(&self.drops),
                        })
                        .expect("reentrant allocation"),
                    );
                });
            }
        }
    }
    Python::initialize();
    let drops = Arc::new(AtomicUsize::new(0));
    Python::attach(|py| {
        for expected_drops in [2, 4, 6] {
            drop(
                Bound::new(py, Reentrant {
                    allocate: true,
                    drops: Arc::clone(&drops),
                })
                .unwrap(),
            );
            assert_eq!(drops.load(Ordering::Relaxed), expected_drops);
        }
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "requires cross-thread CPython allocation and destruction"
)]
fn concurrent_reuse_can_be_destroyed_on_another_thread() {
    Python::initialize();
    let drops = Arc::new(AtomicUsize::new(0));
    let objects = std::thread::scope(|scope| {
        // Start every worker before joining; a lazy spawn/join chain serializes them.
        let mut handles = Vec::with_capacity(4);
        for worker in 0..4 {
            let drops = &drops;
            handles.push(scope.spawn(move || {
                Python::attach(|py| {
                    for value in 0..64 {
                        let object = new_cached(py, drops, value + worker * 100);
                        assert_eq!(object.borrow().value, value + worker * 100);
                    }
                    new_cached(py, drops, worker).unbind()
                })
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    Python::attach(|_py| drop(objects));
    assert_eq!(drops.load(Ordering::Relaxed), 4 * 65);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "requires Python subclass layouts and weakref callbacks"
)]
fn python_subclass_and_reentrant_weakref_callback_keep_their_lifetimes() {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "freelist_lifetimes").unwrap();
        module.add("Cached", py.get_type::<Cached>()).unwrap();
        py.run(
            cr"import gc, weakref
class Derived(Cached):
    __slots__ = ('extra',)
allocated = []
for value in (37, 91, 123):
    base = Cached(456)
    del base
    obj = Derived(value)
    obj.extra = 'derived state'
    obj.link = obj
    ref = weakref.ref(obj, lambda _: allocated.append(Cached(789)))
    assert obj.value == value
    del obj
    gc.collect()
    assert ref() is None
    assert allocated[-1].value == 789
assert len(allocated) == 3
",
            Some(&module.dict()),
            None,
        )
        .unwrap();
    });
}

#[test]
#[cfg(not(Py_GIL_DISABLED))]
#[cfg_attr(miri, ignore = "requires the CPython reference-count implementation")]
fn reuse_balances_heap_type_references() {
    #[pyclass(freelist = 1)]
    struct ReferenceCounted;
    Python::initialize();
    Python::attach(|py| {
        let ty = py.get_type::<ReferenceCounted>();
        drop(Bound::new(py, ReferenceCounted).unwrap());
        // SAFETY: `ty` retains this live type; the GIL excludes concurrent changes.
        let before = unsafe { pyo3::ffi::Py_REFCNT(ty.as_ptr()) };
        for _ in 0..1000 {
            drop(Bound::new(py, ReferenceCounted).unwrap());
        }
        // SAFETY: the same retained type and GIL-held reference-count contract apply.
        assert_eq!(unsafe { pyo3::ffi::Py_REFCNT(ty.as_ptr()) }, before);
    });
}
