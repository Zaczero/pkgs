//! Native heap-type initialization and module registration.

use pyo3::types::PyTypeMethods as _;

/// Register sequence-pattern support before freezing the native type.
#[cfg_attr(
    not(Py_3_14),
    expect(
        clippy::unnecessary_wraps,
        reason = "ABC registration and freezing are fallible on Python 3.14+"
    )
)]
pub fn initialize_sequence<T: pyo3::PyClass>(
    module: &pyo3::Bound<'_, pyo3::types::PyModule>,
) -> pyo3::PyResult<()> {
    const {
        assert!(T::IS_SEQUENCE);
    }
    let ty = module.py().get_type::<T>();
    #[cfg(Py_3_14)]
    {
        pyo3::types::PySequence::register::<T>(module.py())?;
        // SAFETY: PyO3 owns this initialized heap type; its base is immutable.
        if unsafe { pyo3::ffi::PyType_Freeze(ty.as_type_ptr()) } < 0 {
            return Err(pyo3::PyErr::fetch(module.py()));
        }
    }
    // SAFETY: the type is not yet published. Python <3.14 cannot freeze it
    // after ABC registration, so repair PyO3's missing sequence-pattern flag.
    #[cfg(not(Py_3_14))]
    unsafe {
        #[cfg(not(Py_GIL_DISABLED))]
        {
            (*ty.as_type_ptr()).tp_flags |= pyo3::ffi::Py_TPFLAGS_SEQUENCE;
        }
        #[cfg(Py_GIL_DISABLED)]
        (*ty.as_type_ptr()).tp_flags.fetch_or(
            pyo3::ffi::Py_TPFLAGS_SEQUENCE,
            core::sync::atomic::Ordering::Relaxed,
        );
    }
    Ok(())
}
/// Register a list of `#[pyclass]` types on the module in one statement.
#[macro_export]
macro_rules! add_classes {
    ($m:ident; $($class:ty),+ $(,)?) => {
        $($m.add_class::<$class>()?;)+
    };
}
/// Register a list of `#[pyfunction]`s on the module in one statement.
#[macro_export]
macro_rules! add_functions {
    ($m:ident; $($function:path),+ $(,)?) => {
        $($m.add_function($crate::pyo3::wrap_pyfunction!($function, $m)?)?;)+
    };
}
