//! Owned dictionary allocation and reusable literal keys.

#[cfg(not(any(windows, Py_LIMITED_API)))]
mod dict_api {
    use pyo3::ffi;
    use pyo3::prelude::*;
    use pyo3::types::PyDict;

    unsafe extern "C" {
        fn _PyDict_NewPresized(minused: ffi::Py_ssize_t) -> *mut ffi::PyObject;
    }

    pub fn with_capacity(py: Python<'_>, capacity: usize) -> PyResult<Bound<'_, PyDict>> {
        if capacity <= 5 {
            return Ok(PyDict::new(py));
        }

        let capacity = capacity.min(ffi::PY_SSIZE_T_MAX as usize).cast_signed();
        // SAFETY: the current thread is attached through `py`;
        // `_PyDict_NewPresized` returns a new owned dict pointer or sets a
        // Python exception; and the result is converted immediately into
        // `Bound<PyDict>` before exposure.
        unsafe {
            Bound::from_owned_ptr_or_err(py, _PyDict_NewPresized(capacity))
                .map(|dict| dict.cast_into_unchecked::<PyDict>())
        }
    }
}

#[cfg(any(windows, Py_LIMITED_API))]
mod dict_api {
    use pyo3::prelude::*;
    use pyo3::types::PyDict;

    #[expect(
        clippy::unnecessary_wraps,
        reason = "dict construction shares the fallible CPython dictionary interface"
    )]
    pub fn with_capacity(py: Python<'_>, _capacity: usize) -> PyResult<Bound<'_, PyDict>> {
        Ok(PyDict::new(py))
    }
}

/// Create an empty dictionary with an advisory expected item count.
///
/// The capacity hint never limits later insertion. The selected implementation
/// may skip presizing when the interpreter does not expose that capability.
pub use dict_api::with_capacity;

#[doc(hidden)]
#[macro_export]
macro_rules! __dict_slots {
    () => {
        0_usize
    };
    ($key:literal => $value:expr $(, $($rest:tt)*)?) => {
        1_usize + $crate::__dict_slots!($($($rest)*)?)
    };
    (if let $pattern:pat = $value:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {
        $crate::__dict_slots!($($body)*)
            + $crate::__dict_slots!($($($rest)*)?)
    };
    (if $condition:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {
        $crate::__dict_slots!($($body)*)
            + $crate::__dict_slots!($($($rest)*)?)
    };
}

/// Construct a dictionary with cached literal keys and optional fields.
///
/// Dynamic expressions run once. False conditional branches do not evaluate
/// their values. Errors propagate through the caller's `PyResult`.
#[macro_export]
macro_rules! dict {
    ($py:expr, { $($items:tt)* }) => {{
        let __py = $py;
        let __dict =
            $crate::dict::with_capacity(__py, $crate::__dict_slots!($($items)*))?;
        $crate::dict!(@push __dict, __py; $($items)*);
        __dict
    }};
    (@push $dict:ident, $py:ident;) => {};
    (@push $dict:ident, $py:ident; $key:literal => true $(, $($rest:tt)*)?) => {{
        $crate::pyo3::types::PyDictMethods::set_item(&$dict, $crate::pyo3::intern!($py, $key), true)?;
        $crate::dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push $dict:ident, $py:ident; $key:literal => false $(, $($rest:tt)*)?) => {{
        $crate::pyo3::types::PyDictMethods::set_item(&$dict, $crate::pyo3::intern!($py, $key), false)?;
        $crate::dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push $dict:ident, $py:ident; $key:literal => $value:literal $(, $($rest:tt)*)?) => {{
        static CACHED: $crate::pyo3::sync::PyOnceLock<
            $crate::pyo3::Py<$crate::pyo3::types::PyAny>,
        > = $crate::pyo3::sync::PyOnceLock::new();
        let value = CACHED
            .get_or_try_init(
                $py,
                || -> $crate::pyo3::PyResult<$crate::pyo3::Py<$crate::pyo3::types::PyAny>> {
                    Ok(
                        $crate::literal::value($py, $value)?.unbind(),
                    )
                },
            )?
            .bind($py);
        $crate::pyo3::types::PyDictMethods::set_item(&$dict, $crate::pyo3::intern!($py, $key), value)?;
        $crate::dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push $dict:ident, $py:ident; $key:literal => $value:expr $(, $($rest:tt)*)?) => {{
        $crate::pyo3::types::PyDictMethods::set_item(&$dict, $crate::pyo3::intern!($py, $key), $value)?;
        $crate::dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push $dict:ident, $py:ident; if let $pattern:pat = $value:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {{
        if let $pattern = $value {
            $crate::dict!(@push $dict, $py; $($body)*);
        }
        $crate::dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push $dict:ident, $py:ident; if $condition:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {{
        if $condition {
            $crate::dict!(@push $dict, $py; $($body)*);
        }
        $crate::dict!(@push $dict, $py; $($($rest)*)?);
    }};
}
