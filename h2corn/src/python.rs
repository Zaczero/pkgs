#[cfg(not(any(windows, Py_LIMITED_API)))]
mod dict_api {
    use pyo3::ffi;

    use crate::python::{Bound, PyDict, PyResult, Python};

    unsafe extern "C" {
        fn _PyDict_NewPresized(minused: ffi::Py_ssize_t) -> *mut ffi::PyObject;
    }

    pub(super) fn new_dict(py: Python<'_>, capacity: usize) -> PyResult<Bound<'_, PyDict>> {
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
    use crate::python::{Bound, PyDict, PyResult, Python};

    #[expect(
        clippy::unnecessary_wraps,
        reason = "dict construction shares the fallible CPython dictionary interface"
    )]
    pub(super) fn new_dict(py: Python<'_>, _capacity: usize) -> PyResult<Bound<'_, PyDict>> {
        Ok(PyDict::new(py))
    }
}

use std::any::Any;

use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
pub(crate) use pyo3::sync::PyOnceLock;
pub(crate) use pyo3::types::{PyAny, PyBytes, PyDict, PyString};
pub(crate) use pyo3::{Py, PyResult};

// Re-export crate-root macros at the module path used by their recursive expansions.
pub(crate) use crate::{
    py_dict, py_dict_slots, py_match_cached_bytes, py_match_cached_string, py_static_key,
};

#[macro_export]
macro_rules! py_static_key {
    ($key:literal) => {{
        static KEY: $crate::python::StaticPyKey = $crate::python::StaticPyKey::new($key);
        &KEY
    }};
}

#[macro_export]
macro_rules! py_dict_slots {
    () => {
        0_usize
    };
    ($key:literal => $value:expr $(, $($rest:tt)*)?) => {
        1_usize + $crate::python::py_dict_slots!($($($rest)*)?)
    };
    (if let $pattern:pat = $value:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {
        $crate::python::py_dict_slots!($($body)*)
            + $crate::python::py_dict_slots!($($($rest)*)?)
    };
    (if $condition:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {
        $crate::python::py_dict_slots!($($body)*)
            + $crate::python::py_dict_slots!($($($rest)*)?)
    };
}

#[macro_export]
macro_rules! py_dict {
    ($py:expr, { $($items:tt)* }) => {{
        let __py = $py;
        let __dict =
            $crate::python::py_new_dict(__py, $crate::python::py_dict_slots!($($items)*))?;
        $crate::python::py_dict!(@push __dict, __py; $($items)*);
        __dict
    }};
    (@push $dict:ident, $py:ident;) => {};
    (@push $dict:ident, $py:ident; $key:literal => true $(, $($rest:tt)*)?) => {{
        $crate::python::py_dict!(
            @push_value $dict,
            $py,
            $crate::python::py_static_key!($key),
            true
        )?;
        $crate::python::py_dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push $dict:ident, $py:ident; $key:literal => false $(, $($rest:tt)*)?) => {{
        $crate::python::py_dict!(
            @push_value $dict,
            $py,
            $crate::python::py_static_key!($key),
            false
        )?;
        $crate::python::py_dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push $dict:ident, $py:ident; $key:literal => $value:literal $(, $($rest:tt)*)?) => {{
        static CACHED: $crate::python::PyOnceLock<
            $crate::python::Py<$crate::python::PyAny>,
        > = $crate::python::PyOnceLock::new();
        let value = CACHED
            .get_or_try_init(
                $py,
                || -> $crate::python::PyResult<$crate::python::Py<$crate::python::PyAny>> {
                    Ok(
                        $crate::python::py_dict_literal_value($py, $value)?.unbind(),
                    )
                },
            )?
            .bind($py);
        $crate::python::py_dict!(
            @push_bound $dict,
            $py,
            $crate::python::py_static_key!($key),
            value
        )?;
        $crate::python::py_dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push $dict:ident, $py:ident; $key:literal => $value:expr $(, $($rest:tt)*)?) => {{
        $crate::python::py_dict!(
            @push_value $dict,
            $py,
            $crate::python::py_static_key!($key),
            $value
        )?;
        $crate::python::py_dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push $dict:ident, $py:ident; if let $pattern:pat = $value:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {{
        if let $pattern = $value {
            $crate::python::py_dict!(@push $dict, $py; $($body)*);
        }
        $crate::python::py_dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push $dict:ident, $py:ident; if $condition:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {{
        if $condition {
            $crate::python::py_dict!(@push $dict, $py; $($body)*);
        }
        $crate::python::py_dict!(@push $dict, $py; $($($rest)*)?);
    }};
    (@push_value $dict:ident, $py:ident, $key:expr, $value:expr) => {{
        let __key = $key;
        let __value = ::pyo3::IntoPyObjectExt::into_bound_py_any($value, $py)?;
        __key.set_item($py, &$dict, &__value)
    }};
    (@push_bound $dict:ident, $py:ident, $key:expr, $value:expr) => {{
        let __key = $key;
        __key.set_item($py, &$dict, $value)
    }};
}

#[macro_export]
macro_rules! py_match_cached_string {
    (
        $py:expr,
        $value:expr,
        [ $($value_text:literal),* $(,)? ]
    ) => {{
        let value = $value;
        match value {
            $($value_text => ::pyo3::intern!($py, $value_text).clone(),)*
            _ => $crate::python::PyString::new($py, value),
        }
    }};
    (
        $py:expr,
        $value:expr,
        {
            $($pattern:pat => $value_text:literal,)*
            ; _ => $fallback:expr $(,)?
        }
    ) => {{
        match $value {
            $($pattern => ::pyo3::intern!($py, $value_text).clone(),)*
            _ => $fallback,
        }
    }};
}

#[macro_export]
macro_rules! py_match_cached_bytes {
    (
        $py:expr,
        $value:expr,
        [ $($value_text:literal),* $(,)? ]
    ) => {{
        let value = $value;
        match value {
            $(
                $value_text => {
                    static CACHED: $crate::python::PyOnceLock<
                        $crate::python::Py<$crate::python::PyBytes>,
                    > = $crate::python::PyOnceLock::new();
                    CACHED
                        .get_or_init($py, || {
                            #[expect(
                                clippy::string_lit_as_bytes,
                                reason = "a str literal is matched but bytes are cached; the \
                                          suggested byte-literal form cannot be spelled \
                                          generically in a macro"
                            )]
                            $crate::python::PyBytes::new($py, $value_text.as_bytes()).unbind()
                        })
                        .bind($py)
                        .clone()
                },
            )*
            _ => $crate::python::PyBytes::new($py, value.as_bytes()),
        }
    }};
    (
        $py:expr,
        $value:expr,
        {
            $($pattern:pat => $value_text:literal,)*
        }
    ) => {{
        match $value {
            $(
                $pattern => {
                    static CACHED: $crate::python::PyOnceLock<
                        $crate::python::Py<$crate::python::PyBytes>,
                    > = $crate::python::PyOnceLock::new();
                    CACHED
                        .get_or_init($py, || $crate::python::PyBytes::new($py, $value_text).unbind())
                        .bind($py)
                        .clone()
                },
            )*
        }
    }};
    (
        $py:expr,
        $value:expr,
        {
            $($pattern:pat => $value_text:literal,)*
            ; _ => $fallback:expr $(,)?
        }
    ) => {{
        match $value {
            $(
                $pattern => {
                    static CACHED: $crate::python::PyOnceLock<
                        $crate::python::Py<$crate::python::PyBytes>,
                    > = $crate::python::PyOnceLock::new();
                    CACHED
                        .get_or_init($py, || $crate::python::PyBytes::new($py, $value_text).unbind())
                        .bind($py)
                        .clone()
                },
            )*
            _ => $fallback,
        }
    }};
}

pub(crate) struct StaticPyKey {
    text: &'static str,
    cached: PyOnceLock<Py<PyString>>,
}

impl StaticPyKey {
    pub(crate) const fn new(text: &'static str) -> Self {
        Self {
            text,
            cached: PyOnceLock::new(),
        }
    }

    fn key(&self, py: Python<'_>) -> &Py<PyString> {
        self.cached
            .get_or_init(py, || PyString::intern(py, self.text).unbind())
    }

    pub(crate) fn get_item<'py>(
        &self,
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        dict.get_item(self.key(py).bind(py))
    }

    pub(crate) fn set_item<'py>(
        &self,
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<()> {
        dict.set_item(self.key(py).bind(py), value)
    }
}

pub(crate) fn py_new_dict(py: Python<'_>, capacity: usize) -> PyResult<Bound<'_, PyDict>> {
    dict_api::new_dict(py, capacity)
}

pub(crate) fn py_dict_literal_value<'py, V>(
    py: Python<'py>,
    value: V,
) -> PyResult<Bound<'py, PyAny>>
where
    V: Copy + IntoPyObject<'py> + 'static,
{
    let value_any = &value as &dyn Any;
    value_any.downcast_ref::<&'static str>().map_or_else(
        || value.into_bound_py_any(py),
        |text| Ok(PyString::intern(py, text).clone().into_any()),
    )
}

#[cfg(test)]
mod tests {
    #[cfg(Py_GIL_DISABLED)]
    use std::sync::{Arc, Barrier};

    #[cfg(Py_GIL_DISABLED)]
    use pyo3::types::PyDict;
    use pyo3::types::{
        PyAnyMethods as _, PyBool, PyBytes, PyBytesMethods as _, PyDictMethods as _,
    };
    use pyo3::{IntoPyObjectExt as _, PyResult, Python, ffi};

    use super::PyString;
    #[cfg(Py_GIL_DISABLED)]
    use super::StaticPyKey;

    fn init_python() {
        Python::initialize();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn py_dict_interns_static_string_literal_values() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let dict = py_dict!(py, {
                "type" => "http.request",
            });
            let value = dict.get_item("type")?.unwrap().cast_into::<PyString>()?;
            let interned = PyString::intern(py, "http.request");

            // SAFETY: both pointers are live Python objects bound under the current GIL.
            assert!(unsafe { ffi::Py_Is(value.as_ptr(), interned.as_ptr()) } != 0);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn py_match_cached_bytes_list_form_caches_each_literal() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let root = py_match_cached_bytes!(py, "/", ["", "/"]);
            let empty = py_match_cached_bytes!(py, "", ["", "/"]);
            let other = py_match_cached_bytes!(py, "/other", ["", "/"]);
            assert_eq!(root.as_bytes(), b"/");
            assert_eq!(empty.as_bytes(), b"");
            assert_eq!(other.as_bytes(), b"/other");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn py_dict_caches_static_bytes_literal_values() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let first = py_dict!(py, {
                "body" => b"",
            });
            let second = py_dict!(py, {
                "body" => b"",
            });
            let first_value = first.get_item("body")?.unwrap().cast_into::<PyBytes>()?;
            let second_value = second.get_item("body")?.unwrap().cast_into::<PyBytes>()?;

            // SAFETY: both pointers are live Python objects bound under the current GIL.
            assert!(unsafe { ffi::Py_Is(first_value.as_ptr(), second_value.as_ptr()) } != 0);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn py_dict_uses_python_bool_singletons_for_bool_literals() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let dict = py_dict!(py, {
                "more_body" => true,
            });
            let value = dict.get_item("more_body")?.unwrap().cast_into::<PyBool>()?;
            let expected = true.into_bound_py_any(py)?.cast_into::<PyBool>()?;

            // SAFETY: both pointers are live Python objects bound under the current GIL.
            assert!(unsafe { ffi::Py_Is(value.as_ptr(), expected.as_ptr()) } != 0);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn py_dict_omits_false_conditional_fields() {
        init_python();
        Python::attach(|py| -> PyResult<()> {
            let include = false;
            let dict = py_dict!(py, {
                "first" => 1,
                if include => {
                    "omitted" => 2,
                },
                "last" => 3,
            });
            assert_eq!(dict.len(), 2);
            assert_eq!(dict.get_item("first")?.unwrap().extract::<usize>()?, 1);
            assert!(dict.get_item("omitted")?.is_none());
            assert_eq!(dict.get_item("last")?.unwrap().extract::<usize>()?, 3);
            Ok(())
        })
        .unwrap();
    }

    #[cfg(Py_GIL_DISABLED)]
    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn static_key_lookup_survives_concurrent_dict_mutation() {
        static VALUE_KEY: StaticPyKey = StaticPyKey::new("value");
        const ITERATIONS: usize = 50_000;
        const READERS: usize = 3;

        init_python();
        let dict = Arc::new(Python::attach(|py| PyDict::new(py).unbind()));
        let barrier = Arc::new(Barrier::new(READERS + 1));

        let writer_dict = Arc::clone(&dict);
        let writer_barrier = Arc::clone(&barrier);
        let writer = std::thread::spawn(move || -> PyResult<()> {
            writer_barrier.wait();
            Python::attach(|py| {
                let dict = writer_dict.bind(py);
                let key = PyString::intern(py, "value");
                for value in 0..ITERATIONS {
                    dict.set_item(&key, value)?;
                    dict.del_item(&key)?;
                }
                Ok(())
            })
        });

        let readers = std::array::from_fn::<_, READERS, _>(|_| {
            let dict = Arc::clone(&dict);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || -> PyResult<()> {
                barrier.wait();
                Python::attach(|py| {
                    for _ in 0..ITERATIONS {
                        if let Some(value) = VALUE_KEY.get_item(py, dict.bind(py))? {
                            let _ = value.extract::<usize>()?;
                        }
                    }
                    Ok(())
                })
            })
        });

        writer
            .join()
            .expect("writer thread does not panic")
            .unwrap();
        for reader in readers {
            reader
                .join()
                .expect("reader thread does not panic")
                .unwrap();
        }
    }
}
