#[cfg(not(any(PyPy, GraalPy, Py_LIMITED_API)))]
mod dict_api {
    use std::ffi::c_int;

    use pyo3::{PyErr, ffi};

    use super::*;

    pub(super) type CachedKey = (Py<PyString>, ffi::Py_hash_t);

    unsafe extern "C" {
        fn _PyDict_NewPresized(minused: ffi::Py_ssize_t) -> *mut ffi::PyObject;
        #[cfg(not(Py_GIL_DISABLED))]
        fn _PyDict_GetItem_KnownHash(
            mp: *mut ffi::PyObject,
            key: *mut ffi::PyObject,
            hash: ffi::Py_hash_t,
        ) -> *mut ffi::PyObject;
        fn _PyDict_SetItem_KnownHash(
            mp: *mut ffi::PyObject,
            key: *mut ffi::PyObject,
            value: *mut ffi::PyObject,
            hash: ffi::Py_hash_t,
        ) -> c_int;
    }

    pub(super) fn cache_key(py: Python<'_>, text: &'static str) -> PyResult<CachedKey> {
        let key = PyString::intern(py, text).unbind();
        // SAFETY: `key` is a live Python object and the current thread is attached
        // to the interpreter through `py`.
        let hash = unsafe { ffi::PyObject_Hash(key.as_ptr()) };
        if hash == -1 {
            Err(PyErr::fetch(py))
        } else {
            Ok((key, hash))
        }
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

    #[cfg(not(Py_GIL_DISABLED))]
    pub(super) fn get_item<'py>(
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
        (key, hash): &CachedKey,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        let key = key.bind(py);
        // SAFETY: the GIL is held by `py`; `dict` and `key` are live bound Python
        // objects; the cached hash was computed for this exact interned key.
        let value = unsafe { _PyDict_GetItem_KnownHash(dict.as_ptr(), key.as_ptr(), *hash) };
        if value.is_null() {
            // SAFETY: the GIL is held by `py`; checking the current Python error indicator
            // is valid.
            if unsafe { ffi::PyErr_Occurred().is_null() } {
                Ok(None)
            } else {
                Err(PyErr::fetch(py))
            }
        } else {
            // SAFETY: `_PyDict_GetItem_KnownHash` returns a borrowed reference from `dict`,
            // which is bound to the same GIL lifetime as `py`.
            Ok(Some(unsafe { Bound::from_borrowed_ptr(py, value) }))
        }
    }

    #[cfg(Py_GIL_DISABLED)]
    pub(super) fn get_item<'py>(
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
        (key, _hash): &CachedKey,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        use pyo3::types::PyDictMethods;

        // A borrowed dict value can be invalidated by another thread between
        // lookup and INCREF on free-threaded CPython. PyO3's safe path uses
        // `PyDict_GetItemRef`, which returns a strong reference atomically with
        // the lookup. The cached interned key still avoids key construction and
        // its hash is already cached by CPython.
        dict.get_item(key.bind(py))
    }

    pub(super) fn set_item<'py>(
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
        (key, hash): &CachedKey,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<()> {
        let key = key.bind(py);
        // SAFETY: the current thread is attached through `py`; `dict`, `key`,
        // and `value` are live bound Python objects; the cached hash was
        // computed for this exact interned key. CPython's function enters the
        // dict critical section on free-threaded builds.
        if unsafe { _PyDict_SetItem_KnownHash(dict.as_ptr(), key.as_ptr(), value.as_ptr(), *hash) }
            == -1
        {
            Err(PyErr::fetch(py))
        } else {
            Ok(())
        }
    }
}

#[cfg(any(PyPy, GraalPy, Py_LIMITED_API))]
mod dict_api {
    use pyo3::types::PyDictMethods;

    use super::*;

    pub(super) type CachedKey = Py<PyString>;

    pub(super) fn cache_key(py: Python<'_>, text: &'static str) -> PyResult<CachedKey> {
        Ok(PyString::intern(py, text).unbind())
    }

    pub(super) fn new_dict(py: Python<'_>, _capacity: usize) -> PyResult<Bound<'_, PyDict>> {
        Ok(PyDict::new(py))
    }

    pub(super) fn get_item<'py>(
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
        key: &CachedKey,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        dict.get_item(key.bind(py))
    }

    pub(super) fn set_item<'py>(
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
        key: &CachedKey,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<()> {
        dict.set_item(key.bind(py), value)
    }
}

use std::any::Any;
use std::mem::MaybeUninit;
use std::slice::from_raw_parts;

use pyo3::prelude::*;
pub(crate) use pyo3::sync::PyOnceLock;
pub(crate) use pyo3::types::{PyAny, PyBytes, PyDict, PyString};
use pyo3::{IntoPyObject, IntoPyObjectExt};
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
        let mut __scratch = $crate::python::PyDictScratch::<
            { $crate::python::py_dict_slots!($($items)*) }
        >::new($py);
        $crate::python::py_dict!(@push __scratch, 0_usize; $($items)*);
        __scratch.finish()?
    }};
    (@push $scratch:ident, $index:expr;) => {};
    (@push $scratch:ident, $index:expr; $key:literal => true $(, $($rest:tt)*)?) => {{
        // SAFETY: macro expansion assigns each lexical field one static slot
        // and visits it at most once.
        unsafe { $scratch.push_at::<{ $index }, _>($crate::python::py_static_key!($key), true)? };
        $crate::python::py_dict!(@push $scratch, $index + 1_usize; $($($rest)*)?);
    }};
    (@push $scratch:ident, $index:expr; $key:literal => false $(, $($rest:tt)*)?) => {{
        // SAFETY: macro expansion assigns each lexical field one static slot
        // and visits it at most once.
        unsafe { $scratch.push_at::<{ $index }, _>($crate::python::py_static_key!($key), false)? };
        $crate::python::py_dict!(@push $scratch, $index + 1_usize; $($($rest)*)?);
    }};
    (@push $scratch:ident, $index:expr; $key:literal => $value:literal $(, $($rest:tt)*)?) => {{
        static CACHED: $crate::python::PyOnceLock<
            $crate::python::Py<$crate::python::PyAny>,
        > = $crate::python::PyOnceLock::new();
        let value = CACHED
            .get_or_try_init(
                $scratch.py(),
                || -> $crate::python::PyResult<$crate::python::Py<$crate::python::PyAny>> {
                    Ok(
                        $crate::python::py_dict_literal_value($scratch.py(), $value)?
                        .unbind(),
                    )
                },
            )?
            .bind($scratch.py())
            .clone();
        // SAFETY: macro expansion assigns each lexical field one static slot
        // and visits it at most once.
        unsafe { $scratch.push_bound_at::<{ $index }>($crate::python::py_static_key!($key), value) };
        $crate::python::py_dict!(@push $scratch, $index + 1_usize; $($($rest)*)?);
    }};
    (@push $scratch:ident, $index:expr; $key:literal => $value:expr $(, $($rest:tt)*)?) => {{
        // SAFETY: macro expansion assigns each lexical field one static slot
        // and visits it at most once.
        unsafe { $scratch.push_at::<{ $index }, _>($crate::python::py_static_key!($key), $value)? };
        $crate::python::py_dict!(@push $scratch, $index + 1_usize; $($($rest)*)?);
    }};
    (@push $scratch:ident, $index:expr; if let $pattern:pat = $value:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {{
        if let $pattern = $value {
            $crate::python::py_dict!(@push $scratch, $index; $($body)*);
        }
        $crate::python::py_dict!(@push $scratch, $index + $crate::python::py_dict_slots!($($body)*); $($($rest)*)?);
    }};
    (@push $scratch:ident, $index:expr; if $condition:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {{
        if $condition {
            $crate::python::py_dict!(@push $scratch, $index; $($body)*);
        }
        $crate::python::py_dict!(@push $scratch, $index + $crate::python::py_dict_slots!($($body)*); $($($rest)*)?);
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
                            // A str literal is matched but bytes are cached;
                            // the suggested byte-literal form cannot be
                            // spelled generically in a macro.
                            #[expect(clippy::string_lit_as_bytes)]
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
    cached: PyOnceLock<dict_api::CachedKey>,
}

impl StaticPyKey {
    pub(crate) const fn new(text: &'static str) -> Self {
        Self {
            text,
            cached: PyOnceLock::new(),
        }
    }

    fn key(&self, py: Python<'_>) -> PyResult<&dict_api::CachedKey> {
        self.cached
            .get_or_try_init(py, || dict_api::cache_key(py, self.text))
    }

    pub(crate) fn get_item<'py>(
        &self,
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        dict_api::get_item(py, dict, self.key(py)?)
    }

    fn set_item<'py>(
        &self,
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<()> {
        dict_api::set_item(py, dict, self.key(py)?, value)
    }
}

pub(crate) struct PyDictScratch<'py, const N: usize> {
    py: Python<'py>,
    len: usize,
    items: [MaybeUninit<PendingDictItem<'py>>; N],
}

impl<'py, const N: usize> PyDictScratch<'py, N> {
    pub(crate) const fn new(py: Python<'py>) -> Self {
        Self {
            py,
            len: 0,
            items: [const { MaybeUninit::uninit() }; N],
        }
    }

    /// # Safety
    ///
    /// Macro expansion must visit lexical slot `I` at most once and in field
    /// order. This makes the number of prior successful pushes at most `I`.
    pub(crate) unsafe fn push_at<const I: usize, V>(
        &mut self,
        key: &'static StaticPyKey,
        value: V,
    ) -> PyResult<()>
    where
        V: IntoPyObject<'py>,
    {
        let value = value.into_bound_py_any(self.py)?;
        // SAFETY: forwarded from this function's contract.
        unsafe { self.push_bound_at::<I>(key, value) };
        Ok(())
    }

    /// # Safety
    ///
    /// Macro expansion must visit lexical slot `I` at most once and in field
    /// order. This makes the number of prior successful pushes at most `I`.
    pub(crate) unsafe fn push_bound_at<const I: usize>(
        &mut self,
        key: &'static StaticPyKey,
        value: Bound<'py, PyAny>,
    ) {
        const { assert!(I < N) };
        debug_assert!(self.len <= I);
        // SAFETY: `len <= I < N` by the macro contract and const assertion.
        unsafe { self.items.get_unchecked_mut(self.len) }.write(PendingDictItem { key, value });
        // Publish the new live prefix only after initialization completes.
        self.len += 1;
    }

    pub(crate) const fn py(&self) -> Python<'py> {
        self.py
    }

    pub(crate) fn finish(self) -> PyResult<Bound<'py, PyDict>> {
        // SAFETY: `len` is advanced only after initializing a slot and the
        // macro contract prevents it from exceeding `N`.
        let items = unsafe { from_raw_parts(self.items.as_ptr(), self.len) };
        finish_pending_dict(self.py, items)
    }
}

impl<const N: usize> Drop for PyDictScratch<'_, N> {
    fn drop(&mut self) {
        while self.len != 0 {
            self.len -= 1;
            // SAFETY: `0..len` is exactly the initialized prefix. Decrementing
            // first ensures a panicking item destructor cannot be visited a
            // second time if cleanup resumes.
            unsafe { self.items.get_unchecked_mut(self.len).assume_init_drop() };
        }
    }
}

struct PendingDictItem<'py> {
    key: &'static StaticPyKey,
    value: Bound<'py, PyAny>,
}

fn finish_pending_dict<'py>(
    py: Python<'py>,
    items: &[MaybeUninit<PendingDictItem<'py>>],
) -> PyResult<Bound<'py, PyDict>> {
    let dict = py_new_dict(py, items.len())?;
    for item in items {
        // SAFETY: callers expose only the initialized prefix.
        let item = unsafe { item.assume_init_ref() };
        item.key.set_item(py, &dict, &item.value)?;
    }
    Ok(dict)
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
    use pyo3::types::{PyAnyMethods, PyBool, PyBytes, PyBytesMethods, PyDictMethods};
    use pyo3::{IntoPyObjectExt, PyResult, Python, ffi};

    use super::{PyDictScratch, PyString, StaticPyKey};

    fn init_python() {
        Python::initialize();
    }

    #[test]
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
    fn py_dict_static_slots_keep_conditional_fields_sparse_and_ordered() {
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

    #[cfg(not(Py_GIL_DISABLED))]
    #[test]
    fn py_dict_scratch_partial_drop_releases_every_initialized_value() {
        static FIRST: StaticPyKey = StaticPyKey::new("first");
        static THIRD: StaticPyKey = StaticPyKey::new("third");

        init_python();
        Python::attach(|py| {
            let value = PyBytes::new(py, b"owned").into_any();
            // SAFETY: `value` is live under the attached interpreter.
            let baseline = unsafe { ffi::Py_REFCNT(value.as_ptr()) };
            {
                let mut scratch = PyDictScratch::<3>::new(py);
                // SAFETY: lexical slots 0 and 2 are visited once and in order;
                // the skipped middle slot models a false conditional field.
                unsafe {
                    scratch.push_bound_at::<0>(&FIRST, value.clone());
                    scratch.push_bound_at::<2>(&THIRD, value.clone());
                }
                // SAFETY: `value` remains live under the attached interpreter.
                assert_eq!(unsafe { ffi::Py_REFCNT(value.as_ptr()) }, baseline + 2);
            }
            // SAFETY: `value` remains live under the attached interpreter.
            assert_eq!(unsafe { ffi::Py_REFCNT(value.as_ptr()) }, baseline);
        });
    }

    #[cfg(Py_GIL_DISABLED)]
    #[test]
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

        let readers = (0..READERS)
            .map(|_| {
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
            })
            .collect::<Vec<_>>();

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
