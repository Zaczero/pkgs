pub(crate) use pyo3::sync::PyOnceLock;
pub(crate) use pyo3::types::{PyAny, PyBytes, PyDict, PyString};
use pyo3::{prelude::*, IntoPyObject, IntoPyObjectExt};
pub(crate) use pyo3::{Py, PyResult};
use std::{any::Any, mem::MaybeUninit};

#[cfg(not(any(PyPy, GraalPy, Py_LIMITED_API)))]
mod dict_api {
    use super::*;
    use pyo3::{ffi, PyErr};
    use std::ffi::c_int;

    pub(super) type CachedKey = (Py<PyString>, ffi::Py_hash_t);

    unsafe extern "C" {
        fn _PyDict_NewPresized(minused: ffi::Py_ssize_t) -> *mut ffi::PyObject;
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
        // SAFETY: the GIL is held by `py`; `_PyDict_NewPresized` returns a new
        // owned dict pointer or sets a Python exception; and the result is
        // converted immediately into `Bound<PyDict>` before exposure.
        unsafe {
            Bound::from_owned_ptr_or_err(py, _PyDict_NewPresized(capacity))
                .map(|dict| dict.cast_into_unchecked::<PyDict>())
        }
    }

    pub(super) fn get_item<'py>(
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
        (key, hash): &CachedKey,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        let key = key.bind(py);
        let value = unsafe { _PyDict_GetItem_KnownHash(dict.as_ptr(), key.as_ptr(), *hash) };
        if value.is_null() {
            if unsafe { ffi::PyErr_Occurred().is_null() } {
                Ok(None)
            } else {
                Err(PyErr::fetch(py))
            }
        } else {
            Ok(Some(unsafe { Bound::from_borrowed_ptr(py, value) }))
        }
    }

    pub(super) fn set_item<'py>(
        py: Python<'py>,
        dict: &Bound<'py, PyDict>,
        (key, hash): &CachedKey,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<()> {
        let key = key.bind(py);
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
    use super::*;
    use pyo3::types::PyDictMethods;

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

pub(crate) fn py_new_dict(py: Python<'_>, capacity: usize) -> PyResult<Bound<'_, PyDict>> {
    dict_api::new_dict(py, capacity)
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
    pub(crate) fn new(py: Python<'py>) -> Self {
        Self {
            py,
            len: 0,
            items: unsafe {
                MaybeUninit::<[MaybeUninit<PendingDictItem<'py>>; N]>::uninit().assume_init()
            },
        }
    }

    pub(crate) fn push<V>(&mut self, key: &'static StaticPyKey, value: V) -> PyResult<()>
    where
        V: IntoPyObject<'py>,
    {
        let value = value.into_bound_py_any(self.py)?;
        self.push_bound(key, value);
        Ok(())
    }

    pub(crate) fn push_bound(&mut self, key: &'static StaticPyKey, value: Bound<'py, PyAny>) {
        unsafe {
            self.items
                .get_unchecked_mut(self.len)
                .write(PendingDictItem { key, value });
        }
        self.len += 1;
    }

    pub(crate) fn py(&self) -> Python<'py> {
        self.py
    }

    pub(crate) fn finish(self) -> PyResult<Bound<'py, PyDict>> {
        let dict = py_new_dict(self.py, self.len)?;
        for index in 0..self.len {
            let item = unsafe { self.items.get_unchecked(index).assume_init_ref() };
            item.key.set_item(self.py, &dict, &item.value)?;
        }
        Ok(dict)
    }
}

struct PendingDictItem<'py> {
    key: &'static StaticPyKey,
    value: Bound<'py, PyAny>,
}

impl<'py, const N: usize> Drop for PyDictScratch<'py, N> {
    fn drop(&mut self) {
        for index in 0..self.len {
            unsafe {
                self.items.get_unchecked_mut(index).assume_init_drop();
            }
        }
    }
}

macro_rules! py_static_key {
    ($key:literal) => {{
        static KEY: $crate::python::StaticPyKey = $crate::python::StaticPyKey::new($key);
        &KEY
    }};
}

pub(crate) use py_static_key;

macro_rules! py_dict_slots {
    () => {
        0usize
    };
    ($key:literal => $value:expr $(, $($rest:tt)*)?) => {
        1usize + $crate::python::py_dict_slots!($($($rest)*)?)
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

pub(crate) use py_dict_slots;

pub(crate) fn py_dict_literal_value<'py, V>(
    py: Python<'py>,
    value: V,
) -> PyResult<Bound<'py, PyAny>>
where
    V: Copy + IntoPyObject<'py> + 'static,
{
    let value_any = &value as &dyn Any;
    if let Some(text) = value_any.downcast_ref::<&'static str>() {
        Ok(PyString::intern(py, text).clone().into_any())
    } else {
        value.into_bound_py_any(py)
    }
}

macro_rules! py_dict {
    ($py:expr, { $($items:tt)* }) => {{
        let mut __scratch = $crate::python::PyDictScratch::<
            { $crate::python::py_dict_slots!($($items)*) }
        >::new($py);
        $crate::python::py_dict!(@push __scratch, $($items)*);
        __scratch.finish()?
    }};
    (@push $scratch:ident,) => {};
    (@push $scratch:ident, $key:literal => true $(, $($rest:tt)*)?) => {{
        $scratch.push($crate::python::py_static_key!($key), true)?;
        $crate::python::py_dict!(@push $scratch, $($($rest)*)?);
    }};
    (@push $scratch:ident, $key:literal => false $(, $($rest:tt)*)?) => {{
        $scratch.push($crate::python::py_static_key!($key), false)?;
        $crate::python::py_dict!(@push $scratch, $($($rest)*)?);
    }};
    (@push $scratch:ident, $key:literal => $value:literal $(, $($rest:tt)*)?) => {{
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
        $scratch.push_bound($crate::python::py_static_key!($key), value);
        $crate::python::py_dict!(@push $scratch, $($($rest)*)?);
    }};
    (@push $scratch:ident, $key:literal => $value:expr $(, $($rest:tt)*)?) => {{
        $scratch.push($crate::python::py_static_key!($key), $value)?;
        $crate::python::py_dict!(@push $scratch, $($($rest)*)?);
    }};
    (@push $scratch:ident, if let $pattern:pat = $value:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {{
        if let $pattern = $value {
            $crate::python::py_dict!(@push $scratch, $($body)*);
        }
        $crate::python::py_dict!(@push $scratch, $($($rest)*)?);
    }};
    (@push $scratch:ident, if $condition:expr => { $($body:tt)* } $(, $($rest:tt)*)?) => {{
        if $condition {
            $crate::python::py_dict!(@push $scratch, $($body)*);
        }
        $crate::python::py_dict!(@push $scratch, $($($rest)*)?);
    }};
}

pub(crate) use py_dict;

macro_rules! py_cached_dict {
    ($py:expr, { $($key:literal => $value:expr),* $(,)? }) => {{
        static CACHED: $crate::python::PyOnceLock<
            $crate::python::Py<$crate::python::PyDict>,
        > = $crate::python::PyOnceLock::new();
        {
            let dict = CACHED.get_or_try_init(
                $py,
                || -> $crate::python::PyResult<$crate::python::Py<$crate::python::PyDict>> {
                    Ok($crate::python::py_dict!($py, {
                        $($key => $value),*
                    })
                    .unbind())
                },
            )?;
            ::std::result::Result::<
                ::pyo3::Bound<'_, $crate::python::PyDict>,
                ::pyo3::PyErr,
            >::Ok(dict.bind($py).clone())
        }
    }};
}

pub(crate) use py_cached_dict;

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

pub(crate) use py_match_cached_string;

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

pub(crate) use py_match_cached_bytes;

#[cfg(test)]
mod tests {
    use super::PyString;
    use pyo3::{
        ffi,
        types::{PyBool, PyBytes, PyDictMethods},
        IntoPyObjectExt, PyResult, Python,
    };

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

            assert!(unsafe { ffi::Py_Is(value.as_ptr(), interned.as_ptr()) } != 0);
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

            assert!(unsafe { ffi::Py_Is(value.as_ptr(), expected.as_ptr()) } != 0);
            Ok(())
        })
        .unwrap();
    }
}
