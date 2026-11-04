//! Reuse of fixed Python string and byte literals.
use std::any::Any;

use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use pyo3::types::PyString;
#[doc(hidden)]
pub fn value<'py, V>(py: Python<'py>, value: V) -> PyResult<Bound<'py, PyAny>>
where
    V: Copy + IntoPyObject<'py> + 'static,
{
    let value_any = &value as &dyn Any;
    value_any.downcast_ref::<&'static str>().map_or_else(
        || value.into_bound_py_any(py),
        |text| Ok(PyString::intern(py, text).clone().into_any()),
    )
}
#[macro_export]
macro_rules! cached_string {
    (
        $py:expr,
        $value:expr,
        [ $($value_text:literal),* $(,)? ]
    ) => {{
        let value = $value;
        match value {
            $($value_text => $crate::pyo3::intern!($py, $value_text).clone(),)*
            _ => $crate::pyo3::types::PyString::new($py, value),
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
            $($pattern => $crate::pyo3::intern!($py, $value_text).clone(),)*
            _ => $fallback,
        }
    }};
}
#[macro_export]
macro_rules! cached_bytes {
    (
        $py:expr,
        $value:expr,
        [ $($value_text:literal),* $(,)? ]
    ) => {{
        let value = $value;
        match value {
            $(
                $value_text => {
                    static CACHED: $crate::pyo3::sync::PyOnceLock<
                        $crate::pyo3::Py<$crate::pyo3::types::PyBytes>,
                    > = $crate::pyo3::sync::PyOnceLock::new();
                    CACHED
                        .get_or_init($py, || {
                            #[expect(
                                clippy::string_lit_as_bytes,
                                reason = "a str literal is matched but bytes are cached; the \
                                          suggested byte-literal form cannot be spelled \
                                          generically in a macro"
                            )]
                            $crate::pyo3::types::PyBytes::new($py, $value_text.as_bytes()).unbind()
                        })
                        .bind($py)
                        .clone()
                },
            )*
            _ => $crate::pyo3::types::PyBytes::new($py, value.as_bytes()),
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
                    static CACHED: $crate::pyo3::sync::PyOnceLock<
                        $crate::pyo3::Py<$crate::pyo3::types::PyBytes>,
                    > = $crate::pyo3::sync::PyOnceLock::new();
                    CACHED
                        .get_or_init($py, || $crate::pyo3::types::PyBytes::new($py, $value_text).unbind())
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
                    static CACHED: $crate::pyo3::sync::PyOnceLock<
                        $crate::pyo3::Py<$crate::pyo3::types::PyBytes>,
                    > = $crate::pyo3::sync::PyOnceLock::new();
                    CACHED
                        .get_or_init($py, || $crate::pyo3::types::PyBytes::new($py, $value_text).unbind())
                        .bind($py)
                        .clone()
                },
            )*
            _ => $fallback,
        }
    }};
}
