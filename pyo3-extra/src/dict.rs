//! Dictionary composition with reusable literal keys.

/// Construct a dictionary with cached literal keys and optional fields.
///
/// Dynamic expressions run once. False conditional branches do not evaluate
/// their values. Errors propagate through the caller's `PyResult`. Unconditional
/// fields use PyO3's borrowed bulk constructor; conditional fields are inserted
/// incrementally so branch-local values need no longer-lived temporary storage.
#[macro_export]
macro_rules! dict {
    ($py:expr, { $($items:tt)* }) => {{
        let __py = $py;
        $crate::dict!(@bulk __py; [$($items)*]; []; []; []; $($items)*)
    }};
    // Accumulate syntax, not values: encountering a conditional selects the
    // incremental path without evaluating any expression twice.
    // Fresh hygienic bindings avoid numeric tuple indices or caller feature gates.
    (@bulk $py:ident; [$($original:tt)*]; [$($keys:literal,)*]; [$($values:expr,)*]; [$($bindings:ident,)*];) => {{
        // A match retains expression temporaries and borrowed conversion outputs
        // until the dictionary has acquired references.
        match ($($values,)*) {
            ($($bindings,)*) => $crate::pyo3::types::PyDict::from_borrowed_items(
                $py,
                &[$(
                    $crate::pyo3::BoundObject::into_any(
                        $crate::pyo3::intern!($py, $keys).as_borrowed()
                    ),
                )*],
                &[$(
                    $crate::pyo3::BoundObject::into_any(
                        $crate::pyo3::BoundObject::as_borrowed(&$bindings)
                    ),
                )*],
            )?,
        }
    }};
    (@bulk $py:ident; [$($original:tt)*]; [$($keys:literal,)*]; [$($values:expr,)*]; [$($bindings:ident,)*];
        $key:literal => true $(, $($rest:tt)*)?) => {
        $crate::dict!(@bulk $py; [$($original)*]; [$($keys,)* $key,];
            [$($values,)* $crate::pyo3::types::PyBool::new($py, true),];
            [$($bindings,)* __value,]; $($($rest)*)?)
    };
    (@bulk $py:ident; [$($original:tt)*]; [$($keys:literal,)*]; [$($values:expr,)*]; [$($bindings:ident,)*];
        $key:literal => false $(, $($rest:tt)*)?) => {
        $crate::dict!(@bulk $py; [$($original)*]; [$($keys,)* $key,];
            [$($values,)* $crate::pyo3::types::PyBool::new($py, false),];
            [$($bindings,)* __value,]; $($($rest)*)?)
    };
    (@bulk $py:ident; [$($original:tt)*]; [$($keys:literal,)*]; [$($values:expr,)*]; [$($bindings:ident,)*];
        $key:literal => $value:literal $(, $($rest:tt)*)?) => {
        $crate::dict!(@bulk $py; [$($original)*]; [$($keys,)* $key,];
            [$($values,)* $crate::dict!(@literal $py; $value),];
            [$($bindings,)* __value,]; $($($rest)*)?)
    };
    (@bulk $py:ident; [$($original:tt)*]; [$($keys:literal,)*]; [$($values:expr,)*]; [$($bindings:ident,)*];
        $key:literal => $value:expr $(, $($rest:tt)*)?) => {
        $crate::dict!(@bulk $py; [$($original)*]; [$($keys,)* $key,];
            [$($values,)* $crate::pyo3::IntoPyObjectExt::into_pyobject_any($value, $py)?,];
            [$($bindings,)* __value,];
            $($($rest)*)?)
    };
    (@bulk $py:ident; [$($original:tt)*]; [$($keys:literal,)*]; [$($values:expr,)*]; [$($bindings:ident,)*];
        if $($rest:tt)*) => {{
        let __dict = $crate::pyo3::types::PyDict::new($py);
        $crate::dict!(@push __dict, $py; $($original)*);
        __dict
    }};
    (@literal $py:ident; $value:literal) => {{
        static CACHED: $crate::pyo3::sync::PyOnceLock<
            $crate::pyo3::Py<$crate::pyo3::types::PyAny>,
        > = $crate::pyo3::sync::PyOnceLock::new();
        CACHED
            .get_or_try_init($py, || {
                $crate::literal::value($py, $value).map($crate::pyo3::Bound::unbind)
            })?
            .bind($py)
            .as_borrowed()
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
        let value = $crate::dict!(@literal $py; $value);
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
