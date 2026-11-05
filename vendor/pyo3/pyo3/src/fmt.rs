//! Python-owned Unicode construction and Rust formatting integration.
//!
//! The writer owns unfinished CPython storage until finish or drop. Native
//! append methods propagate Python exceptions; fmt::Write stores its exception
//! until the caller retrieves it or consumes the writer.

#![warn(clippy::undocumented_unsafe_blocks)]

#[allow(unused_imports, reason = "used to build docs")]
use crate::platform::prelude::*;
#[cfg(all(feature = "experimental-inspect", Py_3_14, not(Py_LIMITED_API)))]
use crate::{PyTypeInfo, inspect::PyStaticExpr};
#[cfg(any(doc, all(Py_3_14, not(Py_LIMITED_API))))]
use crate::{Python, types::PyString};
#[cfg(all(feature = "nightly", Py_3_14, not(Py_LIMITED_API)))]
use core::ascii::Char;
#[cfg(all(Py_3_14, not(Py_LIMITED_API)))]
use {
    crate::IntoPyObject,
    crate::ffi::{
        PyUnicodeWriter_Create, PyUnicodeWriter_Discard, PyUnicodeWriter_Finish,
        PyUnicodeWriter_WriteChar, PyUnicodeWriter_WriteUTF8,
    },
    crate::ffi_ptr_ext::FfiPtrExt,
    crate::py_result_ext::PyResultExt,
    crate::{Bound, PyErr, PyResult, ffi},
    core::fmt,
    core::mem::ManuallyDrop,
    core::ptr::NonNull,
};

/// This macro is analogous to Rust's [`format!`] macro, but returns a [`PyString`] instead of a [`String`].
///
/// # Arguments
///
/// The arguments are exactly like [`format!`], but with `py` (a [`Python`] token) as the first argument:
///
/// # Interning Advantage
///
/// If the format string is a static string and all arguments are constant at compile time,
/// this macro will intern the string in Python, offering better performance and memory usage
/// compared to [`PyString::from_fmt`].
///
/// ```rust
/// # use pyo3::{py_format, Python, types::PyString, Bound};
/// Python::attach(|py| {
///     let py_string: Bound<'_, PyString> = py_format!(py, "{} {}", "hello", "world").unwrap();
///     assert_eq!(py_string.to_string(), "hello world");
/// });
/// ```
#[macro_export]
macro_rules! py_format {
    ($py: expr, $($arg:tt)*) => {{
        let __py = $py;
        // A match extends the formatting temporaries through both paths without
        // evaluating dynamic arguments again after the static-string probe.
        match format_args!($($arg)*) {
            __args => if let Some(static_string) = __args.as_str() {
                static INTERNED: $crate::sync::PyOnceLock<$crate::Py<$crate::types::PyString>> = $crate::sync::PyOnceLock::new();
                Ok($crate::Bound::clone(
                    INTERNED
                    .get_or_init(__py, || $crate::types::PyString::intern(__py, static_string).unbind())
                    .bind(__py)
                ))
            } else {
                $crate::types::PyString::from_fmt(__py, __args)
            }
        }
    }}
}

#[cfg(all(Py_3_14, not(Py_LIMITED_API)))]
/// Owns an unfinished Python Unicode string until completion or drop.
///
/// Append operations copy directly into CPython-managed storage, widening its
/// character representation when needed. Capacity is advisory and measured in
/// characters. Dropping the writer releases unfinished storage; finishing consumes
/// it even if CPython reports an error. Rust formatting errors retain their Python
/// cause until [`Self::take_error`] or [`Self::into_py_string`] retrieves it.
pub struct PyUnicodeWriter<'py> {
    python: Python<'py>,
    writer: NonNull<ffi::PyUnicodeWriter>,
    last_error: Option<PyErr>,
}

#[cfg(all(Py_3_14, not(Py_LIMITED_API)))]
impl<'py> PyUnicodeWriter<'py> {
    /// Appends Rust formatted arguments, preserving a Python allocation error.
    ///
    /// This is the native `write!` entry point, so callers can propagate its
    /// `PyResult` directly. As with Rust string formatting, a Display
    /// implementation that returns an error without a writer error panics.
    pub fn write_fmt(&mut self, arguments: fmt::Arguments<'_>) -> PyResult<()> {
        fmt::Write::write_fmt(self, arguments).map_err(|_| {
            self.take_error()
                .expect("a formatter returned an error without a writer error")
        })
    }

    /// Appends Rust UTF-8 text without an intermediate Python string.
    pub fn write_str(&mut self, text: &str) -> PyResult<()> {
        // SAFETY: this attached owner retains the live writer; str supplies valid
        // UTF-8 and a live pointer for the checked byte count. CPython copies it.
        crate::err::error_on_negative(self.python, unsafe {
            PyUnicodeWriter_WriteUTF8(self.as_ptr(), text.as_ptr().cast(), text.len().try_into()?)
        })
    }

    /// Appends one Unicode scalar value.
    pub fn write_char(&mut self, character: char) -> PyResult<()> {
        // SAFETY: the writer is live and attached, and Rust char is a valid scalar.
        crate::err::error_on_negative(self.python, unsafe {
            PyUnicodeWriter_WriteChar(self.as_ptr(), character.into())
        })
    }

    /// Appends a Python string in its native Unicode representation.
    ///
    /// No UTF-8 encoding or cache is requested, and unpaired surrogates are
    /// preserved. The input is borrowed only for this append.
    pub fn write_py_string(&mut self, text: &Bound<'_, PyString>) -> PyResult<()> {
        // SAFETY: both objects remain live for this attached call. CPython copies
        // the text or retains its own reference when reusing read-only storage.
        crate::err::error_on_negative(self.python, unsafe {
            ffi::PyUnicodeWriter_WriteStr(self.as_ptr(), text.as_ptr())
        })
    }

    /// Appends typed ASCII without UTF-8 decoding.
    ///
    /// The character slice is borrowed only for this call. Allocation errors
    /// propagate without consuming the writer.
    #[cfg(feature = "nightly")]
    pub fn write_ascii(&mut self, characters: &[Char]) -> PyResult<()> {
        let bytes = characters.as_bytes();
        // SAFETY: Char guarantees ASCII; the pointer is live for the call.
        // CPython copies it into writer-owned storage and reports allocation errors.
        crate::err::error_on_negative(self.python, unsafe {
            ffi::PyUnicodeWriter_WriteASCII(
                self.as_ptr(),
                bytes.as_ptr().cast(),
                bytes.len().try_into()?,
            )
        })
    }

    /// Creates a new `PyUnicodeWriter`.
    pub fn new(py: Python<'py>) -> PyResult<Self> {
        Self::with_capacity(py, 0)
    }

    /// Creates a new `PyUnicodeWriter` with the specified initial capacity.
    #[inline]
    pub fn with_capacity(py: Python<'py>, capacity: usize) -> PyResult<Self> {
        // SAFETY: attached thread and representable capacity; CPython returns a
        // new writer owned here, or null with an exception.
        match NonNull::new(unsafe { PyUnicodeWriter_Create(capacity.try_into()?) }) {
            Some(ptr) => Ok(PyUnicodeWriter {
                python: py,
                writer: ptr,
                last_error: None,
            }),
            None => Err(PyErr::fetch(py)),
        }
    }

    /// Consumes the `PyUnicodeWriter` and returns a `Bound<PyString>` containing the constructed string.
    #[inline]
    pub fn into_py_string(mut self) -> PyResult<Bound<'py, PyString>> {
        let py = self.python;
        if let Some(error) = self.take_error() {
            Err(error)
        } else {
            // SAFETY: finish consumes this live writer even on failure and returns
            // a new exact str or an exception. Suppress Drop's second discard.
            unsafe {
                PyUnicodeWriter_Finish(ManuallyDrop::new(self).as_ptr())
                    .assume_owned_or_err(py)
                    .cast_into_unchecked()
            }
        }
    }

    /// When fmt::Write returned an error, this function can be used to retrieve the last error that occurred.
    #[inline]
    pub fn take_error(&mut self) -> Option<PyErr> {
        self.last_error.take()
    }

    #[inline]
    fn as_ptr(&self) -> *mut ffi::PyUnicodeWriter {
        self.writer.as_ptr()
    }
}

#[cfg(all(Py_3_14, not(Py_LIMITED_API)))]
impl fmt::Write for PyUnicodeWriter<'_> {
    #[inline]
    fn write_str(&mut self, s: &str) -> fmt::Result {
        match PyUnicodeWriter::write_str(self, s) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.last_error = Some(error);
                Err(fmt::Error)
            }
        }
    }

    #[inline]
    fn write_char(&mut self, c: char) -> fmt::Result {
        match PyUnicodeWriter::write_char(self, c) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.last_error = Some(error);
                Err(fmt::Error)
            }
        }
    }
}

#[cfg(all(Py_3_14, not(Py_LIMITED_API)))]
impl Drop for PyUnicodeWriter<'_> {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: only unfinished writers reach Drop; successful or failed finish
        // transfers ownership out through ManuallyDrop.
        unsafe {
            PyUnicodeWriter_Discard(self.as_ptr());
        }
    }
}

#[cfg(all(Py_3_14, not(Py_LIMITED_API)))]
impl<'py> IntoPyObject<'py> for PyUnicodeWriter<'py> {
    type Target = PyString;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    #[cfg(feature = "experimental-inspect")]
    const OUTPUT_TYPE: PyStaticExpr = PyString::TYPE_HINT;

    #[inline]
    fn into_pyobject(self, _py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        self.into_py_string()
    }
}

#[cfg(all(Py_3_14, not(Py_LIMITED_API)))]
impl<'py> TryInto<Bound<'py, PyString>> for PyUnicodeWriter<'py> {
    type Error = PyErr;

    #[inline]
    fn try_into(self) -> PyResult<Bound<'py, PyString>> {
        self.into_py_string()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(all(Py_3_14, not(Py_LIMITED_API)))]
    use super::*;
    use crate::types::PyStringMethods;
    use crate::{IntoPyObject, Python};

    #[test]
    fn formatting_evaluates_dynamic_expressions_once() {
        Python::attach(|py| {
            let mut tokens = 0;
            let mut values = 0;
            let output = crate::py_format!(
                {
                    tokens += 1;
                    py
                },
                "value:{}",
                {
                    values += 1;
                    values
                }
            )
            .unwrap();
            assert_eq!(output.to_str().unwrap(), "value:1");
            assert_eq!((tokens, values), (1, 1));
        });
    }

    #[test]
    #[allow(clippy::write_literal)]
    #[cfg(all(Py_3_14, not(Py_LIMITED_API)))]
    fn unicode_writer_test() {
        Python::attach(|py| {
            let mut writer = PyUnicodeWriter::new(py).unwrap();
            write!(writer, "Hello {}!", "world").unwrap();
            writer.write_char('😎').unwrap();
            let result = writer.into_py_string().unwrap();
            assert_eq!(result.to_string(), "Hello world!😎");
        });
    }

    #[test]
    #[allow(clippy::write_literal)]
    #[cfg(all(Py_3_14, not(Py_LIMITED_API)))]
    fn unicode_writer_with_capacity() {
        Python::attach(|py| {
            let mut writer = PyUnicodeWriter::with_capacity(py, 10).unwrap();
            write!(writer, "Hello {}!", "world").unwrap();
            writer.write_char('😎').unwrap();
            let result = writer.into_py_string().unwrap();
            assert_eq!(result.to_string(), "Hello world!😎");
        });
    }

    #[test]
    fn test_pystring_from_fmt() {
        Python::attach(|py| {
            py_format!(py, "Hello {}!", "world").unwrap();
        });
    }

    #[test]
    fn test_complex_format() {
        Python::attach(|py| {
            let complex_value = (42, "foo", [0; 0]).into_pyobject(py).unwrap();
            let py_string = py_format!(py, "This is some complex value: {complex_value}").unwrap();
            let actual = py_string.to_cow().unwrap();
            let expected = "This is some complex value: (42, 'foo', [])";
            assert_eq!(actual, expected);
        });
    }
}
