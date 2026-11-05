//! See [`PyBytes`] for more info. This mod also contains its helper type [`PyBytesWriter`].

use crate::ffi_ptr_ext::FfiPtrExt;
use crate::instance::{Borrowed, Bound};
#[allow(unused_imports, reason = "used to build docs")]
use crate::platform::prelude::*;
use crate::{Py, PyAny, PyResult, Python, ffi};
#[cfg(RustPython)]
use crate::{
    sync::PyOnceLock,
    types::{PyType, PyTypeMethods},
};
use core::ops::Index;
use core::slice::SliceIndex;
use core::str;

pub use self::writer::PyBytesWriter;

mod writer;

/// Represents a Python `bytes` object.
///
/// This type is immutable.
///
/// Values of this type are accessed via PyO3's smart pointers, e.g. as
/// [`Py<PyBytes>`][crate::Py] or [`Bound<'py, PyBytes>`][Bound].
///
/// For APIs available on `bytes` objects, see the [`PyBytesMethods`] trait which is implemented for
/// [`Bound<'py, PyBytes>`][Bound].
///
/// # Equality
///
/// For convenience, [`Bound<'py, PyBytes>`][Bound] implements [`PartialEq<[u8]>`][PartialEq] to allow comparing the
/// data in the Python bytes to a Rust `[u8]` byte slice.
///
/// This is not always the most appropriate way to compare Python bytes, as Python bytes subclasses
/// may have different equality semantics. In situations where subclasses overriding equality might
/// be relevant, use [`PyAnyMethods::eq`](crate::types::any::PyAnyMethods::eq), at cost of the
/// additional overhead of a Python method call.
///
/// ```rust
/// # use pyo3::prelude::*;
/// use pyo3::types::PyBytes;
///
/// # Python::attach(|py| {
/// let py_bytes = PyBytes::new(py, b"foo".as_slice());
/// // via PartialEq<[u8]>
/// assert_eq!(py_bytes, b"foo".as_slice());
///
/// // via Python equality
/// let other = PyBytes::new(py, b"foo".as_slice());
/// assert!(py_bytes.as_any().eq(other).unwrap());
///
/// // Note that `eq` will convert its argument to Python using `IntoPyObject`.
/// // Byte collections are specialized, so that the following slice will indeed
/// // convert into a `bytes` object and not a `list`:
/// assert!(py_bytes.as_any().eq(b"foo".as_slice()).unwrap());
/// # });
/// ```
#[repr(transparent)]
pub struct PyBytes(PyAny);

#[cfg(not(RustPython))]
pyobject_native_type_core!(PyBytes, pyobject_native_static_type_object!(ffi::PyBytes_Type), "builtins", "bytes", #checkfunction=ffi::PyBytes_Check);

#[cfg(RustPython)]
pyobject_native_type_core!(
    PyBytes,
    |py| {
        static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
        TYPE.import(py, "builtins", "bytes").unwrap().as_type_ptr()
    },
    "builtins",
    "bytes",
    #checkfunction=ffi::PyBytes_Check
);

impl PyBytes {
    /// Concatenates borrowed byte slices directly into the final Python allocation.
    ///
    /// A clone of the iterator determines the total byte length before allocation.
    /// The original iterator then supplies the bytes in order, without an
    /// intermediate buffer, growth, or zero-initialization pass. Source slices
    /// are borrowed only during construction; the result owns its bytes.
    ///
    /// Returns an overflow error if the total length cannot fit Python's size
    /// type, an allocation error if Python cannot allocate the result, or a
    /// `ValueError` if cloning the iterator changes its total byte length.
    /// Iterator panics discard the unfinished allocation.
    pub fn from_slices<'a, 'py, I>(py: Python<'py>, slices: I) -> PyResult<Bound<'py, PyBytes>>
    where
        I: IntoIterator<Item = &'a [u8]>,
        I::IntoIter: Clone,
    {
        let slices = slices.into_iter();
        let length = slices
            .clone()
            .try_fold(0_usize, |total, slice| total.checked_add(slice.len()))
            .ok_or_else(|| crate::exceptions::PyOverflowError::new_err("byte length overflow"))?;
        // SAFETY: the checked length is representable by Python. The new bytes
        // remain private until all their storage has been initialized below.
        let bytes: Bound<'py, PyBytes> = unsafe {
            ffi::PyBytes_FromStringAndSize(core::ptr::null(), length.try_into()?)
                .assume_owned_or_err(py)?
                .cast_into_unchecked()
        };
        // SAFETY: the constructor returned an exact bytes object. Its private
        // payload is writable even though it has not been initialized yet.
        let data = unsafe {
            cfg_select! {
                not(any(Py_LIMITED_API, PyPy, GraalPy, RustPython)) => {
                    ffi::PyBytes_AS_STRING(bytes.as_ptr()).cast_mut()
                }
                _ => ffi::PyBytes_AsString(bytes.as_ptr()),
            }
        };
        // SAFETY: the exact, private allocation contains `length` writable bytes.
        // MaybeUninit permits its current uninitialized contents. No reference
        // to the Python object or its storage escapes through the iterator.
        let mut remaining = unsafe {
            core::slice::from_raw_parts_mut(data.cast::<core::mem::MaybeUninit<u8>>(), length)
        };
        for slice in slices {
            let (target, rest) = remaining.split_at_mut_checked(slice.len()).ok_or_else(|| {
                crate::exceptions::PyValueError::new_err(
                    "byte slices exceeded their measured length",
                )
            })?;
            // SAFETY: the checked split gives exactly slice.len() writable
            // bytes. This private allocation cannot overlap a source slice.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    slice.as_ptr(),
                    target.as_mut_ptr().cast(),
                    slice.len(),
                );
            }
            remaining = rest;
        }
        if !remaining.is_empty() {
            return Err(crate::exceptions::PyValueError::new_err(
                "byte slices did not fill their measured length",
            ));
        }
        Ok(bytes)
    }

    /// Creates a new Python bytestring object.
    /// The bytestring is initialized by copying the data from the `&[u8]`.
    ///
    /// Panics if out of memory.
    pub fn new<'p>(py: Python<'p>, s: &[u8]) -> Bound<'p, PyBytes> {
        let ptr = s.as_ptr().cast();
        let len = s.len() as ffi::Py_ssize_t;
        unsafe {
            ffi::PyBytes_FromStringAndSize(ptr, len)
                .assume_owned(py)
                .cast_into_unchecked()
        }
    }

    /// Creates a new Python `bytes` object with an `init` closure to write its contents.
    /// Before calling `init` the bytes' contents are zero-initialised.
    /// * If Python raises a MemoryError on the allocation, `new_with` will return
    ///   it inside `Err`.
    /// * If `init` returns `Err(e)`, `new_with` will return `Err(e)`.
    /// * If `init` returns `Ok(())`, `new_with` will return `Ok(&PyBytes)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use pyo3::{prelude::*, types::PyBytes};
    ///
    /// # fn main() -> PyResult<()> {
    /// Python::attach(|py| -> PyResult<()> {
    ///     let py_bytes = PyBytes::new_with(py, 10, |bytes: &mut [u8]| {
    ///         bytes.copy_from_slice(b"Hello Rust");
    ///         Ok(())
    ///     })?;
    ///     let bytes: &[u8] = py_bytes.extract()?;
    ///     assert_eq!(bytes, b"Hello Rust");
    ///     Ok(())
    /// })
    /// # }
    /// ```
    #[inline]
    pub fn new_with<F>(py: Python<'_>, len: usize, init: F) -> PyResult<Bound<'_, PyBytes>>
    where
        F: FnOnce(&mut [u8]) -> PyResult<()>,
    {
        unsafe {
            let pyptr = ffi::PyBytes_FromStringAndSize(core::ptr::null(), len as ffi::Py_ssize_t);
            // Check for an allocation error and return it
            let pybytes = pyptr.assume_owned_or_err(py)?.cast_into_unchecked();
            let buffer: *mut u8 = ffi::PyBytes_AsString(pyptr).cast();
            debug_assert!(!buffer.is_null());
            // Zero-initialise the uninitialised bytestring
            core::ptr::write_bytes(buffer, 0u8, len);
            // (Further) Initialise the bytestring in init
            // If init returns an Err, pypybytearray will automatically deallocate the buffer
            init(core::slice::from_raw_parts_mut(buffer, len)).map(|_| pybytes)
        }
    }

    /// Creates a new Python `bytes` object using a writer closure.
    ///
    /// This function reserves at least `reserved_capacity` bytes, then provides a
    /// mutable writer to the closure `write`. The closure can write any number of
    /// bytes, even more than the reserved capacity; the buffer grows as needed.
    ///
    /// If `reserved_capacity` is 0, the buffer will start empty and grow as the writer writes data.
    ///
    /// After the closure returns, the resulting bytes object contains the written data.
    /// A failed native write discards unfinished storage and is returned even if
    /// the closure ignores that write error and returns `Ok(())`.
    ///
    /// # Example
    ///
    /// ```
    /// use pyo3::{prelude::*, types::PyBytes};
    ///
    /// # fn main() -> PyResult<()> {
    /// Python::attach(|py| -> PyResult<()> {
    ///     let py_bytes = PyBytes::new_with_writer(py, 0, |writer| {
    ///         writer.write_bytes(b"hello world")?;
    ///         Ok(())
    ///     })?;
    ///     assert_eq!(py_bytes.as_bytes(), b"hello world");
    ///     Ok(())
    /// })
    /// # }
    /// ```
    #[inline]
    pub fn new_with_writer<'py, F>(
        py: Python<'py>,
        reserved_capacity: usize,
        write: F,
    ) -> PyResult<Bound<'py, PyBytes>>
    where
        F: FnOnce(&mut PyBytesWriter<'py>) -> PyResult<()>,
    {
        let mut writer = PyBytesWriter::with_capacity(py, reserved_capacity)?;
        write(&mut writer)?;
        writer.try_into()
    }

    /// Creates a new Python byte string object from a raw pointer and length.
    ///
    /// Panics if out of memory.
    ///
    /// # Safety
    ///
    /// This function dereferences the raw pointer `ptr` as the
    /// leading pointer of a slice of length `len`. [As with
    /// `core::slice::from_raw_parts`, this is
    /// unsafe](https://doc.rust-lang.org/std/slice/fn.from_raw_parts.html#safety).
    pub unsafe fn from_ptr(py: Python<'_>, ptr: *const u8, len: usize) -> Bound<'_, PyBytes> {
        unsafe {
            ffi::PyBytes_FromStringAndSize(ptr.cast(), len as isize)
                .assume_owned(py)
                .cast_into_unchecked()
        }
    }
}

/// Implementation of functionality for [`PyBytes`].
///
/// These methods are defined for the `Bound<'py, PyBytes>` smart pointer, so to use method call
/// syntax these methods are separated into a trait, because stable Rust does not yet support
/// `arbitrary_self_types`.
#[doc(alias = "PyBytes")]
pub trait PyBytesMethods<'py>: crate::sealed::Sealed {
    /// Gets the Python string as a byte slice.
    fn as_bytes(&self) -> &[u8];
}

impl<'py> PyBytesMethods<'py> for Bound<'py, PyBytes> {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.as_borrowed().as_bytes()
    }
}

impl<'a> Borrowed<'a, '_, PyBytes> {
    /// Gets the Python string as a byte slice.
    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn as_bytes(self) -> &'a [u8] {
        #[cfg(not(Py_LIMITED_API))]
        unsafe {
            let buffer = ffi::PyBytes_AS_STRING(self.as_ptr()).cast::<u8>();
            let length = ffi::Py_SIZE(self.as_ptr()) as usize;
            debug_assert!(!buffer.is_null());
            core::slice::from_raw_parts(buffer, length)
        }

        #[cfg(Py_LIMITED_API)]
        unsafe {
            let buffer = ffi::PyBytes_AsString(self.as_ptr()) as *const u8;
            let length = ffi::PyBytes_Size(self.as_ptr()) as usize;
            debug_assert!(!buffer.is_null());
            core::slice::from_raw_parts(buffer, length)
        }
    }
}

impl Py<PyBytes> {
    /// Gets the Python bytes as a byte slice. Because Python bytes are
    /// immutable, the result may be used for as long as the reference to
    /// `self` is held, including when the GIL is released.
    pub fn as_bytes<'a>(&'a self, py: Python<'_>) -> &'a [u8] {
        self.bind_borrowed(py).as_bytes()
    }
}

/// This is the same way [Vec] is indexed.
impl<I: SliceIndex<[u8]>> Index<I> for Bound<'_, PyBytes> {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        &self.as_bytes()[index]
    }
}

/// Compares whether the Python bytes object is equal to the [u8].
///
/// In some cases Python equality might be more appropriate; see the note on [`PyBytes`].
impl PartialEq<[u8]> for Bound<'_, PyBytes> {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        self.as_borrowed() == *other
    }
}

/// Compares whether the Python bytes object is equal to the [u8].
///
/// In some cases Python equality might be more appropriate; see the note on [`PyBytes`].
impl PartialEq<&'_ [u8]> for Bound<'_, PyBytes> {
    #[inline]
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_borrowed() == **other
    }
}

/// Compares whether the Python bytes object is equal to the [u8].
///
/// In some cases Python equality might be more appropriate; see the note on [`PyBytes`].
impl PartialEq<Bound<'_, PyBytes>> for [u8] {
    #[inline]
    fn eq(&self, other: &Bound<'_, PyBytes>) -> bool {
        *self == other.as_borrowed()
    }
}

/// Compares whether the Python bytes object is equal to the [u8].
///
/// In some cases Python equality might be more appropriate; see the note on [`PyBytes`].
impl PartialEq<&'_ Bound<'_, PyBytes>> for [u8] {
    #[inline]
    fn eq(&self, other: &&Bound<'_, PyBytes>) -> bool {
        *self == other.as_borrowed()
    }
}

/// Compares whether the Python bytes object is equal to the [u8].
///
/// In some cases Python equality might be more appropriate; see the note on [`PyBytes`].
impl PartialEq<Bound<'_, PyBytes>> for &'_ [u8] {
    #[inline]
    fn eq(&self, other: &Bound<'_, PyBytes>) -> bool {
        **self == other.as_borrowed()
    }
}

/// Compares whether the Python bytes object is equal to the [u8].
///
/// In some cases Python equality might be more appropriate; see the note on [`PyBytes`].
impl PartialEq<[u8]> for &'_ Bound<'_, PyBytes> {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        self.as_borrowed() == other
    }
}

/// Compares whether the Python bytes object is equal to the [u8].
///
/// In some cases Python equality might be more appropriate; see the note on [`PyBytes`].
impl PartialEq<[u8]> for Borrowed<'_, '_, PyBytes> {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes() == other
    }
}

/// Compares whether the Python bytes object is equal to the [u8].
///
/// In some cases Python equality might be more appropriate; see the note on [`PyBytes`].
impl PartialEq<&[u8]> for Borrowed<'_, '_, PyBytes> {
    #[inline]
    fn eq(&self, other: &&[u8]) -> bool {
        *self == **other
    }
}

/// Compares whether the Python bytes object is equal to the [u8].
///
/// In some cases Python equality might be more appropriate; see the note on [`PyBytes`].
impl PartialEq<Borrowed<'_, '_, PyBytes>> for [u8] {
    #[inline]
    fn eq(&self, other: &Borrowed<'_, '_, PyBytes>) -> bool {
        other == self
    }
}

/// Compares whether the Python bytes object is equal to the [u8].
///
/// In some cases Python equality might be more appropriate; see the note on [`PyBytes`].
impl PartialEq<Borrowed<'_, '_, PyBytes>> for &'_ [u8] {
    #[inline]
    fn eq(&self, other: &Borrowed<'_, '_, PyBytes>) -> bool {
        other == self
    }
}

impl<'a> AsRef<[u8]> for Borrowed<'a, '_, PyBytes> {
    #[inline]
    fn as_ref(&self) -> &'a [u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for Bound<'_, PyBytes> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PyAnyMethods as _;

    #[test]
    fn test_bytes_index() {
        Python::attach(|py| {
            let bytes = PyBytes::new(py, b"Hello World");
            assert_eq!(bytes[1], b'e');
        });
    }

    #[test]
    fn test_bound_bytes_index() {
        Python::attach(|py| {
            let bytes = PyBytes::new(py, b"Hello World");
            assert_eq!(bytes[1], b'e');

            let bytes = &bytes;
            assert_eq!(bytes[1], b'e');
        });
    }

    #[test]
    fn test_bytes_new_with() -> super::PyResult<()> {
        Python::attach(|py| -> super::PyResult<()> {
            let py_bytes = PyBytes::new_with(py, 10, |b: &mut [u8]| {
                b.copy_from_slice(b"Hello Rust");
                Ok(())
            })?;
            let bytes: &[u8] = py_bytes.extract()?;
            assert_eq!(bytes, b"Hello Rust");
            Ok(())
        })
    }

    #[test]
    fn test_bytes_new_with_zero_initialised() -> super::PyResult<()> {
        Python::attach(|py| -> super::PyResult<()> {
            let py_bytes = PyBytes::new_with(py, 10, |_b: &mut [u8]| Ok(()))?;
            let bytes: &[u8] = py_bytes.extract()?;
            assert_eq!(bytes, &[0; 10]);
            Ok(())
        })
    }

    #[test]
    fn test_bytes_new_with_error() {
        use crate::exceptions::PyValueError;
        Python::attach(|py| {
            let py_bytes_result = PyBytes::new_with(py, 10, |_b: &mut [u8]| {
                Err(PyValueError::new_err("Hello Crustaceans!"))
            });
            assert!(py_bytes_result.is_err());
            assert!(
                py_bytes_result
                    .err()
                    .unwrap()
                    .is_instance_of::<PyValueError>(py)
            );
        });
    }

    #[test]
    fn test_bytes_from_slices_preserves_segment_boundaries() {
        Python::attach(|py| {
            let data = [0x81; 257];
            for slices in [
                vec![],
                vec![b"".as_slice()],
                vec![b"x".as_slice()],
                vec![b"ab".as_slice(), b"".as_slice(), b"\0cd".as_slice()],
                vec![data.as_slice(), data.as_slice(), b"tail".as_slice()],
            ] {
                let expected = slices.concat();
                let result = PyBytes::from_slices(py, slices.iter().copied()).unwrap();
                assert_eq!(result.as_bytes(), expected);
            }
        });
    }

    #[test]
    fn test_bytes_from_slices_rejects_inconsistent_clone_lengths() {
        struct Slices {
            current: Option<&'static [u8]>,
            measured: &'static [u8],
        }
        impl Clone for Slices {
            fn clone(&self) -> Self {
                Self {
                    current: Some(self.measured),
                    measured: self.measured,
                }
            }
        }
        impl Iterator for Slices {
            type Item = &'static [u8];

            fn next(&mut self) -> Option<Self::Item> {
                self.current.take()
            }
        }
        Python::attach(|py| {
            for (current, measured) in [
                (b"x".as_slice(), b"xyz".as_slice()),
                (b"xyz".as_slice(), b"x".as_slice()),
            ] {
                let slices = Slices {
                    current: Some(current),
                    measured,
                };
                let error = PyBytes::from_slices(py, slices).unwrap_err();
                assert!(error.is_instance_of::<crate::exceptions::PyValueError>(py));
            }
        });
    }

    #[test]
    fn test_comparisons() {
        Python::attach(|py| {
            let b = b"hello, world".as_slice();
            let py_bytes = PyBytes::new(py, b);

            assert_eq!(py_bytes, b"hello, world".as_slice());

            assert_eq!(py_bytes, b);
            assert_eq!(&py_bytes, b);
            assert_eq!(b, py_bytes);
            assert_eq!(b, &py_bytes);

            assert_eq!(py_bytes, *b);
            assert_eq!(&py_bytes, *b);
            assert_eq!(*b, py_bytes);
            assert_eq!(*b, &py_bytes);

            let py_string = py_bytes.as_borrowed();

            assert_eq!(py_string, b);
            assert_eq!(&py_string, b);
            assert_eq!(b, py_string);
            assert_eq!(b, &py_string);

            assert_eq!(py_string, *b);
            assert_eq!(*b, py_string);
        })
    }

    #[test]
    #[cfg(not(Py_LIMITED_API))]
    fn test_as_string() {
        Python::attach(|py| {
            let b = b"hello, world".as_slice();
            let py_bytes = PyBytes::new(py, b);
            unsafe {
                assert_eq!(
                    ffi::PyBytes_AsString(py_bytes.as_ptr()) as *const core::ffi::c_char,
                    ffi::PyBytes_AS_STRING(py_bytes.as_ptr()) as *const core::ffi::c_char
                );
            }
        })
    }

    #[test]
    fn test_as_ref_slice() {
        Python::attach(|py| {
            let b = b"hello, world";
            let py_bytes = PyBytes::new(py, b);
            let ref_bound: &[u8] = py_bytes.as_ref();
            assert_eq!(ref_bound, b);
            let py_bytes_borrowed = py_bytes.as_borrowed();
            let ref_borrowed: &[u8] = py_bytes_borrowed.as_ref();
            assert_eq!(ref_borrowed, b);
        })
    }

    #[test]
    fn test_py_as_bytes() {
        let pyobj: Py<PyBytes> = Python::attach(|py| PyBytes::new(py, b"abc").unbind());

        let data = Python::attach(|py| pyobj.as_bytes(py));

        assert_eq!(data, b"abc");

        Python::attach(move |_py| drop(pyobj));
    }

    #[test]
    fn test_with_writer() {
        Python::attach(|py| {
            let bytes = PyBytes::new_with_writer(py, 0, |writer| {
                writer.write_bytes(b"hallo")?;
                Ok(())
            })
            .unwrap();

            assert_eq!(bytes.as_bytes(), b"hallo");
        })
    }
}
