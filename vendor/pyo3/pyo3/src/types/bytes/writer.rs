// TODO https://github.com/PyO3/pyo3/issues/5487
#![allow(clippy::undocumented_unsafe_blocks)]

//! Helper for making a [`PyBytes`], see [`PyBytesWriter`] for details.

#[cfg(feature = "experimental-inspect")]
use crate::PyTypeInfo;
#[cfg(feature = "experimental-inspect")]
use crate::inspect::PyStaticExpr;
#[allow(unused_imports, reason = "conditionally used")]
use crate::platform::prelude::*;
use crate::{Bound, IntoPyObject, PyErr, PyResult, Python, types::PyBytes};
#[cfg(not(Py_LIMITED_API))]
use crate::{
    err,
    ffi::{
        self,
        compat::{
            PyBytesWriter_Create, PyBytesWriter_Discard, PyBytesWriter_Finish,
            PyBytesWriter_GetData, PyBytesWriter_GetSize, PyBytesWriter_Resize,
        },
    },
    ffi_ptr_ext::FfiPtrExt,
    py_result_ext::PyResultExt,
};
#[cfg(not(Py_LIMITED_API))]
use core::{mem::ManuallyDrop, ptr::NonNull};

/// Type used to create a [`PyBytes`], used in [`PyBytes::new_with_writer`].
///
/// A native write failure discards unfinished storage. Further writes and
/// completion return that error rather than publishing a partial result.
pub struct PyBytesWriter<'py> {
    python: Python<'py>,
    #[cfg(not(Py_LIMITED_API))]
    writer: PyResult<NonNull<ffi::compat::PyBytesWriter>>,
    #[cfg(Py_LIMITED_API)]
    buffer: Vec<u8>,
}

impl<'py> PyBytesWriter<'py> {
    /// Create a new `PyBytesWriter` with the specified initial capacity.
    #[inline]
    #[cfg_attr(Py_LIMITED_API, allow(clippy::unnecessary_wraps))]
    pub(super) fn with_capacity(py: Python<'py>, capacity: usize) -> PyResult<Self> {
        cfg_select! {
            not(Py_LIMITED_API) => {
                NonNull::new(unsafe { PyBytesWriter_Create(capacity.try_into()?) }).map_or_else(
                    || Err(PyErr::fetch(py)),
                    |writer| {
                        let mut writer = PyBytesWriter {
                            python: py,
                            writer: Ok(writer),
                        };
                        if capacity > 0 {
                            // SAFETY: By setting the length to 0, we ensure no bytes are considered uninitialized.
                            unsafe { writer.set_len(0)? };
                        }
                        Ok(writer)
                    },
                )
            }
            Py_LIMITED_API => Ok(PyBytesWriter {
                python: py,
                buffer: Vec::with_capacity(capacity),
            }),
        }
    }

    /// Returns true if [`PyBytesWriter::len`] is 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the current length of the internal buffer, or zero after a native write failure.
    #[inline]
    pub fn len(&self) -> usize {
        cfg_select! {
            not(Py_LIMITED_API) => self.writer.as_ref().map_or(0, |writer| unsafe {
                PyBytesWriter_GetSize(writer.as_ptr()) as _
            }),
            Py_LIMITED_API => self.buffer.len(),
        }
    }

    #[cfg(not(Py_LIMITED_API))]
    fn writer_pointer(&self) -> PyResult<NonNull<ffi::compat::PyBytesWriter>> {
        self.writer
            .as_ref()
            .copied()
            .map_err(|error| error.clone_ref(self.python))
    }

    #[cfg(not(Py_LIMITED_API))]
    fn check_write(&mut self, result: PyResult<()>) -> PyResult<()> {
        if let Err(error) = result {
            let previous = core::mem::replace(&mut self.writer, Err(error.clone_ref(self.python)));
            if let Ok(writer) = previous {
                // SAFETY: the failing operation leaves only discard permitted.
                // This transition removes the handle before releasing it.
                unsafe { PyBytesWriter_Discard(writer.as_ptr()) };
            }
            return Err(error);
        }
        Ok(())
    }

    #[inline]
    #[cfg(not(Py_LIMITED_API))]
    fn as_mut_ptr(&mut self) -> PyResult<*mut u8> {
        // SAFETY: only a live writer can pass writer_pointer.
        Ok(unsafe { PyBytesWriter_GetData(self.writer_pointer()?.as_ptr()) as _ })
    }

    /// Set the length of the internal buffer to `new_len`. The new bytes are uninitialized.
    ///
    /// # Safety
    /// On success, the caller must ensure the new bytes are initialized. This will also make all pointers
    /// returned by `as_mut_ptr` invalid, so the caller must not hold any references to the buffer
    /// across this call.
    #[inline]
    #[cfg(not(Py_LIMITED_API))]
    unsafe fn set_len(&mut self, new_len: usize) -> PyResult<()> {
        let writer = self.writer_pointer()?;
        let result = new_len.try_into().map_err(PyErr::from).and_then(|size| {
            // SAFETY: the handle is live, and the caller initializes new bytes
            // before publication. A failed resize is discarded by check_write.
            unsafe {
                err::error_on_negative(self.python, PyBytesWriter_Resize(writer.as_ptr(), size))
            }
        });
        self.check_write(result)
    }

    /// Writes the `bytes` into `self`.
    #[allow(clippy::unnecessary_wraps)]
    pub fn write_bytes(&mut self, bytes: &[u8]) -> PyResult<()> {
        cfg_select! {
            Py_LIMITED_API => self.buffer.extend_from_slice(bytes),

            Py_3_15 => {
                let writer = self.writer_pointer()?;
                // SAFETY: the writer owns its live storage, and a Rust byte
                // slice's length fits Py_ssize_t. The source remains borrowed
                // throughout the native growth and copy operation.
                let result = unsafe {
                    err::error_on_negative(
                        self.python,
                        ffi::PyBytesWriter_WriteBytes(
                            writer.as_ptr(),
                            bytes.as_ptr().cast(),
                            bytes.len() as ffi::Py_ssize_t,
                        ),
                    )
                };
                self.check_write(result)?;
            }

            _ => {
                let len = bytes.len();
                let pos = self.len();
                // Each length fits isize, so the sum fits usize. set_len
                // checks Python's smaller signed size limit.
                let new_len = pos + len;

                // SAFETY: We write the new uninitialized bytes below.
                unsafe { self.set_len(new_len)? }

                // SAFETY: We have ensured enough capacity above.
                unsafe {
                    core::ptr::copy_nonoverlapping(bytes.as_ptr(), self.as_mut_ptr()?.add(pos), len)
                };
            }
        }

        Ok(())
    }
}

impl<'py> TryFrom<PyBytesWriter<'py>> for Bound<'py, PyBytes> {
    type Error = PyErr;

    #[inline]
    fn try_from(value: PyBytesWriter<'py>) -> Result<Self, Self::Error> {
        let py = value.python;
        cfg_select! {
            // SAFETY:
            //  - we no longer use `value` after this call
            //  - `PyBytesWriter_Finish` will return a new reference to a bytes object on success
            not(Py_LIMITED_API) => unsafe {
                let value = ManuallyDrop::new(value);
                // Move the sole handle or remembered error out without running
                // Drop. Finish consumes a live handle even when it fails.
                match core::ptr::read(&value.writer) {
                    Ok(writer) => PyBytesWriter_Finish(writer.as_ptr())
                        .assume_owned_or_err(py)
                        .cast_into_unchecked(),
                    Err(error) => Err(error),
                }
            },
            Py_LIMITED_API => Ok(PyBytes::new(py, &value.buffer)),
        }
    }
}

impl<'py> IntoPyObject<'py> for PyBytesWriter<'py> {
    type Target = PyBytes;
    type Output = Bound<'py, PyBytes>;
    type Error = PyErr;

    #[cfg(feature = "experimental-inspect")]
    const OUTPUT_TYPE: PyStaticExpr = PyBytes::TYPE_HINT;

    #[inline]
    fn into_pyobject(self, _py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.try_into()
    }
}

#[cfg(not(Py_LIMITED_API))]
impl<'py> Drop for PyBytesWriter<'py> {
    #[inline]
    fn drop(&mut self) {
        if let Ok(writer) = &self.writer {
            // SAFETY: failed writers were already discarded by check_write.
            unsafe { PyBytesWriter_Discard(writer.as_ptr()) }
        }
    }
}

#[cfg(all(wip_feature_std, not(Py_LIMITED_API)))]
impl std::io::Write for PyBytesWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_all(buf)?;
        Ok(buf.len())
    }

    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        let len = bufs
            .iter()
            .try_fold(0_usize, |total, buf| total.checked_add(buf.len()))
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "byte length overflow")
            })?;
        let pos = self.len();
        let new_len = pos.checked_add(len).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "byte length overflow")
        })?;

        // SAFETY: We write the new uninitialized bytes below.
        unsafe { self.set_len(new_len)? }

        // SAFETY: We ensured enough capacity above and the ptr will be valid because we will not be
        // resizing the buffer until we have written all the data.
        let mut ptr = unsafe { self.as_mut_ptr()?.add(pos) };

        for buf in bufs {
            // SAFETY: We have ensured enough capacity above.
            unsafe { core::ptr::copy_nonoverlapping(buf.as_ptr(), ptr, buf.len()) };

            // SAFETY: We just wrote buf.len() bytes
            ptr = unsafe { ptr.add(buf.len()) };
        }
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.write_bytes(buf)?;
        Ok(())
    }
}

#[cfg(all(wip_feature_std, Py_LIMITED_API))]
impl std::io::Write for PyBytesWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.write(buf)
    }

    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        self.buffer.write_vectored(bufs)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.buffer.write_all(buf)
    }

    fn write_fmt(&mut self, args: core::fmt::Arguments<'_>) -> std::io::Result<()> {
        self.buffer.write_fmt(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(Py_LIMITED_API))]
    use crate::types::PyAnyMethods;
    #[cfg(any(wip_feature_std, Py_LIMITED_API))]
    use crate::types::PyBytesMethods;
    #[cfg(wip_feature_std)]
    use std::io::{IoSlice, Write};

    #[test]
    fn new_byteswriter_is_empty() {
        Python::attach(|py| {
            let writer = PyBytesWriter::with_capacity(py, 0).unwrap();
            assert!(writer.is_empty());
        });
    }

    #[test]
    #[cfg(not(Py_LIMITED_API))]
    fn failed_byteswriter_growth_discards_unfinished_storage() {
        Python::attach(|py| {
            let mut writer = PyBytesWriter::with_capacity(py, 512).unwrap();
            writer.write_bytes(&[0x81; 512]).unwrap();
            // SAFETY: the writer is live. No Python bytes allocation can hold
            // PY_SSIZE_T_MAX payload bytes plus its object header, so this
            // request must fail without acquiring uninitialized storage.
            let error = unsafe { writer.set_len(ffi::PY_SSIZE_T_MAX as usize) }.unwrap_err();
            assert!(
                error.is_instance_of::<crate::exceptions::PyMemoryError>(py)
                    || error.is_instance_of::<crate::exceptions::PyOverflowError>(py)
            );
            assert!(writer.is_empty());
            let repeated = writer.write_bytes(b"tail").unwrap_err();
            assert!(repeated.value(py).is(error.value(py)));
            let finished = Bound::<'_, PyBytes>::try_from(writer).unwrap_err();
            assert!(finished.value(py).is(error.value(py)));
        });
    }

    #[test]
    #[cfg_attr(not(wip_feature_std), ignore)]
    fn test_io_write() {
        #[cfg(wip_feature_std)]
        Python::attach(|py| {
            let buf = b"hallo world";
            let mut writer = PyBytesWriter::with_capacity(py, 0).unwrap();
            assert_eq!(writer.write(buf).unwrap(), 11);
            let bytes: Bound<'_, PyBytes> = writer.try_into().unwrap();
            assert_eq!(bytes.as_bytes(), buf);
        })
    }

    #[test]
    #[cfg_attr(not(wip_feature_std), ignore)]
    fn test_pre_allocated() {
        #[cfg(wip_feature_std)]
        Python::attach(|py| {
            let buf = b"hallo world";
            let mut writer = PyBytesWriter::with_capacity(py, buf.len()).unwrap();
            assert_eq!(writer.len(), 0, "Writer position should be zero");
            assert_eq!(writer.write(buf).unwrap(), 11);
            let bytes: Bound<'_, PyBytes> = writer.try_into().unwrap();
            assert_eq!(bytes.as_bytes(), buf);
        })
    }

    #[test]
    #[cfg_attr(not(wip_feature_std), ignore)]
    fn test_io_write_vectored() {
        #[cfg(wip_feature_std)]
        Python::attach(|py| {
            let bufs = [IoSlice::new(b"hallo "), IoSlice::new(b"world")];
            let mut writer = PyBytesWriter::with_capacity(py, 0).unwrap();
            assert_eq!(writer.write_vectored(&bufs).unwrap(), 11);
            let bytes: Bound<'_, PyBytes> = writer.try_into().unwrap();
            assert_eq!(bytes.as_bytes(), b"hallo world");
        })
    }

    #[test]
    #[cfg_attr(not(wip_feature_std), ignore)]
    fn test_io_write_vectored_large() {
        #[cfg(wip_feature_std)]
        Python::attach(|py| {
            let large_data = vec![b'\n'; 1024]; // 1 KB
            let bufs = [
                IoSlice::new(b"hallo"),
                IoSlice::new(&large_data),
                IoSlice::new(b"world"),
            ];
            let mut writer = PyBytesWriter::with_capacity(py, 0).unwrap();
            assert_eq!(writer.write_vectored(&bufs).unwrap(), 1034);
            let bytes: Bound<'_, PyBytes> = writer.try_into().unwrap();
            assert!(bytes.as_bytes().starts_with(b"hallo\n"));
            assert!(bytes.as_bytes().ends_with(b"world"));
            assert_eq!(bytes.as_bytes().len(), 1034);
        })
    }

    #[test]
    #[cfg_attr(not(wip_feature_std), ignore)]
    fn test_large_data() {
        #[cfg(wip_feature_std)]
        Python::attach(|py| {
            let mut writer = PyBytesWriter::with_capacity(py, 0).unwrap();
            let large_data = vec![0; 1024]; // 1 KB
            writer.write_all(&large_data).unwrap();
            let bytes: Bound<'_, PyBytes> = writer.try_into().unwrap();
            assert_eq!(bytes.as_bytes(), large_data.as_slice());
        })
    }
}
