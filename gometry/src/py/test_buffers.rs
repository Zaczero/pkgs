//! Test-owned PEP-3118 producers with genuine `suboffsets` (indirect / PIL-style).
//!
//! CPython's `_testbuffer` is not shipped in uv builds, so the ordinary suite
//! cannot exercise the production suboffset guards at
//! `boundary/coordinate_input.rs` without a producer under our control.
//!
//! Private surface only (`_IndirectFloat64Buffer`, `_indirect_float64_buffer`);
//! stripped from `__all__` by the leading underscore. Not a public API — it is
//! absent from `gometry.__all__`, from `dir(gometry)`, and from `_lib.__all__`.
//!
//! DELIBERATELY NOT FEATURE-GATED, and this has been re-litigated once. Gating
//! it out of the default build looks attractive — it is ~240 lines of raw
//! buffer-protocol `unsafe` in a shipped wheel that exists only for tests. But
//! it is the *sole* enabler of executable coverage for the suboffset guards:
//! `_testbuffer` is unavailable here, so with this module gated off the tests in
//! `test_buffer_admission.py` and `test_unsafe_boundaries.py` silently skip
//! and a real soundness fix goes back to being unverified — which is the state
//! that let the original defect (indirect buffers read as coordinate data) live.
//! Trading proven coverage of a memory-safety guard for wheel tidiness is the
//! wrong side of that bargain. If this is ever revisited, the replacement must
//! keep those tests *running*, not merely keep them present.

#![allow(
    clippy::undocumented_unsafe_blocks,
    clippy::missing_const_for_fn,
    reason = "buffer-protocol slots document safety at the method level; PyO3 pymethods cannot be const"
)]

use std::ffi::{c_int, c_void};
use std::ptr;

use pyo3::exceptions::{PyBufferError, PyValueError};
use pyo3::prelude::*;
use pyo3::{Bound, ffi};

/// Owned 1-D or 2-D `f64` buffer that exports a genuine indirect view.
///
/// Layout matches CPython `ND_PIL` / PEP-3118 indirection:
/// - 1-D: `buf` is a pointer table; `suboffsets[0] == 0`.
/// - 2-D: `buf` is a row-pointer table; `suboffsets[0] == 0`, `suboffsets[1] == -1`
///   with contiguous row storage.
#[pyclass(
    name = "_IndirectFloat64Buffer",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
pub(crate) struct IndirectFloat64Buffer {
    /// Contiguous coordinate payload (row-major for 2-D).
    values: Box<[f64]>,
    /// Pointer table as `usize` addresses (one per element for 1-D, per row for
    /// 2-D). Stored as integers so the pyclass is `Send + Sync`; reconstructed
    /// to `*const f64` only inside `__getbuffer__` while the owner is live.
    pointers: Box<[usize]>,
    shape: Box<[isize]>,
    strides: Box<[isize]>,
    suboffsets: Box<[isize]>,
}

impl IndirectFloat64Buffer {
    fn from_values_shape(values: Vec<f64>, shape: Vec<usize>) -> PyResult<Self> {
        match shape.as_slice() {
            [n] => {
                let n = *n;
                if values.len() != n {
                    return Err(PyValueError::new_err(format!(
                        "values length {} does not match shape ({n},)",
                        values.len()
                    )));
                }
                let values: Box<[f64]> = values.into_boxed_slice();
                let pointers: Box<[usize]> = (0..n)
                    .map(|i| std::ptr::from_ref(&values[i]) as usize)
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                Ok(Self {
                    values,
                    pointers,
                    shape: Box::new([isize::try_from(n)
                        .map_err(|_| PyValueError::new_err("shape dimension too large"))?]),
                    strides: Box::new([isize::try_from(std::mem::size_of::<usize>())
                        .expect("pointer size fits isize")]),
                    suboffsets: Box::new([0]),
                })
            },
            [n, d] => {
                let n = *n;
                let d = *d;
                let total = n
                    .checked_mul(d)
                    .ok_or_else(|| PyValueError::new_err("shape product overflows"))?;
                if values.len() != total {
                    return Err(PyValueError::new_err(format!(
                        "values length {} does not match shape ({n}, {d})",
                        values.len()
                    )));
                }
                let values: Box<[f64]> = values.into_boxed_slice();
                // One pointer per row into the contiguous row-major payload.
                let pointers: Box<[usize]> = if n == 0 || d == 0 {
                    Box::from([])
                } else {
                    (0..n)
                        .map(|row| std::ptr::from_ref(&values[row * d]) as usize)
                        .collect::<Vec<_>>()
                        .into_boxed_slice()
                };
                let n_i = isize::try_from(n)
                    .map_err(|_| PyValueError::new_err("shape dimension too large"))?;
                let d_i = isize::try_from(d)
                    .map_err(|_| PyValueError::new_err("shape dimension too large"))?;
                let ptr_stride =
                    isize::try_from(std::mem::size_of::<usize>()).expect("pointer size fits isize");
                let item_stride =
                    isize::try_from(std::mem::size_of::<f64>()).expect("f64 size fits isize");
                Ok(Self {
                    values,
                    pointers,
                    shape: Box::new([n_i, d_i]),
                    strides: Box::new([ptr_stride, item_stride]),
                    // Row indirection; columns are contiguous within each row.
                    suboffsets: Box::new([0, -1]),
                })
            },
            _ => Err(PyValueError::new_err(
                "shape must be 1-D (n,) or 2-D (n, d)",
            )),
        }
    }
}

#[pymethods]
impl IndirectFloat64Buffer {
    /// # Safety
    /// Buffer-protocol slot; CPython guarantees a valid `view` pointer.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("view is null"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            return Err(PyBufferError::new_err(
                "gometry indirect test buffers are read-only",
            ));
        }
        // Snapshot layout fields under a short borrow, then consume `slf` as
        // the view owner. The Box heap pointers remain valid for the view
        // lifetime because the same frozen instance is retained via `view.obj`.
        let (len, itemsize_i, ndim_i, buf, shape_ptr, strides_ptr, suboffsets_ptr) = {
            let this = slf.get();
            let ndim = this.shape.len();
            let itemsize = std::mem::size_of::<f64>();
            let n_items = this.values.len();
            let len = n_items
                .checked_mul(itemsize)
                .and_then(|n| isize::try_from(n).ok())
                .ok_or_else(|| PyBufferError::new_err("buffer is too large for CPython"))?;
            let itemsize_i = isize::try_from(itemsize)
                .map_err(|_| PyBufferError::new_err("buffer item size is too large for CPython"))?;
            let ndim_i = c_int::try_from(ndim)
                .map_err(|_| PyBufferError::new_err("ndim too large for CPython"))?;
            (
                len,
                itemsize_i,
                ndim_i,
                this.pointers.as_ptr().cast::<c_void>().cast_mut(),
                this.shape.as_ptr().cast_mut(),
                this.strides.as_ptr().cast_mut(),
                this.suboffsets.as_ptr().cast_mut(),
            )
        };

        // SAFETY: view is non-null; owner handle keeps values/pointers/shape
        // alive for the life of the buffer view; suboffsets are genuine
        // indirection (not null) so the flat path must not be taken.
        unsafe {
            (*view).obj = slf.into_any().into_ptr();
            (*view).buf = buf;
            (*view).len = len;
            (*view).readonly = 1;
            (*view).itemsize = itemsize_i;
            (*view).ndim = ndim_i;
            (*view).internal = ptr::null_mut();
            (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
                shape_ptr
            } else {
                ptr::null_mut()
            };
            (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
                strides_ptr
            } else {
                ptr::null_mut()
            };
            // Always export suboffsets — this is the whole point of the producer.
            (*view).suboffsets = suboffsets_ptr;
            (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
                // Static CStr — never freed in release.
                c"d".as_ptr().cast_mut()
            } else {
                ptr::null_mut()
            };
        }
        Ok(())
    }

    /// # Safety
    /// Paired with `__getbuffer__`; no view-private allocation to free.
    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}

    /// Number of stored `f64` values (not pointer-table entries).
    fn __len__(&self) -> usize {
        self.values.len()
    }

    /// Shape as a Python tuple (for test assertions).
    #[getter]
    fn shape(&self) -> Vec<isize> {
        self.shape.to_vec()
    }

    /// True when the export sets a non-null `suboffsets` pointer table.
    #[getter]
    fn is_indirect(&self) -> bool {
        true
    }
}

/// Build a test-owned indirect `f64` buffer with genuine PEP-3118 `suboffsets`.
///
/// Parameters
/// ----------
/// values : sequence of float
///     Contiguous payload in row-major order.
/// shape : sequence of int
///     ``(n,)`` for a 1-D column or ``(n, d)`` for an N×D matrix.
///
/// Returns
/// -------
/// _IndirectFloat64Buffer
///     Object exporting an indirect buffer view (PIL-style pointer table).
#[pyfunction]
#[pyo3(name = "_indirect_float64_buffer", text_signature = "(values, shape)")]
pub(crate) fn indirect_float64_buffer(
    values: Vec<f64>,
    shape: Vec<usize>,
) -> PyResult<IndirectFloat64Buffer> {
    IndirectFloat64Buffer::from_values_shape(values, shape)
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<IndirectFloat64Buffer>()?;
    module.add_function(wrap_pyfunction!(indirect_float64_buffer, module)?)?;
    Ok(())
}
