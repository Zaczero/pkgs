//! Coordinate-vertex argument parsing and broadcasting for the column-form
//! constructors (`line_string(x=, y=, …)`, `multi_point(...)`, etc.).
//!
//! `CoordinateInput` holds parsed `x`/`y`/`z`/`m` columns; the helpers coerce
//! Python sequences/scalars, broadcast scalar lanes to a common length, and
//! validate equal column lengths. Re-exported at the crate root via `use
//! super::*`.
use numpy::{PyUntypedArray, PyUntypedArrayMethods as _};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyFloat, PyInt};

use crate::boundary::buffer_endian;
use crate::boundary::buffer_endian::{
    buffer_item_count, buffer_to_column_f64, buffer_to_vec_f64, try_typed_buffer,
};
use crate::geometry::column_all_finite;
use crate::py::errors;
use crate::py::errors::{GeometryError, InvalidGeometryError};
use crate::py::numpy::reject_masked_array;
use crate::{
    CoordSeq, PyBuffer, collect_py_iter_at_most_from_iter, collect_py_iter_exact,
    collect_py_iter_exact_from_iter, collect_py_iter_from_iter, f64_required, f64_scalar_named,
    finite_coordinate_required, finite_f64_required, py_iter_or_error,
};

pub(crate) struct CoordinateInput {
    pub values: Vec<f64>,
    pub scalar: bool,
}

/// How a float input is admitted: coordinates are finite values from a
/// scalar or one-dimensional column; parameters accept any float.
#[derive(Clone, Copy)]
pub(crate) enum FloatInput {
    Coordinate,
    Parameter,
}

impl FloatInput {
    pub(crate) fn item(self, name: &str, value: &Bound<'_, PyAny>) -> PyResult<f64> {
        let parsed = match self {
            Self::Coordinate => crate::finite_coordinate_required(name, value),
            Self::Parameter => f64_required(name, value),
        };
        match parsed {
            Err(error)
                if error.is_instance_of::<PyTypeError>(value.py())
                    && value
                        .getattr_opt(pyo3::intern!(value.py(), "__float__"))?
                        .is_none() =>
            {
                Err(not_a_float_iterable(name))
            },
            parsed => parsed,
        }
    }
}

fn not_a_float_iterable(name: &str) -> PyErr {
    PyTypeError::new_err(format!(
        "{name} must be a float or an iterable of finite floats"
    ))
}

/// Coordinate columns are scalar or one-dimensional.  A two-dimensional
/// numeric array is a coordinate matrix, not a column; accepting its buffer
/// item count would silently flatten rows into unrelated points.
pub(crate) fn reject_multidimensional_coordinate_array(
    value: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<()> {
    reject_masked_array(value)?;
    let dimensions = if let Some(buffer) = try_typed_buffer::<f64>(value)? {
        buffer_item_count(&buffer)?;
        buffer.dimensions()
    } else if let Ok(array) = value.cast::<PyUntypedArray>() {
        array.ndim()
    } else {
        return Ok(());
    };
    reject_coordinate_buffer_rank(dimensions, name)
}

fn reject_coordinate_buffer_rank(dimensions: usize, name: &str) -> PyResult<()> {
    if dimensions > 1 {
        Err(PyTypeError::new_err(format!(
            "{name} must be a scalar or a one-dimensional sequence; got a {dimensions}-dimensional array"
        )))
    } else {
        Ok(())
    }
}

fn coordinate_scalar_named(name: &str, value: &Bound<'_, PyAny>) -> PyResult<Option<f64>> {
    let scalar = match f64_scalar_named(name, value) {
        Ok(scalar) => scalar,
        Err(_error) if value.cast::<PyInt>().is_ok() || value.cast::<PyFloat>().is_ok() => {
            return Err(InvalidGeometryError::new_err(format!(
                "{name} must be finite"
            )));
        },
        Err(error) => return Err(error),
    };
    let Some(value) = scalar else {
        return Ok(None);
    };
    if value.is_finite() {
        Ok(Some(value))
    } else {
        Err(InvalidGeometryError::new_err(format!(
            "{name} must be finite"
        )))
    }
}

/// Broadcastable numeric kwarg with a real Rust default: omission uses
/// `Default`, explicit Python ``None`` is rejected by [`FromPyObject`], and
/// any other value is held as `Supplied` until the receiver length is known.
pub enum DefaultedF64Input {
    Default(f64),
    Supplied(Py<PyAny>),
}

impl DefaultedF64Input {
    /// Resolve a scalar-only parameter while retaining the wrapper's real
    /// Python default in the generated signature.
    pub(crate) fn resolve_scalar(self, py: Python<'_>, name: &str) -> PyResult<f64> {
        match self {
            Self::Default(value) => Ok(value),
            Self::Supplied(value) => finite_f64_required(name, value.bind(py)),
        }
    }

    /// Resolve a scalar coordinate ordinate, preserving the structural
    /// `InvalidGeometryError` used by coordinate admission.
    pub(crate) fn resolve_coordinate_scalar(self, py: Python<'_>, name: &str) -> PyResult<f64> {
        match self {
            Self::Default(value) => Ok(value),
            Self::Supplied(value) => finite_coordinate_required(name, value.bind(py)),
        }
    }

    /// Resolve to [`F64Param`] after the unary/array receiver length is known.
    pub(crate) fn resolve(self, py: Python<'_>, name: &str, len: usize) -> PyResult<F64Param> {
        match self {
            Self::Default(value) => Ok(F64Param::Scalar(value)),
            Self::Supplied(value) => F64Param::parse(value.bind(py), name, len),
        }
    }

    /// Resolve without a finite check when the receiving parameter has a
    /// stricter domain validator that supplies the public diagnostic.
    pub(crate) fn resolve_raw(self, py: Python<'_>, name: &str, len: usize) -> PyResult<F64Param> {
        match self {
            Self::Default(value) => Ok(F64Param::Scalar(value)),
            Self::Supplied(value) => F64Param::parse_raw(value.bind(py), name, len),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for DefaultedF64Input {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_none() {
            return Err(PyTypeError::new_err(
                "parameter cannot be None; omit it to use the default",
            ));
        }
        Ok(Self::Supplied(value.as_any().clone().unbind()))
    }
}

/// Integer counterpart of [`DefaultedF64Input`] for broadcastable integer
/// kwargs (e.g. ``smooth(iterations=2)``).
pub enum DefaultedI64Input {
    Default(i64),
    Supplied(Py<PyAny>),
}

impl DefaultedI64Input {
    /// Resolve to [`I64Param`] after the unary/array receiver length is known.
    pub(crate) fn resolve(self, py: Python<'_>, name: &str, len: usize) -> PyResult<I64Param> {
        match self {
            Self::Default(value) => Ok(I64Param::Scalar(value)),
            Self::Supplied(value) => I64Param::parse(value.bind(py), name, len),
        }
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for DefaultedI64Input {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if value.is_none() {
            return Err(PyTypeError::new_err(
                "parameter cannot be None; omit it to use the default",
            ));
        }
        Ok(Self::Supplied(value.as_any().clone().unbind()))
    }
}

/// A finite-`f64` transform argument that is EITHER one value broadcast to
/// every row of a `GeometryArray`, OR a per-row sequence (`list`/`tuple`/
/// `ndarray`/buffer/iterable) of length equal to the array. The scalar is the
/// 0-d case of the same idiom (numpy-style broadcasting), so `arr.buffer(1.0)` and
/// `arr.buffer([1.0, 2.0, …])` share one path; a scalar keeps the existing
/// columnar/packed fast lanes via [`as_scalar`](Self::as_scalar). Values are
/// validated finite at the boundary, so internal kernels trust them (`DbC`).
#[derive(Clone)]
pub(crate) enum F64Param {
    /// One value applied to every row.
    Scalar(f64),
    /// One value per row — `len()` equals the array length by construction.
    PerElement(Box<[f64]>),
}

impl F64Param {
    /// Parse against the array length `len`: a scalar (`int`/`float`/0-d) or a
    /// length-`len` sequence of finite floats. The default non-finite message
    /// suits most magnitudes; geodesic surfaces pass their own phrasing via
    /// [`parse_with_error`](Self::parse_with_error).
    pub(crate) fn parse(value: &Bound<'_, PyAny>, name: &str, len: usize) -> PyResult<Self> {
        Self::parse_with_error(value, name, len, &|| format!("{name} must be finite"))
    }

    /// Parse a scalar or a per-row sequence without applying an operation's
    /// value-domain validator. Callers that use this form own that validator.
    pub(crate) fn parse_raw(value: &Bound<'_, PyAny>, name: &str, len: usize) -> PyResult<Self> {
        if let Some(value) = f64_scalar_named(name, value)? {
            return Ok(Self::Scalar(value));
        }
        let values =
            float_values_exact(value.py(), value, name, FloatInput::Parameter, len, |got| {
                GeometryError::new_err(format!(
                    "{name} must be a float or a length-{len} sequence (one per geometry); \
                     got length {got}"
                ))
            })?;
        Ok(Self::PerElement(values.into_boxed_slice()))
    }

    /// [`parse`](Self::parse) with a caller-supplied non-finite message.
    pub(crate) fn parse_with_error(
        value: &Bound<'_, PyAny>,
        name: &str,
        len: usize,
        non_finite: &dyn Fn() -> String,
    ) -> PyResult<Self> {
        if let Some(value) = f64_scalar_named(name, value)? {
            if value.is_finite() {
                return Ok(Self::Scalar(value));
            }
            return Err(errors::float_parameter_error(non_finite(), name, value));
        }
        let values =
            float_values_exact(value.py(), value, name, FloatInput::Parameter, len, |got| {
                GeometryError::new_err(format!(
                    "{name} must be a float or a length-{len} sequence (one per geometry); \
                 got length {got}"
                ))
            })?;
        if let Some(&value) = values.iter().find(|value| !value.is_finite()) {
            Err(errors::float_parameter_error(non_finite(), name, value))
        } else {
            Ok(Self::PerElement(values.into_boxed_slice()))
        }
    }

    /// Validate every element is finite, with the kernel-standard
    /// ``{name} must be finite, got {v}`` message. For `parse_raw` lanes
    /// whose downstream kernel cannot own the check (the packed
    /// `line_interpolate_point` row loop interpolates at coordseq level).
    pub(crate) fn ensure_finite(&self, name: &'static str) -> PyResult<()> {
        match self {
            Self::Scalar(value) => {
                if !value.is_finite() {
                    return Err(errors::float_parameter_error(
                        format!("{name} must be finite"),
                        name,
                        *value,
                    ));
                }
            },
            Self::PerElement(values) => {
                for &value in values {
                    if !value.is_finite() {
                        return Err(errors::float_parameter_error(
                            format!("{name} must be finite"),
                            name,
                            value,
                        ));
                    }
                }
            },
        }
        Ok(())
    }

    /// The value for row `row`. Trusts `row < len` (the caller iterates the
    /// array it was parsed against), so `PerElement` indexes
    /// unchecked-in-spirit via the validated slice.
    pub(crate) fn get(&self, row: usize) -> f64 {
        match self {
            Self::Scalar(value) => *value,
            Self::PerElement(values) => values[row],
        }
    }

    /// Validate every value at the boundary (with the GIL) so the detached
    /// per-row kernels trust them — e.g. an op whose magnitude must be
    /// non-negative checks the whole lane once, up front.
    pub(crate) fn try_validate(&self, check: impl Fn(f64) -> PyResult<()>) -> PyResult<()> {
        match self {
            Self::Scalar(value) => check(*value),
            Self::PerElement(values) => values.iter().copied().try_for_each(check),
        }
    }

    /// Reject any negative value, naming the parameter `name`.
    pub(crate) fn require_non_negative(&self, name: &'static str) -> PyResult<()> {
        self.try_validate(|value| {
            crate::NonNegative::try_new(name, value)
                .map(|_| ())
                .map_err(PyErr::from)
        })
    }

    /// The single value when scalar — lets a method keep its columnar/packed
    /// fast path for the common broadcast case, falling to the per-row lane
    /// only for a genuine per-element array.
    pub(crate) const fn as_scalar(&self) -> Option<f64> {
        match self {
            Self::Scalar(value) => Some(*value),
            Self::PerElement(_) => None,
        }
    }
}

/// A signed-integer transform argument broadcast like [`F64Param`]: one scalar
/// applies to every row, or a length-`len` per-element sequence.
#[derive(Clone)]
pub(crate) enum I64Param {
    Scalar(i64),
    PerElement(Box<[i64]>),
}

impl I64Param {
    pub(crate) fn parse(value: &Bound<'_, PyAny>, name: &str, len: usize) -> PyResult<Self> {
        if let Ok(array) = value.cast::<PyUntypedArray>() {
            match array.ndim() {
                0 => {
                    let scalar = crate::py_i64_required(name, value)?;
                    return Ok(Self::Scalar(scalar));
                },
                1 => {},
                ndim => {
                    return Err(PyTypeError::new_err(format!(
                        "{name} must be an integer or a one-dimensional length-{len} sequence \
                         (one per geometry); got a {ndim}-dimensional array"
                    )));
                },
            }
        } else if value.cast_exact::<pyo3::types::PyBool>().is_err() && crate::py_index_check(value)
        {
            let scalar = crate::py_i64_required(name, value)?;
            return Ok(Self::Scalar(scalar));
        }
        // Known receiver length: bound the drain so `itertools.repeat` cannot
        // hang (D11).
        let values = integer_values_exact(value, name, len, |got| {
            GeometryError::new_err(format!(
                "{name} must be an integer or a length-{len} sequence (one per geometry); \
                 got length {got}"
            ))
        })?;
        Ok(Self::PerElement(values.into_boxed_slice()))
    }

    pub(crate) fn get(&self, row: usize) -> i64 {
        match self {
            Self::Scalar(value) => *value,
            Self::PerElement(values) => values[row],
        }
    }

    pub(crate) fn try_validate(&self, check: impl Fn(i64) -> PyResult<()>) -> PyResult<()> {
        match self {
            Self::Scalar(value) => check(*value),
            Self::PerElement(values) => values.iter().copied().try_for_each(check),
        }
    }
}

fn map_integer_item(item: &Bound<'_, PyAny>, name: &str) -> PyResult<i64> {
    crate::py_i64_required(name, item)
}

/// Collect exactly `expected` integers; reject infinite/mismatched streams
/// without draining them (D11).
pub(crate) fn integer_values_exact(
    value: &Bound<'_, PyAny>,
    name: &str,
    expected: usize,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<i64>> {
    reject_masked_array(value)?;
    collect_py_iter_exact(
        value,
        expected,
        |item| map_integer_item(&item, name),
        on_len,
    )
}

/// Capture a coordinate column in owned native-endian storage. Contiguous and
/// simple strided buffers fill the column directly; general inputs use
/// [`float_values`]. The producer must remain quiescent until capture
/// returns; retaining a `PyBuffer` pins its allocation but does not synchronize
/// native writers.
pub(crate) fn coordinate_column_values(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<Vec<f64>> {
    reject_masked_array(value)?;
    if let Some(buffer) = try_typed_buffer::<f64>(value)? {
        let item_count = buffer_item_count(&buffer)?;
        reject_coordinate_buffer_rank(buffer.dimensions(), name)?;
        return coordinate_column_values_from_buffer(py, &buffer, item_count);
    }
    float_values(py, value, name, FloatInput::Coordinate)
}

/// Copy an already-admitted `f64` buffer into its final native-endian column.
/// Keeping the admission object across the caller's length checks avoids a
/// second `PyBuffer::get` and preserves the same contiguous/strided safety
/// paths as [`coordinate_column_values`]. `item_count` must come from
/// [`buffer_item_count`] for this buffer.
pub(crate) fn coordinate_column_values_from_buffer(
    py: Python<'_>,
    buffer: &PyBuffer<f64>,
    item_count: usize,
) -> PyResult<Vec<f64>> {
    if buffer.is_c_contiguous() {
        return buffer_to_column_f64(py, buffer, item_count);
    }
    // Strided 1-D (or flattened multi-D): fill the column via a strided walk.
    strided_buffer_to_column_f64(py, buffer, item_count)
}

/// Capture a non-contiguous buffer in an owned column.
///
/// Each element is read through [`pyo3::buffer::ReadOnlyCell`] — never as a
/// plain `&f64` / `&[f64]` over buffer memory. `ReadOnlyCell::get` is a
/// non-atomic load, and the fallback copy is an ordinary copy rather than a
/// synchronization protocol: the producer must stay quiescent until this
/// capture returns. Afterward only the owned column is retained.
fn strided_buffer_to_column_f64(
    py: Python<'_>,
    buffer: &PyBuffer<f64>,
    len: usize,
) -> PyResult<Vec<f64>> {
    use pyo3::buffer::ReadOnlyCell;

    // The strided fast path below reads through a raw pointer. When the
    // exporter does not support direct capture, take the ordinary contiguous
    // copy the suboffset case already falls back to. Neither choice
    // synchronizes a native writer; the producer remains quiescent for both.
    if !buffer_endian::exporter_supports_direct_capture(py, buffer) {
        return buffer_to_vec_f64(py, buffer, len);
    }

    // Indirect (PIL-style) buffers store pointers in the strided slots;
    // `suboffsets[n] >= 0` requires a pointer dereference before adding the
    // suboffset. The flat `buf_ptr + i*stride` arithmetic would read the
    // pointer-table bytes as f64 coordinates. Route through PyO3's safe
    // contiguous copy which honors suboffsets via `PyBuffer_GetPointer`.
    if buffer.suboffsets().is_some() {
        return buffer_to_vec_f64(py, buffer, len);
    }
    // General strides and byte order use PyO3's contiguous copy. A simple
    // native-endian column can fill its destination directly.
    if buffer.dimensions() == 1 && buffer_endian::buffer_format_is_native_endian(buffer.format()) {
        let stride = buffer.strides()[0];
        let item = std::mem::size_of::<f64>() as isize;
        if stride % item == 0 {
            check_1d_byte_stride_span(len, stride)?;
            let step = stride / item;
            let mut values = vec![0.0; len];
            // ReadOnlyCell is transparent over T; treating each strided slot as
            // a cell matches PyBuffer::as_slice for the contiguous case.
            let base = buffer.buf_ptr().cast::<ReadOnlyCell<f64>>();
            for (i, value) in values.iter_mut().enumerate() {
                let offset = (i as isize).checked_mul(step).ok_or_else(|| {
                    pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows")
                })?;
                // SAFETY: PyBuffer is held; length and stride were validated;
                // no suboffsets are present, so each step addresses an element
                // in the declared range for CPython and NumPy exporters.
                *value = unsafe { (*base.offset(offset)).get() };
            }
            return Ok(values);
        }
    }
    buffer_to_vec_f64(py, buffer, len)
}

fn check_1d_byte_stride_span(len: usize, stride: isize) -> PyResult<()> {
    let Some(last_index) = len.checked_sub(1) else {
        return Ok(());
    };
    let index = isize::try_from(last_index)
        .map_err(|_| pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows"))?;
    index.checked_mul(stride).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows")
    })?;
    Ok(())
}

/// Build a [`CoordSeq`] from an indirect (suboffset) N×D `f64` buffer via
/// PyO3's safe contiguous copy, then de-interleave into SoA columns.
fn coordseq_from_indirect_nd_buffer(
    py: Python<'_>,
    buffer: &PyBuffer<f64>,
    item_count: usize,
    n: usize,
    d: usize,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
) -> PyResult<CoordSeq> {
    let flat = buffer_to_vec_f64(py, buffer, item_count)?;
    let mut xs = Vec::with_capacity(n);
    let mut ys = Vec::with_capacity(n);
    let mut zs_col = (d >= 3).then(|| Vec::with_capacity(n));
    let mut ms_col = (d >= 4).then(|| Vec::with_capacity(n));
    for row in 0..n {
        let base = row * d;
        xs.push(flat[base]);
        ys.push(flat[base + 1]);
        if let Some(zs) = zs_col.as_mut() {
            zs.push(flat[base + 2]);
        }
        if let Some(ms) = ms_col.as_mut() {
            ms.push(flat[base + 3]);
        }
    }
    let mut zs = zs_col;
    let mut ms = ms_col;
    if let Some(z_col) = z {
        zs = Some(float_values_exact(
            z_col.py(),
            z_col,
            "z",
            FloatInput::Coordinate,
            n,
            |got| {
                InvalidGeometryError::new_err(format!(
                    "z must have the same length as x/y coordinates (got length {got})"
                ))
            },
        )?);
    }
    if let Some(m_col) = m {
        ms = Some(float_values_exact(
            m_col.py(),
            m_col,
            "m",
            FloatInput::Coordinate,
            n,
            |got| {
                InvalidGeometryError::new_err(format!(
                    "m must have the same length as x/y coordinates (got length {got})"
                ))
            },
        )?);
    }
    Ok(CoordSeq::try_from_vecs(xs, ys, zs, ms)?)
}

/// Try to build a final SoA [`CoordSeq`] from a contiguous (or regularly
/// strided) N×D numeric buffer directly into owned columns.
/// Returns `Ok(None)` when the value is not an N×D
/// float buffer (caller falls back to the general extractor).
///
/// `z`/`m` optional columns override inline ordinates and cannot combine with
/// D>2 buffers (same rule as [`crate::py::support::extract::extract_points`]).
pub(crate) fn try_coordseq_from_nd_buffer(
    value: &Bound<'_, PyAny>,
    z: Option<&Bound<'_, PyAny>>,
    m: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<CoordSeq>> {
    reject_masked_array(value)?;
    let Some(buffer) = try_typed_buffer::<f64>(value)? else {
        return Ok(None);
    };
    let item_count = buffer_item_count(&buffer)?;
    if buffer.dimensions() != 2 {
        return Ok(None);
    }
    let Some(&[n, d]) = buffer.shape().first_chunk::<2>() else {
        return Ok(None);
    };
    if !(2..=4).contains(&d) {
        return Ok(None);
    }
    if d > 2 && (z.is_some() || m.is_some()) {
        return Err(InvalidGeometryError::new_err(
            "inline Z/M coordinates cannot be combined with z/m arrays",
        ));
    }
    // Indirect (PIL-style) N×D buffers need pointer indirection via suboffsets;
    // the flat row/col stride arithmetic would read the pointer table as
    // coordinates. Snapshot through PyO3's safe contiguous copy.
    if buffer.suboffsets().is_some() {
        return coordseq_from_indirect_nd_buffer(value.py(), &buffer, item_count, n, d, z, m)
            .map(Some);
    }
    let Some(&[row_stride, col_stride]) = buffer.strides().first_chunk::<2>() else {
        return Ok(None);
    };
    let item = std::mem::size_of::<f64>() as isize;
    if row_stride % item != 0 || col_stride % item != 0 {
        return Ok(None);
    }
    let row_step = row_stride / item;
    let col_step = col_stride / item;
    // The direct stride walk and the ordinary copy both require a quiescent
    // producer; the copy path is not a native-writer synchronization protocol.
    if !buffer_endian::exporter_supports_direct_capture(value.py(), &buffer) {
        return coordseq_from_indirect_nd_buffer(value.py(), &buffer, item_count, n, d, z, m)
            .map(Some);
    }
    check_2d_byte_stride_corners(n, d, row_stride, col_stride)?;
    let native = buffer_endian::buffer_format_is_native_endian(buffer.format());
    let swap = !native;

    let fill_column = |col: usize| -> crate::error::Result<Vec<f64>> {
        use pyo3::buffer::ReadOnlyCell;

        let mut values = vec![0.0; n];
        // Load each element through ReadOnlyCell rather than forming a slice
        // over foreign storage. The loads are non-atomic; the producer must
        // stay quiescent for this finite capture.
        let base = buffer.buf_ptr().cast::<ReadOnlyCell<f64>>();
        for (i, value) in values.iter_mut().enumerate() {
            let offset = (i as isize)
                .checked_mul(row_step)
                .and_then(|row| row.checked_add((col as isize).checked_mul(col_step)?))
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            // SAFETY: buffer is held; shape/strides and `col < d` were
            // validated; no suboffsets are present, so this is in the declared
            // range. The provider must remain quiescent during capture.
            let mut ordinate = unsafe { (*base.offset(offset)).get() };
            if swap {
                ordinate = buffer_endian::swap_f64_endian(ordinate);
            }
            *value = ordinate;
        }
        Ok(values)
    };

    let xs = fill_column(0).map_err(pyo3::PyErr::from)?;
    let ys = match fill_column(1) {
        Ok(values) => values,
        Err(error) => {
            drop(xs);
            return Err(pyo3::PyErr::from(error));
        },
    };
    let mut zs = match (d >= 3).then(|| fill_column(2)).transpose() {
        Ok(values) => values,
        Err(error) => {
            drop((xs, ys));
            return Err(pyo3::PyErr::from(error));
        },
    };
    let mut ms = match (d >= 4).then(|| fill_column(3)).transpose() {
        Ok(values) => values,
        Err(error) => {
            drop((xs, ys, zs));
            return Err(pyo3::PyErr::from(error));
        },
    };

    // Optional parallel z/m columns (only valid when D == 2).
    if let Some(z_col) = z {
        let values =
            float_values_exact(z_col.py(), z_col, "z", FloatInput::Coordinate, n, |got| {
                InvalidGeometryError::new_err(format!(
                    "z must have the same length as x/y coordinates (got length {got})"
                ))
            })?;
        zs = Some(values);
    }
    if let Some(m_col) = m {
        let values =
            float_values_exact(m_col.py(), m_col, "m", FloatInput::Coordinate, n, |got| {
                InvalidGeometryError::new_err(format!(
                    "m must have the same length as x/y coordinates (got length {got})"
                ))
            })?;
        ms = Some(values);
    }

    // Finite check + axes from column presence.
    Ok(Some(CoordSeq::try_from_vecs(xs, ys, zs, ms)?))
}

fn check_2d_byte_stride_corners(
    rows: usize,
    columns: usize,
    row_stride: isize,
    column_stride: isize,
) -> PyResult<()> {
    let (Some(last_row), Some(last_column)) = (rows.checked_sub(1), columns.checked_sub(1)) else {
        return Ok(());
    };
    let last_row = isize::try_from(last_row)
        .map_err(|_| pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows"))?;
    let last_column = isize::try_from(last_column)
        .map_err(|_| pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows"))?;
    let row_offset = last_row.checked_mul(row_stride).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows")
    })?;
    let column_offset = last_column.checked_mul(column_stride).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows")
    })?;
    row_offset.checked_add(column_offset).ok_or_else(|| {
        pyo3::exceptions::PyOverflowError::new_err("buffer stride index overflows")
    })?;
    Ok(())
}

/// Like [`coordinate_column_values`], but stop after `expected + 1` items when
/// the length is known from the receiver (D11: `set_coordinates`, …).
pub(crate) fn coordinate_column_values_exact(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    expected: usize,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<f64>> {
    reject_masked_array(value)?;
    if let Some(buffer) = try_typed_buffer::<f64>(value)? {
        let got = buffer_item_count(&buffer)?;
        reject_coordinate_buffer_rank(buffer.dimensions(), name)?;
        if got != expected {
            return Err(on_len(got));
        }
        return coordinate_column_values_from_buffer(py, &buffer, got);
    }
    float_values_exact(py, value, name, FloatInput::Coordinate, expected, on_len)
}

/// Optional `z`/`m` column for the columnar constructor path — the
/// [`coordinate_column_values`] sibling of [`optional_coordinates`].
pub(crate) fn optional_coordinate_column_values(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    expected_len: usize,
    name: &str,
) -> PyResult<Option<Vec<f64>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    let values = coordinate_column_values_exact(py, value, name, expected_len, |got| {
        InvalidGeometryError::new_err(format!(
            "{name} must have the same length as x/y coordinates (got length {got})"
        ))
    })?;
    Ok(Some(values))
}

/// Length-check and copy an already-admitted optional `f64` column.
pub(crate) fn optional_coordinate_column_values_from_buffer(
    py: Python<'_>,
    buffer: Option<&PyBuffer<f64>>,
    expected_len: usize,
    name: &str,
) -> PyResult<Option<Vec<f64>>> {
    let Some(buffer) = buffer else {
        return Ok(None);
    };
    let got = buffer_item_count(buffer)?;
    if got != expected_len {
        return Err(InvalidGeometryError::new_err(format!(
            "{name} must have the same length as x/y coordinates (got length {got})"
        )));
    }
    coordinate_column_values_from_buffer(py, buffer, got).map(Some)
}

pub(crate) fn float_values(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    kind: FloatInput,
) -> PyResult<Vec<f64>> {
    reject_masked_array(value)?;
    if let Some(buffer) = try_typed_buffer::<f64>(value)? {
        // Normalize non-native endian (e.g. NumPy `>f8` on little-endian) —
        // raw `to_vec` reinterprets bytes and silently corrupts.
        let item_count = buffer_item_count(&buffer)?;
        if matches!(kind, FloatInput::Coordinate) {
            reject_coordinate_buffer_rank(buffer.dimensions(), name)?;
        }
        return buffer_to_vec_f64(py, &buffer, item_count);
    }
    if matches!(kind, FloatInput::Coordinate)
        && let Ok(array) = value.cast::<PyUntypedArray>()
    {
        reject_coordinate_buffer_rank(array.ndim(), name)?;
    }
    let iter = py_iter_or_error(value, || not_a_float_iterable(name))?;
    collect_py_iter_from_iter(value, iter, |item| kind.item(name, &item))
}

/// Collect a value with a variable but finite arity. Reading at most
/// `maximum + 1` items makes malformed or infinite providers terminate while
/// retaining the caller's arity admission rules.
pub(crate) fn float_values_at_most(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    kind: FloatInput,
    maximum: usize,
    on_excess: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<f64>> {
    reject_masked_array(value)?;
    if let Some(buffer) = try_typed_buffer::<f64>(value)? {
        let got = buffer_item_count(&buffer)?;
        if matches!(kind, FloatInput::Coordinate) {
            reject_coordinate_buffer_rank(buffer.dimensions(), name)?;
        }
        if got > maximum {
            return Err(on_excess(got));
        }
        return buffer_to_vec_f64(py, &buffer, got);
    }
    if matches!(kind, FloatInput::Coordinate)
        && let Ok(array) = value.cast::<PyUntypedArray>()
    {
        reject_coordinate_buffer_rank(array.ndim(), name)?;
    }
    let iter = py_iter_or_error(value, || not_a_float_iterable(name))?;
    collect_py_iter_at_most_from_iter(
        value,
        iter,
        maximum,
        |item| kind.item(name, &item),
        on_excess,
    )
}

/// Collect exactly `expected` values (D11).
///
/// Buffer / ndarray fast paths stay zero-copy-friendly (length-checked then
/// bulk copy). Bare iterables stop after `expected + 1` items so
/// `itertools.repeat` cannot hang when a sibling column or receiver pins
/// the count.
pub(crate) fn float_values_exact(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    kind: FloatInput,
    expected: usize,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<f64>> {
    reject_masked_array(value)?;
    if let Some(buffer) = try_typed_buffer::<f64>(value)? {
        let got = buffer_item_count(&buffer)?;
        if matches!(kind, FloatInput::Coordinate) {
            reject_coordinate_buffer_rank(buffer.dimensions(), name)?;
        }
        if got != expected {
            return Err(on_len(got));
        }
        return buffer_to_vec_f64(py, &buffer, got);
    }
    if matches!(kind, FloatInput::Coordinate)
        && let Ok(array) = value.cast::<PyUntypedArray>()
    {
        reject_coordinate_buffer_rank(array.ndim(), name)?;
    }
    let iter = py_iter_or_error(value, || not_a_float_iterable(name))?;
    collect_py_iter_exact_from_iter(value, iter, expected, |item| kind.item(name, &item), on_len)
}

/// Reported sequence length when discoverable without draining a bare
/// iterable (`list`/`tuple`/`ndarray`/buffer). Scalars and bare iterators
/// (no `__len__`) return `None`.
pub(crate) fn coordinate_sequence_len_hint(
    value: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<Option<usize>> {
    reject_masked_array(value)?;
    // Do not invoke a user `__float__` provider merely to discover the pair
    // length. Numeric admission below owns that conversion and must call it
    // once. Check f64 buffers first because their item count is authoritative
    // after the rank check below.
    if let Some(buffer) = try_typed_buffer::<f64>(value)? {
        let item_count = buffer_item_count(&buffer)?;
        let dimensions = buffer.dimensions();
        reject_coordinate_buffer_rank(dimensions, name)?;
        return Ok((dimensions != 0).then_some(item_count));
    }
    // NumPy integer/object arrays do not export as f64 buffers, but their
    // reported length still pins a paired bare iterator. A 0-D array is a
    // scalar and deliberately has no sequence length.
    if let Ok(array) = value.cast::<PyUntypedArray>() {
        return if array.ndim() == 0 {
            Ok(None)
        } else {
            reject_coordinate_buffer_rank(array.ndim(), name)?;
            crate::optional_len(value)
        };
    }
    if value.cast::<PyInt>().is_ok() || value.cast::<PyFloat>().is_ok() {
        return Ok(None);
    }
    crate::optional_len(value)
}

/// Parse a finite scalar or sequence. When `expected` is `Some` and `value` is
/// a **bare** iterable (no reported length), exact collection lets a sibling
/// list/buffer pin an infinite `itertools.repeat` (D11). Sources that report
/// `__len__` retain the ordinary collector path so the final broadcast step
/// owns finite mismatch messages.
pub(crate) fn coordinate_input_with_expected(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    expected: Option<usize>,
    non_finite: &dyn Fn() -> String,
) -> PyResult<CoordinateInput> {
    if let Some(value) = coordinate_scalar_named(name, value)? {
        return Ok(CoordinateInput {
            values: vec![value],
            scalar: true,
        });
    }
    // A bare source has no length to report, so a sibling's count bounds it.
    // Sources with their own length keep the ordinary drain path, preserving
    // the shared broadcast error for finite mismatches.
    let values = if coordinate_sequence_len_hint(value, name)?.is_none()
        && let Some(expected) = expected
    {
        float_values_exact(py, value, name, FloatInput::Coordinate, expected, |got| {
            InvalidGeometryError::new_err(format!(
                "{name} must have the same length, got {got} and {expected}"
            ))
        })?
    } else {
        float_values(py, value, name, FloatInput::Coordinate)?
    };
    if column_all_finite(&values) {
        Ok(CoordinateInput {
            values,
            scalar: false,
        })
    } else {
        Err(InvalidGeometryError::new_err(non_finite()))
    }
}

pub(crate) fn parameter_input_with_expected(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    expected: Option<usize>,
    non_finite: &dyn Fn() -> String,
) -> PyResult<CoordinateInput> {
    if let Some(value) = f64_scalar_named(name, value)? {
        if value.is_finite() {
            return Ok(CoordinateInput {
                values: vec![value],
                scalar: true,
            });
        }
        return Err(GeometryError::new_err(non_finite()));
    }
    let values = if coordinate_sequence_len_hint(value, name)?.is_none()
        && let Some(expected) = expected
    {
        float_values_exact(py, value, name, FloatInput::Parameter, expected, |got| {
            GeometryError::new_err(format!(
                "{name} must have the same length, got {got} and {expected}"
            ))
        })?
    } else {
        float_values(py, value, name, FloatInput::Parameter)?
    };
    if column_all_finite(&values) {
        Ok(CoordinateInput {
            values,
            scalar: false,
        })
    } else {
        Err(GeometryError::new_err(non_finite()))
    }
}

pub(crate) fn optional_coordinate_input_with_expected(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    name: &str,
    expected: Option<usize>,
) -> PyResult<Option<CoordinateInput>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    coordinate_input_with_expected(py, value, name, expected, &|| {
        format!("{name} must be finite")
    })
    .map(Some)
}

pub(crate) fn coordinate_inputs_are_scalar(
    x: &CoordinateInput,
    y: &CoordinateInput,
    z: Option<&CoordinateInput>,
    t: Option<&CoordinateInput>,
) -> bool {
    x.scalar && y.scalar && z.is_none_or(|value| value.scalar) && t.is_none_or(|value| value.scalar)
}

/// Z/T lane state: absent, Z-only, T-only, or Z+T.
#[derive(Clone, Copy)]
#[expect(
    clippy::enum_variant_names,
    reason = "Zt::Zt mirrors the compact Z/T lane vocabulary"
)]
pub(crate) enum Zt<T> {
    None,
    Z(T),
    T(T),
    Zt { z: T, t: T },
}

pub(crate) type ZtInput = Zt<CoordinateInput>;
pub(crate) type ZtLanes<'a> = Zt<&'a mut [f64]>;
pub(crate) type ZtLaneRefs<'a> = Zt<&'a [f64]>;
pub(crate) type ZtValues = Zt<f64>;

impl ZtInput {
    fn parse_with_expected(
        py: Python<'_>,
        z: Option<&Bound<'_, PyAny>>,
        t: Option<&Bound<'_, PyAny>>,
        expected: Option<usize>,
    ) -> PyResult<Self> {
        let z = optional_coordinate_input_with_expected(py, z, "z", expected)?;
        let t = optional_coordinate_input_with_expected(py, t, "t", expected)?;
        match (z, t) {
            (None, None) => Ok(Self::None),
            (Some(z), None) => Ok(Self::Z(z)),
            (None, Some(t)) => Ok(Self::T(t)),
            (Some(z), Some(t)) => Ok(Self::Zt { z, t }),
        }
    }

    const fn scalar(&self) -> bool {
        match self {
            Self::None => true,
            Self::Z(z) => z.scalar,
            Self::T(t) => t.scalar,
            Self::Zt { z, t } => z.scalar && t.scalar,
        }
    }

    fn max_len(&self) -> Option<usize> {
        match self {
            Self::None => None,
            Self::Z(z) => Some(z.values.len()),
            Self::T(t) => Some(t.values.len()),
            Self::Zt { z, t } => Some(z.values.len().max(t.values.len())),
        }
    }

    const fn mismatch_name(&self) -> &'static str {
        match self {
            Self::None => "x and y",
            Self::Z(_) => "x, y, and z",
            Self::T(_) => "x, y, and t",
            Self::Zt { .. } => "x, y, z, and t",
        }
    }

    fn broadcast(&mut self, len: usize, mismatch_name: &str) -> PyResult<()> {
        match self {
            Self::None => Ok(()),
            Self::Z(z) => broadcast_coordinate_input(z, len, mismatch_name),
            Self::T(t) => broadcast_coordinate_input(t, len, mismatch_name),
            Self::Zt { z, t } => {
                broadcast_coordinate_input(z, len, mismatch_name)?;
                broadcast_coordinate_input(t, len, mismatch_name)
            },
        }
    }

    const fn lanes_mut(&mut self) -> ZtLanes<'_> {
        match self {
            Self::None => Zt::None,
            Self::Z(z) => Zt::Z(z.values.as_mut_slice()),
            Self::T(t) => Zt::T(t.values.as_mut_slice()),
            Self::Zt { z, t } => Zt::Zt {
                z: z.values.as_mut_slice(),
                t: t.values.as_mut_slice(),
            },
        }
    }

    const fn lanes(&self) -> ZtLaneRefs<'_> {
        match self {
            Self::None => Zt::None,
            Self::Z(z) => Zt::Z(z.values.as_slice()),
            Self::T(t) => Zt::T(t.values.as_slice()),
            Self::Zt { z, t } => Zt::Zt {
                z: z.values.as_slice(),
                t: t.values.as_slice(),
            },
        }
    }
}

/// Mutable `(x, y, z/t)` column views into a [`CrsCoordinateArgs`].
pub(crate) type CoordinateColumnsMut<'a> = (&'a mut [f64], &'a mut [f64], ZtLanes<'a>);

/// Immutable `(x, y, z/t)` column views into a [`CrsCoordinateArgs`].
pub(crate) type CoordinateColumns<'a> = (&'a [f64], &'a [f64], ZtLaneRefs<'a>);

/// Parsed, broadcast-aligned `x`/`y`/`z`/`t` columns for the raw coordinate
/// entry points (`crs_transform`, `crs_apply`, `crs_roundtrip`). Owns the
/// cross-field invariants: T-only lanes are allowed, scalar-in/scalar-out
/// detection happens before broadcasting, and every present column shares one
/// length.
pub(crate) struct CrsCoordinateArgs {
    pub x: CoordinateInput,
    pub y: CoordinateInput,
    pub zt: ZtInput,
    pub scalar: bool,
}

impl CrsCoordinateArgs {
    pub(crate) fn parse(
        py: Python<'_>,
        x: &Bound<'_, PyAny>,
        y: &Bound<'_, PyAny>,
        z: Option<&Bound<'_, PyAny>>,
        t: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        // Sibling length pin (D11): once any column reports a finite length
        // (list/buffer/ndarray), bare infinite iterators on the other columns
        // reject via `collect_py_iter_exact` instead of draining forever.
        let mut established = coordinate_sequence_len_hint(x, "x")?;
        if established.is_none() {
            established = coordinate_sequence_len_hint(y, "y")?;
        }
        if established.is_none()
            && let Some(value) = z
        {
            established = coordinate_sequence_len_hint(value, "z")?;
        }
        if established.is_none()
            && let Some(value) = t
        {
            established = coordinate_sequence_len_hint(value, "t")?;
        }
        let non_finite = |name: &'static str| -> String { format!("{name} must be finite") };
        let mut x = coordinate_input_with_expected(py, x, "x", established, &|| non_finite("x"))?;
        // After the first non-scalar column materializes, pin remaining columns.
        let established = established.or_else(|| (!x.scalar).then_some(x.values.len()));
        let mut y = coordinate_input_with_expected(py, y, "y", established, &|| non_finite("y"))?;
        let established = established.or_else(|| (!y.scalar).then_some(y.values.len()));
        let mut zt = ZtInput::parse_with_expected(py, z, t, established)?;
        let scalar = x.scalar && y.scalar && zt.scalar();
        let mut len = x.values.len().max(y.values.len());
        if let Some(zt_len) = zt.max_len() {
            len = len.max(zt_len);
        }
        let mismatch_name = zt.mismatch_name();
        broadcast_coordinate_input(&mut x, len, mismatch_name)?;
        broadcast_coordinate_input(&mut y, len, mismatch_name)?;
        zt.broadcast(len, mismatch_name)?;
        Ok(Self { x, y, zt, scalar })
    }

    /// Mutable column views for the in-place transform kernels.
    pub(crate) fn columns_mut(&mut self) -> CoordinateColumnsMut<'_> {
        (&mut self.x.values, &mut self.y.values, self.zt.lanes_mut())
    }

    /// Immutable column views for the read-only kernels.
    pub(crate) fn columns(&self) -> CoordinateColumns<'_> {
        (&self.x.values, &self.y.values, self.zt.lanes())
    }
}

pub(crate) fn broadcast_crs_coordinate_inputs(
    x: &mut CoordinateInput,
    y: &mut CoordinateInput,
    z: &mut Option<CoordinateInput>,
    t: &mut Option<CoordinateInput>,
) -> PyResult<usize> {
    if x.values.len() != y.values.len() && !x.scalar && !y.scalar {
        ensure_coordinate_len(x.values.len(), y.values.len(), "x", "y")?;
    }

    let mut len = x.values.len().max(y.values.len());
    if let Some(value) = z.as_ref() {
        len = len.max(value.values.len());
    }
    if let Some(value) = t.as_ref() {
        len = len.max(value.values.len());
    }
    let mismatch_name = if t.is_some() {
        "x, y, z, and t"
    } else if z.is_some() {
        "x, y, and z"
    } else {
        "x and y"
    };
    broadcast_coordinate_input(x, len, mismatch_name)?;
    broadcast_coordinate_input(y, len, mismatch_name)?;
    broadcast_optional_coordinate_input(z, len, mismatch_name)?;
    broadcast_optional_coordinate_input(t, len, mismatch_name)?;
    Ok(len)
}

pub(crate) fn broadcast_coordinate_group<const N: usize>(
    mut inputs: [(&mut CoordinateInput, &str); N],
    mismatch_name: &str,
) -> PyResult<usize> {
    let len = inputs
        .iter()
        .map(|(input, _)| input.values.len())
        .max()
        .unwrap_or(0);
    for (input, name) in &mut inputs {
        if input.values.len() != len && !input.scalar {
            return Err(InvalidGeometryError::new_err(format!(
                "{mismatch_name} must have the same length, got {} and {len}",
                input.values.len(),
            )));
        }
        broadcast_coordinate_input(input, len, name)?;
    }
    Ok(len)
}

pub(crate) fn broadcast_optional_coordinate_input(
    input: &mut Option<CoordinateInput>,
    len: usize,
    mismatch_name: &str,
) -> PyResult<()> {
    let Some(input) = input else {
        return Ok(());
    };
    if input.values.len() != len && !input.scalar {
        return Err(InvalidGeometryError::new_err(format!(
            "{mismatch_name} must have the same length, got {} and {len}",
            input.values.len(),
        )));
    }
    broadcast_coordinate_input(input, len, mismatch_name)
}

pub(crate) fn broadcast_coordinate_input(
    input: &mut CoordinateInput,
    len: usize,
    name: &str,
) -> PyResult<()> {
    if input.values.len() == len {
        return Ok(());
    }
    if input.scalar {
        input.values.resize(len, input.values[0]);
        return Ok(());
    }
    Err(InvalidGeometryError::new_err(format!(
        "{name} must have the same length, got {} and {len}",
        input.values.len(),
    )))
}

pub(crate) fn ensure_coordinate_len(
    left: usize,
    right: usize,
    left_name: &str,
    right_name: &str,
) -> PyResult<()> {
    if left == right {
        Ok(())
    } else if right_name.is_empty() {
        Err(InvalidGeometryError::new_err(format!(
            "{left_name} must have the same length, got {left} and {right}"
        )))
    } else {
        Err(InvalidGeometryError::new_err(format!(
            "{left_name} and {right_name} must have the same length, got {left} and {right}"
        )))
    }
}
