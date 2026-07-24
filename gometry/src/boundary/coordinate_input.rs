#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Coordinate-vertex argument parsing and broadcasting for the column-form
//! constructors (`line_string(x=, y=, …)`, `multi_point(...)`, etc.).
//!
//! `CoordinateInput` holds parsed `x`/`y`/`z`/`m` columns; the helpers coerce
//! Python sequences/scalars, broadcast scalar lanes to a common length, and
//! validate equal column lengths. Re-exported at the crate root via `use
//! super::*`.

use std::sync::Arc;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyInt};
use pyo3::{Borrowed, FromPyObject};

use crate::boundary::buffer_endian::{buffer_to_arc_f64, buffer_to_vec_f64};
use crate::geometry::column_all_finite;
use crate::py::errors::InvalidGeometryError;
use crate::*;

pub(crate) struct CoordinateInput {
    pub values: Vec<f64>,
    pub scalar: bool,
}

pub(crate) fn coordinate_input(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<CoordinateInput> {
    coordinate_input_with_error(py, value, name, &|| format!("{name} must be finite"))
}

/// [`coordinate_input`] with a caller-supplied non-finite message (the
/// geodesic surfaces phrase the domain differently).
pub(crate) fn coordinate_input_with_error(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    non_finite: &dyn Fn() -> String,
) -> PyResult<CoordinateInput> {
    if let Ok(value) = value.extract::<f64>() {
        if value.is_finite() {
            return Ok(CoordinateInput {
                values: vec![value],
                scalar: true,
            });
        }
        return Err(InvalidGeometryError::new_err(non_finite()));
    }
    let values = coordinate_values(py, value, name)?;
    if column_all_finite(&values) {
        Ok(CoordinateInput {
            values,
            scalar: false,
        })
    } else {
        Err(InvalidGeometryError::new_err(non_finite()))
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

    /// Parse like [`parse`](Self::parse), but leave finiteness validation to
    /// the operation kernel. Use only when a lower layer owns a structured
    /// non-finite error.
    pub(crate) fn parse_raw(value: &Bound<'_, PyAny>, name: &str, len: usize) -> PyResult<Self> {
        if let Ok(value) = value.extract::<f64>() {
            return Ok(Self::Scalar(value));
        }
        // Known receiver length: stop after `len + 1` items so
        // `itertools.repeat` cannot hang (D11).
        let values = coordinate_values_exact(value.py(), value, name, len, |got| {
            crate::py::errors::GeometryError::new_err(format!(
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
        if let Ok(value) = value.extract::<f64>() {
            if value.is_finite() {
                return Ok(Self::Scalar(value));
            }
            return Err(crate::py::errors::float_parameter_error(
                non_finite(),
                name,
                value,
            ));
        }
        let values = coordinate_values_exact(value.py(), value, name, len, |got| {
            crate::py::errors::GeometryError::new_err(format!(
                "{name} must be a float or a length-{len} sequence (one per geometry); \
                 got length {got}"
            ))
        })?;
        if let Some(&value) = values.iter().find(|value| !value.is_finite()) {
            Err(crate::py::errors::float_parameter_error(
                non_finite(),
                name,
                value,
            ))
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
                crate::numeric::Finite::try_new(name, *value)?;
            },
            Self::PerElement(values) => {
                for &value in values {
                    crate::numeric::Finite::try_new(name, value)?;
                }
            },
        }
        Ok(())
    }

    /// The value for row `row`. Trusts `row < len` (the caller iterates the
    /// array it was parsed against), so `PerElement` indexes
    /// unchecked-in-spirit via the validated slice.
    #[must_use]
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

    /// The single value when scalar — lets a method keep its columnar/packed
    /// fast path for the common broadcast case, falling to the per-row lane
    /// only for a genuine per-element array.
    #[must_use]
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
        if value.cast::<PyInt>().is_ok() {
            let scalar = value.extract::<i64>().map_err(|_| {
                crate::py::errors::GeometryError::new_err(format!("{name} is too large"))
            })?;
            return Ok(Self::Scalar(scalar));
        }
        // Known receiver length: bound the drain so `itertools.repeat` cannot
        // hang (D11).
        let values = integer_values_exact(value, name, len, |got| {
            crate::py::errors::GeometryError::new_err(format!(
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
    item.extract::<i64>()
        .map_err(|_| crate::py::errors::GeometryError::new_err(format!("{name} is too large")))
}

/// Collect exactly `expected` integers; reject infinite/mismatched streams
/// without draining them (D11).
pub(crate) fn integer_values_exact(
    value: &Bound<'_, PyAny>,
    name: &str,
    expected: usize,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<i64>> {
    let type_err = || {
        PyTypeError::new_err(format!(
            "{name} must be an integer or an iterable of integers"
        ))
    };
    match collect_py_iter_exact(
        value,
        expected,
        |item| map_integer_item(&item, name),
        on_len,
    ) {
        Ok(values) => Ok(values),
        // Length mismatch (`on_len`), OOM, and domain GeometryError pass through.
        Err(err)
            if err.is_instance_of::<pyo3::exceptions::PyMemoryError>(value.py())
                || err.is_instance_of::<crate::py::errors::GeometryError>(value.py())
                || err.is_instance_of::<PyTypeError>(value.py()) =>
        {
            Err(err)
        },
        Err(_) => Err(type_err()),
    }
}

/// Copy a C-contiguous `f64` buffer into a freshly allocated `Arc<[f64]>` —
/// one allocation, no `Vec`→`Arc` realloc. Non-native endian is bulk-byteswapped
/// into that allocation (still one alloc). Non-contiguous buffers and
/// non-buffer inputs fall back to [`coordinate_values`] + `into()`.
pub(crate) fn coordinate_arc_values(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<Arc<[f64]>> {
    if let Ok(buffer) = PyBuffer::<f64>::get(value) {
        if buffer.is_c_contiguous() {
            return Ok(buffer_to_arc_f64(&buffer));
        }
        return buffer_to_vec_f64(py, &buffer).map(Into::into);
    }
    coordinate_values(py, value, name).map(Into::into)
}

/// Like [`coordinate_arc_values`], but stop after `expected + 1` items when
/// the length is known from the receiver (D11: `set_coordinates`, …).
pub(crate) fn coordinate_arc_values_exact(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    expected: usize,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Arc<[f64]>> {
    if let Ok(buffer) = PyBuffer::<f64>::get(value) {
        let got = buffer.item_count();
        if got != expected {
            return Err(on_len(got));
        }
        if buffer.is_c_contiguous() {
            return Ok(buffer_to_arc_f64(&buffer));
        }
        return buffer_to_vec_f64(py, &buffer).map(Into::into);
    }
    coordinate_values_exact(py, value, name, expected, on_len).map(Into::into)
}

/// Optional `z`/`m` column for the columnar constructor path — the
/// [`coordinate_arc_values`] sibling of [`optional_coordinates`].
pub(crate) fn optional_coordinate_arc_values(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    expected_len: usize,
    name: &str,
) -> PyResult<Option<Arc<[f64]>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    let values = coordinate_arc_values_exact(py, value, name, expected_len, |got| {
        InvalidGeometryError::new_err(format!(
            "{name} must have the same length as x/y coordinates (got length {got})"
        ))
    })?;
    Ok(Some(values))
}

pub(crate) fn coordinate_values(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<Vec<f64>> {
    if let Ok(buffer) = PyBuffer::<f64>::get(value) {
        // Normalize non-native endian (e.g. NumPy `>f8` on little-endian) —
        // raw `to_vec` reinterprets bytes and silently corrupts.
        return buffer_to_vec_f64(py, &buffer);
    }
    // Fallible element-by-element collect via the reservation keystone.
    // Never `extract::<Vec<f64>>()` — that path allocates from a lying
    // `__len__` before any length check and panics on capacity overflow.
    // Unknown cardinality: `collect_py_iter` / try_push → MemoryError on
    // unbounded growth, never hang-then-abort (D11 part 1).
    match collect_py_iter(value, |item| {
        item.extract::<f64>().map_err(|_| {
            PyTypeError::new_err(format!(
                "{name} must be a float or an iterable of finite floats"
            ))
        })
    }) {
        Ok(values) => Ok(values),
        Err(err)
            if err.is_instance_of::<pyo3::exceptions::PyMemoryError>(py)
                || err.is_instance_of::<pyo3::exceptions::PyValueError>(py) =>
        {
            Err(err)
        },
        // Rewrite bare try_iter TypeError ("'int' object is not iterable")
        // into the domain message that names the parameter.
        Err(_) => Err(PyTypeError::new_err(format!(
            "{name} must be a float or an iterable of finite floats"
        ))),
    }
}

/// Collect exactly `expected` finite-f64 coordinate values (D11).
///
/// Buffer / ndarray fast paths stay zero-copy-friendly (length-checked then
/// bulk copy). Bare iterables stop after `expected + 1` items so
/// `itertools.repeat` cannot hang when a sibling column or receiver pins
/// the count.
pub(crate) fn coordinate_values_exact(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    expected: usize,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<f64>> {
    if let Ok(buffer) = PyBuffer::<f64>::get(value) {
        let got = buffer.item_count();
        if got != expected {
            return Err(on_len(got));
        }
        return buffer_to_vec_f64(py, &buffer);
    }
    let type_err = || {
        PyTypeError::new_err(format!(
            "{name} must be a float or an iterable of finite floats"
        ))
    };
    match collect_py_iter_exact(
        value,
        expected,
        |item| item.extract::<f64>().map_err(|_| type_err()),
        on_len,
    ) {
        Ok(values) => Ok(values),
        // Length mismatch (`on_len`), OOM, and domain geometry errors pass through.
        Err(err)
            if err.is_instance_of::<pyo3::exceptions::PyMemoryError>(py)
                || err.is_instance_of::<crate::py::errors::GeometryError>(py)
                || err.is_instance_of::<InvalidGeometryError>(py) =>
        {
            Err(err)
        },
        // Bare try_iter TypeError → parameter-named domain message.
        Err(_) => Err(type_err()),
    }
}

/// Reported sequence length when discoverable without draining a bare
/// iterable (`list`/`tuple`/`ndarray`/buffer). Scalars and bare iterators
/// (no `__len__`) return `None`.
pub(crate) fn coordinate_sequence_len_hint(value: &Bound<'_, PyAny>) -> Option<usize> {
    // Python/NumPy scalars extract as f64 and are broadcast — not a column length.
    if value.extract::<f64>().is_ok() {
        // A multi-element f64 buffer does not extract as a single f64, so this
        // arm is the true scalar case (or a 0-d array treated as scalar).
        return None;
    }
    if let Ok(buffer) = PyBuffer::<f64>::get(value) {
        return Some(buffer.item_count());
    }
    value.len().ok()
}

/// Like [`coordinate_input_with_error`], but when `expected` is `Some` and
/// `value` is a **bare** iterable (no reported length), the non-scalar path
/// uses exact collection so a sibling list/buffer can pin an infinite
/// `itertools.repeat` (D11). Values that already report `__len__`/buffer
/// size drain normally so finite length-mismatch messages stay broadcast's.
pub(crate) fn coordinate_input_with_expected(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    expected: Option<usize>,
    non_finite: &dyn Fn() -> String,
) -> PyResult<CoordinateInput> {
    if let Ok(value) = value.extract::<f64>() {
        if value.is_finite() {
            return Ok(CoordinateInput {
                values: vec![value],
                scalar: true,
            });
        }
        return Err(InvalidGeometryError::new_err(non_finite()));
    }
    // Bare iterable (no `__len__` / buffer): bound by sibling when known.
    // Length-reporting columns keep the drain-then-broadcast path so
    // `points([1,2],[1,2,3])` still raises the historical message.
    let values = if coordinate_sequence_len_hint(value).is_none()
        && let Some(expected) = expected
    {
        coordinate_values_exact(py, value, name, expected, |got| {
            InvalidGeometryError::new_err(format!(
                "{name} must have the same length, got {got} and {expected}"
            ))
        })?
    } else {
        coordinate_values(py, value, name)?
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
/// cross-field invariants: `t` requires `z`, scalar-in/scalar-out detection
/// happens before broadcasting, and every present column shares one length.
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
        let established = coordinate_sequence_len_hint(x)
            .or_else(|| coordinate_sequence_len_hint(y))
            .or_else(|| z.and_then(coordinate_sequence_len_hint))
            .or_else(|| t.and_then(coordinate_sequence_len_hint));
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
