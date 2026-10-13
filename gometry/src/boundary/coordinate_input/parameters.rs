use super::*;

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

pub(super) fn reject_coordinate_buffer_rank(dimensions: usize, name: &str) -> PyResult<()> {
    if dimensions > 1 {
        Err(PyTypeError::new_err(format!(
            "{name} must be a scalar or a one-dimensional sequence; got a {dimensions}-dimensional array"
        )))
    } else {
        Ok(())
    }
}

pub(super) fn coordinate_scalar_named(
    name: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<Option<f64>> {
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

    /// Value for a row from the receiver whose exact length admitted this lane.
    pub(crate) fn get(&self, row: usize) -> f64 {
        match self {
            Self::Scalar(value) => *value,
            Self::PerElement(values) => *values
                .get(row)
                .expect("per-element parameters match the admitted receiver length"),
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
            Self::PerElement(values) => *values
                .get(row)
                .expect("per-element parameters match the admitted receiver length"),
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
