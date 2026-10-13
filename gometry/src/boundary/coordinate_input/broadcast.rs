use super::*;

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

pub(crate) type ZtInput = Zt<CoordinateInput>;

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
        let value = *input
            .values
            .first()
            .expect("scalar coordinate input has one admitted value");
        input.values.resize(len, value);
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
