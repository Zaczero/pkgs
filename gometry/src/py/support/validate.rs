#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::num::NonZeroU32;

use super::*;
use crate::io::WktDimension;
use crate::py::errors::integer_parameter_error;

/// Collect a typed scalar lane from any iterable (sequence, generator, ...),
/// with one canonical error naming the lane and its element kind.
///
/// Element-by-element via the reservation keystone — never generic
/// `Vec<T>: FromPyObject`, which allocates from a lying `__len__`.
pub(crate) fn iterable_lane<'py, T>(
    value: &Bound<'py, PyAny>,
    name: &str,
    kind: &str,
) -> PyResult<Vec<T>>
where
    T: for<'a> pyo3::FromPyObject<'a, 'py>,
    for<'a> <T as pyo3::FromPyObject<'a, 'py>>::Error: Into<PyErr>,
{
    let error = || PyTypeError::new_err(format!("{name} must be an iterable of {kind}"));
    let mut out = Vec::new();
    if let Ok(hint) = value.len() {
        crate::try_reserve_hint(&mut out, hint)?;
    }
    let iter = value.try_iter().map_err(|_| error())?;
    for item in iter {
        let item = item.map_err(|_| error())?;
        let parsed: T = item.extract().map_err(|_| error())?;
        crate::try_push(&mut out, parsed)?;
    }
    Ok(out)
}

pub(crate) fn parse_precision(value: &Bound<'_, PyAny>) -> PyResult<i32> {
    py_i64_bounded("precision", value, 0..=15, &|v| {
        GeometryError::new_err(format!("precision must be between 0 and 15, got {v}"))
    })
    .map(|precision| precision as i32)
}

pub(crate) fn parse_wkt_output_dimension(
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<WktDimension>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    let dimension = py_i64_required("WKT output_dimension", value)?;
    u8::try_from(dimension)
        .ok()
        .and_then(|dimension| WktDimension::try_from(dimension).ok())
        .map(Some)
        .ok_or_else(|| {
            GeometryError::new_err(format!(
                "WKT output_dimension must be 2, 3, or 4, got {dimension}"
            ))
        })
}

pub(crate) fn validate_buffer_quadrant_segments(quadrant_segments: i64) -> PyResult<NonZeroU32> {
    if quadrant_segments <= 0 {
        return Err(integer_parameter_error(
            "buffer quadrant_segments must be at least 1",
            "quadrant_segments",
            quadrant_segments,
        ));
    }
    let n = u32::try_from(quadrant_segments).map_err(|_| {
        integer_parameter_error(
            "buffer quadrant_segments is too large",
            "quadrant_segments",
            quadrant_segments,
        )
    })?;
    NonZeroU32::new(n).ok_or_else(|| {
        integer_parameter_error(
            "buffer quadrant_segments must be at least 1",
            "quadrant_segments",
            quadrant_segments,
        )
    })
}

pub(crate) fn validate_buffer_miter_limit(miter_limit: f64) -> PyResult<Positive> {
    if !miter_limit.is_finite() {
        return Err(GeometryError::new_err("buffer miter_limit must be finite"));
    }
    Ok(Positive::try_new("miter_limit", miter_limit)?)
}

pub(crate) fn validate_nearest_k(k: i64) -> PyResult<usize> {
    non_negative_int("nearest", "k", k)?;
    usize::try_from(k).map_err(|_| GeometryError::new_err("nearest k is too large"))
}

pub(crate) fn parse_spatial_index_handle(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if value.cast_exact::<PyBool>().is_ok() || value.cast::<PyInt>().is_err() {
        // Wrong type -> plain `TypeError`, like every non-int-where-int lane.
        return Err(PyTypeError::new_err(
            "spatial index handle must be an integer",
        ));
    }
    let handle = value
        .extract::<i64>()
        .map_err(|_| GeometryError::new_err("spatial index handle is too large"))?;
    if handle < 0 {
        return Err(GeometryError::new_err(
            "spatial index handle must be non-negative",
        ));
    }
    usize::try_from(handle).map_err(|_| GeometryError::new_err("spatial index handle is too large"))
}

pub(crate) fn validate_smooth_iterations(value: i64) -> PyResult<i32> {
    check_i32_min("iterations", value, 0)
}

/// Check that `value` fits `[min, i32::MAX]`. `min` is 0 for non-negative or 1
/// for positive parameters; the error message reflects which.
pub(crate) fn check_i32_min(name: &str, value: i64, min: i64) -> PyResult<i32> {
    if (min..=i64::from(i32::MAX)).contains(&value) {
        return Ok(value as i32);
    }
    let kind = if min > 0 { "positive" } else { "non-negative" };
    Err(GeometryError::new_err(format!(
        "{name} must be a {kind} integer"
    )))
}

/// Validate an ``equals_exact`` tolerance: `NaN`, infinity, and negative values
/// would silently change the comparison semantics (`inf` equates everything,
/// `NaN`/negatives equate nothing), so they fail fast instead.
pub(crate) fn validate_equals_exact_tolerance(tolerance: f64) -> PyResult<NonNegative> {
    Ok(NonNegative::try_new("tolerance", tolerance)?)
}

/// Attach the failing row to an array-operation error as a note (PEP 678):
/// the class and message stay intact — `except ParseError` still catches —
/// while the traceback gains "while processing array element {row}". Best
/// effort: a note that cannot attach never masks the original error.
pub(crate) fn note_array_row(err: PyErr, row: usize) -> PyErr {
    Python::attach(|py| {
        let _ = err.value(py).call_method1(
            "add_note",
            (format!("while processing array element {row}"),),
        );
    });
    err
}

/// Resolve a space-filling-curve frame: explicit `(minx, miny, maxx, maxy)`
/// bounds, else the shape's own bounds (an empty shape has none).
pub(crate) fn curve_frame_for(
    shape: &Shape,
    level: crate::curves::CurveLevel,
    bounds: Option<&Bound<'_, PyAny>>,
    operation: &str,
) -> PyResult<crate::curves::CurveFrame> {
    let bounds = match parse_curve_bounds(bounds)? {
        Some(bounds) => bounds,
        None => shape.bounds().ok_or_else(|| empty_curve_input(operation))?,
    };
    Ok(crate::curves::CurveFrame::new(bounds, level))
}

/// Shared scalar spatial-key kernel: validate level, resolve the curve frame,
/// and emit the curve key. Array keys reuse the same [`CurveKind::key`] over a
/// single total-bounds frame so scalar and array keys stay bit-identical for
/// the same geometry under the same explicit bounds.
pub(crate) fn spatial_key_for_shape(
    shape: &Shape,
    kind: crate::curves::CurveKind,
    level: i64,
    bounds: Option<&Bound<'_, PyAny>>,
) -> PyResult<u64> {
    let level = crate::boundary::input::validate_curve_level(level)?;
    let operation = kind.operation_name();
    let frame = curve_frame_for(shape, level, bounds, operation)?;
    kind.key(&frame, shape)
        .ok_or_else(|| empty_curve_input(operation))
}

/// Parse optional explicit curve-frame bounds: four finite ordered floats.
pub(crate) fn parse_curve_bounds(
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<crate::geometry::Bounds>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let values = coordinate_values(value.py(), value, "bounds")?;
    let [minx, miny, maxx, maxy] = values.as_slice() else {
        return Err(GeometryError::new_err(format!(
            "bounds must be (minx, miny, maxx, maxy), got {} values",
            values.len(),
        )));
    };
    if !(minx.is_finite() && miny.is_finite() && maxx.is_finite() && maxy.is_finite())
        || minx > maxx
        || miny > maxy
    {
        return Err(GeometryError::new_err(
            "bounds must be finite (minx, miny, maxx, maxy) with min <= max",
        ));
    }
    Ok(Some(crate::geometry::Bounds::new_unchecked(
        *minx, *miny, *maxx, *maxy,
    )))
}

pub(crate) fn empty_curve_input(operation: &str) -> PyErr {
    crate::py::errors::InvalidGeometryError::new_err(format!(
        "{operation} requires a non-empty geometry"
    ))
}

pub(crate) fn validate_distance_arg(value: &Bound<'_, PyAny>) -> PyResult<NonNegative> {
    validate_distance(finite_f64_required("distance", value)?)
}

pub(crate) fn validate_distance(distance: f64) -> PyResult<NonNegative> {
    Ok(NonNegative::try_new("distance", distance)?)
}

pub(crate) fn validate_max_segment_length(max_segment_length: f64) -> PyResult<Positive> {
    Ok(Positive::try_new("max_length", max_segment_length)?)
}

pub(crate) fn validate_densify_fraction(fraction: f64) -> PyResult<f64> {
    if fraction.is_finite() && fraction > 0.0 && fraction <= 1.0 {
        return Ok(fraction);
    }
    Err(GeometryErrorKind::parameter(
        "fraction",
        fraction,
        format!("fraction must be in (0, 1], got {fraction}"),
    )
    .into())
}
