#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::num::NonZeroU32;

use pyo3::types::PyString;

use crate::boundary::FloatInput;
use crate::io::WktDimension;
use crate::py::errors::integer_parameter_error;
use crate::py::support::{
    Bound, DwithinDistance, GeometryError, GeometryErrorKind, NonNegative, Positive, PyAny,
    PyAnyMethods as _, PyErr, PyResult, Python, finite_f64_required, non_negative_int,
    py_i64_bounded, py_i64_required,
};

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
    // Wrong type -> plain `TypeError`, like every non-int-where-int lane.
    let handle = crate::py_i64_required("spatial index handle", value)?;
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
        const PREFIX: &[u8] = b"while processing array element ";
        // Three decimal digits per byte bound every supported usize width.
        const MAX_DIGITS: usize = 3 * std::mem::size_of::<usize>();
        const CAPACITY: usize = PREFIX.len() + MAX_DIGITS;

        let method = match PyString::from_bytes(py, b"add_note") {
            Ok(method) => method,
            Err(secondary) => {
                drop(secondary);
                return;
            },
        };
        let exception = err.value(py);
        let mut bytes = [0_u8; CAPACITY];
        bytes[..PREFIX.len()].copy_from_slice(PREFIX);
        let mut digits = [0_u8; MAX_DIGITS];
        let mut end = digits.len();
        if row == 0 {
            end -= 1;
            digits[end] = b'0';
        } else {
            let mut value = row;
            while value != 0 {
                end -= 1;
                digits[end] = b'0' + (value % 10) as u8;
                value /= 10;
            }
        }
        let digits = &digits[end..];
        let start = PREFIX.len();
        bytes[start..start + digits.len()].copy_from_slice(digits);
        let note = match PyString::from_bytes(py, &bytes[..start + digits.len()]) {
            Ok(note) => note,
            Err(secondary) => {
                drop(secondary);
                return;
            },
        };
        drop(exception.call_method1(method, (note,)));
    });
    err
}

/// Parse optional explicit curve-frame bounds: four finite ordered floats.
pub(crate) fn parse_curve_bounds(
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<crate::geometry::Bounds>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let values = crate::float_values_exact(
        value.py(),
        value,
        "bounds",
        FloatInput::Parameter,
        4,
        |got| {
            GeometryError::new_err(format!(
                "bounds must be finite (minx, miny, maxx, maxy) with min <= max; got {got} values"
            ))
        },
    )?;
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
    Ok(Some(crate::geometry::Bounds::new_raw(
        *minx, *miny, *maxx, *maxy,
    )))
}

pub(crate) fn validate_distance_arg(value: &Bound<'_, PyAny>) -> PyResult<NonNegative> {
    validate_distance(finite_f64_required("distance", value)?)
}

pub(crate) fn validate_dwithin_distance_arg(value: &Bound<'_, PyAny>) -> PyResult<DwithinDistance> {
    validate_dwithin_distance(crate::f64_required("distance", value)?)
}

pub(crate) fn validate_dwithin_distance(distance: f64) -> PyResult<DwithinDistance> {
    Ok(DwithinDistance::try_new("distance", distance)?)
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

#[cfg(test)]
mod tests {
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;

    use super::note_array_row;

    #[test]
    fn row_note_formats_zero_and_maximum_rows() {
        Python::initialize();
        Python::attach(|py| {
            for row in [0, usize::MAX] {
                let error = note_array_row(PyValueError::new_err("row failure"), row);
                let notes = error
                    .value(py)
                    .getattr("__notes__")
                    .expect("row note list")
                    .extract::<Vec<String>>()
                    .expect("row notes are strings");
                assert_eq!(notes, vec![format!("while processing array element {row}")]);
                drop(error);
            }
        });
    }

    #[test]
    fn failed_add_note_returns_the_original_exception() {
        Python::initialize();
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                c"class RejectNote(ValueError):\n    def add_note(self, note):\n        raise RuntimeError('note rejected')\n",
                c"<row-note-test>",
                c"row_note_test",
            )
            .expect("row-note test module");
            let value = module
                .getattr("RejectNote")
                .expect("row-note exception class")
                .call1(("original",))
                .expect("row-note exception instance");
            let original = value.as_ptr();
            let error = note_array_row(PyErr::from_value(value), 3);

            assert_eq!(error.value(py).as_ptr(), original);
            assert!(error.is_instance_of::<PyValueError>(py));
            assert_eq!(
                error
                    .value(py)
                    .str()
                    .expect("exception str")
                    .to_str()
                    .unwrap(),
                "original"
            );
            drop(error);
        });
    }
}
