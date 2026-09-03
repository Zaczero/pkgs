use pyo3::PyResult;

use crate::py::arrow::geoarrow_parse_error;
use crate::py::errors::{GeometryError, InvalidGeometryError};

fn ensure_offsets_len(total: usize, window: usize, count: usize) -> PyResult<()> {
    if count
        .checked_add(1)
        .and_then(|slots| window.checked_add(slots))
        .is_some_and(|end| end <= total)
    {
        return Ok(());
    }
    Err(geoarrow_parse_error(
        "Arrow offsets buffer is shorter than declared array length",
    ))
}

pub(crate) fn ensure_i32_offsets_monotonic(
    offsets: &[i32],
    window: usize,
    count: usize,
    child_len: usize,
) -> PyResult<()> {
    ensure_offsets_len(offsets.len(), window, count)?;
    let mut previous = usize::try_from(offsets[window])
        .map_err(|_| geoarrow_parse_error("Arrow offsets must be non-negative"))?;
    let mut unordered = false;
    for index in 1..=count {
        let next = usize::try_from(offsets[window + index])
            .map_err(|_| geoarrow_parse_error("Arrow offsets must be non-negative"))?;
        unordered |= previous > next;
        previous = next;
    }
    if unordered {
        return Err(geoarrow_parse_error("Arrow offsets must be ordered"));
    }
    if previous > child_len {
        return Err(geoarrow_parse_error(format!(
            "Arrow offset terminal ({previous}) exceeds child length ({child_len})"
        )));
    }
    Ok(())
}

pub(crate) fn push_i32_le(values: &mut Vec<u8>, value: usize) -> PyResult<()> {
    let value = i32::try_from(value).map_err(|_| {
        GeometryError::new_err(
            "Arrow output exceeds the i32 offset capacity for binary/list arrays",
        )
    })?;
    crate::try_reserve_vec(values, 4).map_err(pyo3::PyErr::from)?;
    values.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

pub(crate) fn mixed_axes_error() -> pyo3::PyErr {
    InvalidGeometryError::new_err("mixed Z/M coordinate axes")
}
