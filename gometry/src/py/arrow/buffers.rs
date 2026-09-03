use pyo3::PyResult;

use crate::py::arrow::geoarrow_parse_error;

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
    for index in 1..=count {
        let next = usize::try_from(offsets[window + index])
            .map_err(|_| geoarrow_parse_error("Arrow offsets must be non-negative"))?;
        if previous > next {
            return Err(geoarrow_parse_error("Arrow offsets must be ordered"));
        }
        previous = next;
    }
    if previous > child_len {
        return Err(geoarrow_parse_error(format!(
            "Arrow offset terminal ({previous}) exceeds child length ({child_len})"
        )));
    }
    Ok(())
}
