#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use pyo3::buffer::{PyBuffer, ReadOnlyCell};

use crate::py::arrow::*;

/// When `null_count` is known (non-negative), it must equal the number of null
/// rows in the visible validity window. Unknown (`-1`) is not checked.
pub(crate) fn ensure_null_count_matches_mask(
    null_count: i64,
    missing: impl IntoIterator<Item = bool>,
) -> PyResult<()> {
    if null_count < 0 {
        return Ok(());
    }
    let expected = usize::try_from(null_count)
        .map_err(|_| geoarrow_parse_error("Arrow null_count is negative or too large"))?;
    let actual = missing.into_iter().filter(|&is_missing| is_missing).count();
    if actual != expected {
        return Err(geoarrow_parse_error(format!(
            "Arrow null_count ({expected}) does not match validity bitmap ({actual} null rows)"
        )));
    }
    Ok(())
}

pub(crate) fn arrow_validity(py: Python<'_>, array: &Bound<'_, PyAny>) -> PyResult<ArrowValidity> {
    // Empty arrays never need a validity bitmap (there are no rows to probe),
    // even when `null_count == -1` (unknown). Only a non-empty array with a
    // non-zero null count consults the bitmap.
    let length = array.len()?;
    if length == 0 {
        return Ok(ArrowValidity {
            bitmap: None,
            offset: 0,
        });
    }
    // Native struct-child views may carry an ancestor-OR missing mask that is
    // not present on the raw child validity bitmap. Prefer that combined mask
    // (logical window, offset 0) so parent nulls cannot resurrect geometry.
    if let Some(missing) = crate::py::arrow_c::native_arrow_effective_missing(array) {
        return Ok(arrow_validity_from_missing_mask(&missing));
    }
    let null_count = array.getattr("null_count")?.extract::<i64>()?;
    if null_count == 0 {
        return Ok(ArrowValidity {
            bitmap: None,
            offset: 0,
        });
    }
    let validity = ArrowValidity {
        bitmap: Some(required_arrow_buffer(py, array, 0, "validity bitmap")?),
        offset: arrow_array_offset(array)?,
    };
    // Known-positive null_count must match the visible bitmap (P02).
    if null_count > 0 {
        let mask = (0..length).map(|index| !validity.is_valid(index));
        ensure_null_count_matches_mask(null_count, mask)?;
    }
    Ok(validity)
}

/// Build an `ArrowValidity` over a logical missing mask (`true` = null), with
/// bitmap offset 0 (the mask already covers only the visible window).
pub(crate) fn arrow_validity_from_missing_mask(missing: &[bool]) -> ArrowValidity {
    if !missing.iter().any(|&is_missing| is_missing) {
        return ArrowValidity {
            bitmap: None,
            offset: 0,
        };
    }
    ArrowValidity {
        bitmap: Some(validity_bitmap_from_missing(missing)),
        offset: 0,
    }
}

/// Record a geometry-level null as a missing row (placeholder shape + mask slot).
pub(crate) fn push_geometry_level_missing(
    geometries: &mut Vec<crate::PyGeometry>,
    missing_rows: &mut Vec<usize>,
    row: usize,
    crs: Option<crate::Crs>,
) {
    missing_rows.push(row);
    geometries.push(crate::PyGeometry::from_shape_crs(
        crate::PyGeometryArray::missing_placeholder(),
        crs,
    ));
}

impl ArrowValidity {
    pub(crate) fn is_valid(&self, index: usize) -> bool {
        let Some(bitmap) = &self.bitmap else {
            return true;
        };
        // A wrapped bit position must read as null, never as some other bit.
        let Some(bit) = self.offset.checked_add(index) else {
            return false;
        };
        bitmap
            .get(bit / 8)
            .is_some_and(|byte| (byte & (1 << (bit % 8))) != 0)
    }

    pub(crate) fn first_invalid(&self, len: usize) -> Option<usize> {
        let Some(bitmap) = &self.bitmap else {
            return None;
        };
        let start = self.offset;
        let Some(end) = start.checked_add(len) else {
            return Some(0);
        };
        if len == 0 {
            return None;
        }
        let first_byte = start / 8;
        let last_byte = (end - 1) / 8;
        for byte_index in first_byte..=last_byte {
            let low = if byte_index == first_byte {
                start % 8
            } else {
                0
            };
            let high = if byte_index == last_byte {
                ((end - 1) % 8) + 1
            } else {
                8
            };
            let mask = (((1_u16 << high) - 1) ^ ((1_u16 << low) - 1)) as u8;
            let byte = bitmap.get(byte_index).copied().unwrap_or(0);
            if byte & mask != mask {
                let invalid_bit = ((!byte) & mask).trailing_zeros() as usize;
                return Some(byte_index * 8 + invalid_bit - start);
            }
        }
        None
    }
}

pub(crate) fn arrow_null_error(row: usize) -> PyErr {
    geoarrow_parse_error(format!(
        "Arrow geometry arrays require non-null geometry values; found null at index {row}"
    ))
}

/// GeoArrow permits nulls only at the outer geometry level. Reject a null in a
/// nested list/ring/polygon slot that a parent geometry actually references.
pub(crate) fn reject_inner_nulls_in_range(
    validity: &ArrowValidity,
    start: usize,
    count: usize,
) -> PyResult<()> {
    for index in start..start.saturating_add(count) {
        if !validity.is_valid(index) {
            return Err(geoarrow_parse_error(format!(
                "Arrow GeoArrow nested geometry values must not contain nulls; found null at index {index}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn arrow_array_offset(array: &Bound<'_, PyAny>) -> PyResult<usize> {
    array.getattr("offset")?.extract()
}

pub(crate) fn ensure_arrow_range(
    total: usize,
    offset: usize,
    len: usize,
    field: &str,
) -> PyResult<()> {
    if offset.checked_add(len).is_some_and(|end| end <= total) {
        return Ok(());
    }
    Err(geoarrow_parse_error(format!(
        "Arrow {field} buffer is shorter than declared array length"
    )))
}

pub(crate) fn ensure_arrow_offsets_len(total: usize, offset: usize, len: usize) -> PyResult<()> {
    if len
        .checked_add(1)
        .and_then(|len| offset.checked_add(len))
        .is_some_and(|end| end <= total)
    {
        return Ok(());
    }
    Err(geoarrow_parse_error(
        "Arrow offsets buffer is shorter than declared array length",
    ))
}

/// Arrow offset chains must be non-decreasing across the full visible window,
/// **including null slots** (m01). Hidden null payload is ignored at decode,
/// but the structural offset invariant still applies — matching
/// ``array.validate(full=True)``.
///
/// `count == 0` still validates the single start/terminal slot at `window`
/// (D18): a length-0 binary/list array retains one offset that must be
/// non-negative. A negative start (e.g. `-1`) is malformed.
///
/// `child_len` is the child array length (list) or data-buffer byte length
/// (binary). The terminal offset over the visible window must be
/// `<= child_len` (N2) — a terminal past the child is OOB-adjacent and matches
/// PyArrow's "terminal N > child length M" full-validate rejection.
pub(crate) fn ensure_i32_offsets_monotonic(
    offsets: &[i32],
    window: usize,
    count: usize,
    child_len: usize,
) -> PyResult<()> {
    ensure_arrow_offsets_len(offsets.len(), window, count)?;
    let mut terminal = i32_offset_to_usize(offsets[window])?;
    let mut unordered = false;
    for index in 1..=count {
        let next = i32_offset_to_usize(offsets[window + index])?;
        unordered |= terminal > next;
        terminal = next;
    }
    if unordered {
        return Err(geoarrow_parse_error("Arrow offsets must be ordered"));
    }
    // Every visible slot was converted before reporting any ordering failure,
    // preserving the non-negative error precedence of the former first pass.
    ensure_offset_terminal_within_child(terminal, child_len)
}

/// i64 (LargeBinary) sibling of [`ensure_i32_offsets_monotonic`].
pub(crate) fn ensure_i64_offsets_monotonic(
    offsets: &[i64],
    window: usize,
    count: usize,
    child_len: usize,
) -> PyResult<()> {
    ensure_arrow_offsets_len(offsets.len(), window, count)?;
    let mut terminal = i64_offset_to_usize(offsets[window])?;
    let mut unordered = false;
    for index in 1..=count {
        let next = i64_offset_to_usize(offsets[window + index])?;
        unordered |= terminal > next;
        terminal = next;
    }
    if unordered {
        return Err(geoarrow_parse_error("Arrow offsets must be ordered"));
    }
    ensure_offset_terminal_within_child(terminal, child_len)
}

/// Reject a terminal offset that indexes past the child array / data buffer.
#[inline]
pub(crate) fn ensure_offset_terminal_within_child(
    terminal: usize,
    child_len: usize,
) -> PyResult<()> {
    if terminal > child_len {
        return Err(geoarrow_parse_error(format!(
            "Arrow offset terminal ({terminal}) exceeds child length ({child_len})"
        )));
    }
    Ok(())
}

/// Materialize only `[byte_start, byte_end)` from a binary/list Arrow data
/// buffer (buffer index 2), avoiding a full-parent copy on sliced arrays.
pub(crate) fn arrow_binary_data_span(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    byte_start: usize,
    byte_end: usize,
) -> PyResult<Vec<u8>> {
    let span_error =
        || geoarrow_parse_error("Arrow WKB data buffer is shorter than declared offsets");
    if byte_end < byte_start {
        return Err(span_error());
    }
    // Empty visible span: short-circuit before buffer lookup. Zero-sized Arrow
    // data buffers may carry a null pointer (empty Binary/LargeBinary arrays).
    if byte_start == byte_end {
        return Ok(Vec::new());
    }
    let Some(buffer) = arrow_buffer_object(array, 2)? else {
        return Err(geoarrow_parse_error(
            "Arrow WKB data buffer is required but missing",
        ));
    };
    if let Ok(buffer) = PyBuffer::<u8>::get(&buffer)
        && buffer.is_c_contiguous()
        && buffer.item_size() == 1
    {
        if byte_end > buffer.len_bytes() {
            return Err(span_error());
        }
        // PyO3 returns a slice for every C-contiguous typed buffer; the prior
        // raw-pointer fallback was therefore unreachable after this admission.
        let slice = buffer
            .as_slice(py)
            .expect("C-contiguous PyBuffer must expose a slice");
        let span = slice.get(byte_start..byte_end).ok_or_else(span_error)?;
        return Ok(span.iter().map(ReadOnlyCell::get).collect());
    }
    let bytes = required_arrow_buffer(py, array, 2, "WKB data")?;
    bytes
        .get(byte_start..byte_end)
        .ok_or_else(span_error)
        .map(<[u8]>::to_vec)
}

pub(crate) fn required_arrow_buffer(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    index: usize,
    name: &str,
) -> PyResult<Vec<u8>> {
    arrow_buffer(py, array, index)?
        .ok_or_else(|| geoarrow_parse_error(format!("Arrow {name} buffer is required but missing")))
}

pub(crate) fn arrow_buffer(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    index: usize,
) -> PyResult<Option<Vec<u8>>> {
    let Some(buffer) = arrow_buffer_object(array, index)? else {
        return Ok(None);
    };
    let _ = py;
    if let Ok(buffer) = PyBuffer::<u8>::get(&buffer) {
        return buffer.to_vec(py).map(Some);
    }
    buffer
        .call_method0("to_pybytes")?
        .extract::<Vec<u8>>()
        .map(Some)
}

pub(crate) fn arrow_buffer_object<'py>(
    array: &Bound<'py, PyAny>,
    index: usize,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    // Prefer single-buffer access on the native capsule view so BinaryView
    // imports never rebuild the full buffer tuple per slot (O(N³) DoS).
    if crate::py::arrow_c::is_native_arrow_array(array) {
        let buffer = array.call_method1("buffer", (index,))?;
        if buffer.is_none() {
            return Ok(None);
        }
        return Ok(Some(buffer));
    }
    let buffers = array.call_method0("buffers")?;
    let buffer = buffers.get_item(index)?;
    if buffer.is_none() {
        return Ok(None);
    }
    Ok(Some(buffer))
}

/// Byte length of Binary/LargeBinary data buffer (index 2), or 0 when absent
/// (empty arrays may omit the data pointer). Used as `child_len` for WKB
/// offset terminal checks (N2).
pub(crate) fn arrow_binary_data_buffer_len(array: &Bound<'_, PyAny>) -> PyResult<usize> {
    let Some(buffer) = arrow_buffer_object(array, 2)? else {
        return Ok(0);
    };
    if let Ok(buf) = PyBuffer::<u8>::get(&buffer) {
        return Ok(buf.len_bytes());
    }
    // Fallback: size attribute (pyarrow.Buffer) or full materialize.
    if let Ok(size) = buffer.getattr("size")
        && let Ok(n) = size.extract::<usize>()
    {
        return Ok(n);
    }
    Ok(buffer
        .call_method0("to_pybytes")?
        .extract::<Vec<u8>>()?
        .len())
}

/// Decode only the `[base, base + span)` slice of a coordinate `f64` buffer
/// (the visible coordinate run of a possibly-sliced array), folding in the
/// array's own Arrow offset. Skips materializing the entire parent buffer when
/// importing a small slice of a large `GeoArrow` array.
pub(crate) fn arrow_f64_values_span(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    base: usize,
    span: usize,
) -> PyResult<Arc<[f64]>> {
    let start = arrow_array_offset(array)?
        .checked_add(base)
        .ok_or_else(|| geoarrow_parse_error("Arrow coordinate offset overflow"))?;
    arrow_f64_values_span_fast(py, array, start, span)?.map_or_else(
        || {
            arrow_buffer_values_span(py, array, 1, "values", 8, start, span, |chunk| {
                // `arrow_buffer_values_span` slices `[start*width, end*width)` — exact-width chunks.
                f64::from_le_bytes(chunk.try_into().expect("chunk has 8 bytes"))
            })
            .map(Into::into)
        },
        Ok,
    )
}

pub(crate) fn arrow_f64_values_span_fast(
    _py: Python<'_>,
    array: &Bound<'_, PyAny>,
    start: usize,
    count: usize,
) -> PyResult<Option<Arc<[f64]>>> {
    if !cfg!(target_endian = "little") {
        return Ok(None);
    }
    // Empty coordinate span: short-circuit before values-buffer lookup (empty
    // Point / list-child arrays may have a null zero-sized values pointer).
    if count == 0 {
        return Ok(Some(Arc::from([])));
    }
    let span_error =
        || geoarrow_parse_error("Arrow values buffer is shorter than the visible coordinate span");
    let byte_start = start.checked_mul(8).ok_or_else(span_error)?;
    let byte_end = start
        .checked_add(count)
        .and_then(|end| end.checked_mul(8))
        .ok_or_else(span_error)?;
    let Some(buffer) = arrow_buffer_object(array, 1)? else {
        return Err(geoarrow_parse_error(
            "Arrow values buffer is required but missing",
        ));
    };
    let Ok(buffer) = PyBuffer::<u8>::get(&buffer) else {
        return Ok(None);
    };
    if !buffer.is_c_contiguous() || buffer.item_size() != 1 || byte_end > buffer.len_bytes() {
        return if byte_end > buffer.len_bytes() {
            Err(span_error())
        } else {
            Ok(None)
        };
    }
    let ptr = buffer.buf_ptr().cast::<u8>();
    // SAFETY: the PyBuffer is held for this scope, the buffer is C-contiguous
    // bytes, and the range was bounds-checked against `len_bytes`.
    let bytes = unsafe { std::slice::from_raw_parts(ptr.add(byte_start), byte_end - byte_start) };
    bytemuck::try_cast_slice::<u8, f64>(bytes)
        .map_or_else(|_| Ok(None), |values| Ok(Some(Arc::from(values))))
}

pub(crate) fn arrow_i32_offsets(py: Python<'_>, array: &Bound<'_, PyAny>) -> PyResult<Vec<i32>> {
    arrow_buffer_values(py, array, 1, "offsets", 4, |chunk| {
        // `arrow_buffer_values` rejects non-multiple lengths, then uses `chunks_exact(4)`.
        i32::from_le_bytes(chunk.try_into().expect("chunk has 4 bytes"))
    })
}

pub(crate) fn arrow_i64_offsets(py: Python<'_>, array: &Bound<'_, PyAny>) -> PyResult<Vec<i64>> {
    arrow_buffer_values(py, array, 1, "offsets", 8, |chunk| {
        // `arrow_buffer_values` rejects non-multiple lengths, then uses `chunks_exact(8)`.
        i64::from_le_bytes(chunk.try_into().expect("chunk has 8 bytes"))
    })
}

pub(crate) fn arrow_buffer_values<T>(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    index: usize,
    name: &str,
    width: usize,
    decode: impl Fn(&[u8]) -> T,
) -> PyResult<Vec<T>> {
    let Some(buffer) = arrow_buffer_object(array, index)? else {
        return Err(geoarrow_parse_error(format!(
            "Arrow {name} buffer is required but missing"
        )));
    };
    if let Ok(buffer) = PyBuffer::<u8>::get(&buffer)
        && let Some(bytes) = buffer.as_slice(py)
    {
        if bytes.len() % width != 0 {
            return Err(arrow_buffer_width_error(name, width));
        }
        return Ok(decode_arrow_cell_bytes(bytes, width, decode));
    }
    let bytes = required_arrow_buffer(py, array, index, name)?;
    if bytes.len() % width != 0 {
        return Err(arrow_buffer_width_error(name, width));
    }
    Ok(bytes.chunks_exact(width).map(decode).collect())
}

/// Decode `count` cells of `width` bytes starting at element `start` from the
/// buffer at `index`, slicing the byte range `[start*width,
/// (start+count)*width)` rather than decoding the whole buffer.
pub(crate) fn arrow_buffer_values_span<T>(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    index: usize,
    name: &str,
    width: usize,
    start: usize,
    count: usize,
    decode: impl Fn(&[u8]) -> T,
) -> PyResult<Vec<T>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let span_error = || {
        geoarrow_parse_error(format!(
            "Arrow {name} buffer is shorter than the visible coordinate span"
        ))
    };
    let byte_start = start.checked_mul(width).ok_or_else(span_error)?;
    let byte_end = start
        .checked_add(count)
        .and_then(|end| end.checked_mul(width))
        .ok_or_else(span_error)?;
    let Some(buffer) = arrow_buffer_object(array, index)? else {
        return Err(geoarrow_parse_error(format!(
            "Arrow {name} buffer is required but missing"
        )));
    };
    if let Ok(buffer) = PyBuffer::<u8>::get(&buffer)
        && let Some(bytes) = buffer.as_slice(py)
    {
        let bytes = bytes.get(byte_start..byte_end).ok_or_else(span_error)?;
        return Ok(decode_arrow_cell_bytes(bytes, width, decode));
    }
    let bytes = required_arrow_buffer(py, array, index, name)?;
    let bytes = bytes.get(byte_start..byte_end).ok_or_else(span_error)?;
    Ok(bytes.chunks_exact(width).map(decode).collect())
}

pub(crate) fn decode_arrow_cell_bytes<T>(
    bytes: &[ReadOnlyCell<u8>],
    width: usize,
    decode: impl Fn(&[u8]) -> T,
) -> Vec<T> {
    let mut output = Vec::with_capacity(bytes.len() / width);
    let mut scratch = [0_u8; 8];
    for chunk in bytes.chunks_exact(width) {
        for (idx, byte) in chunk.iter().enumerate() {
            scratch[idx] = byte.get();
        }
        output.push(decode(&scratch[..width]));
    }
    output
}

pub(crate) fn arrow_buffer_width_error(name: &str, width: usize) -> PyErr {
    geoarrow_parse_error(format!(
        "Arrow {name} buffer length is not a multiple of {width}"
    ))
}

pub(crate) fn i32_offset_to_usize(value: i32) -> PyResult<usize> {
    usize::try_from(value).map_err(|_| geoarrow_parse_error("Arrow offsets must be non-negative"))
}

/// The `usize` offset at `index`, bounds-checked — used when following an Arrow
/// offset chain to the visible coordinate span.
pub(crate) fn offset_at(offsets: &[i32], index: usize) -> PyResult<usize> {
    let value = offsets
        .get(index)
        .copied()
        .ok_or_else(|| geoarrow_parse_error("Arrow offsets buffer is shorter than declared"))?;
    i32_offset_to_usize(value)
}

/// The visible coordinate span `(base, span)` between two coordinate-offset
/// endpoints, rejecting a non-monotonic chain.
pub(crate) fn coordinate_span(lo: usize, hi: usize) -> PyResult<(usize, usize)> {
    let span = hi
        .checked_sub(lo)
        .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?;
    Ok((lo, span))
}

pub(crate) fn i64_offset_to_usize(value: i64) -> PyResult<usize> {
    usize::try_from(value).map_err(|_| geoarrow_parse_error("Arrow offsets must be non-negative"))
}

pub(crate) fn push_i32_le(values: &mut Vec<u8>, value: usize) -> PyResult<()> {
    let value = i32::try_from(value).map_err(|_| {
        GeometryError::new_err(
            "Arrow output exceeds the i32 offset capacity for binary/list arrays",
        )
    })?;
    values.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

/// Geometry-construction failures while decoding `GeoArrow` buffers are parse
/// errors: the input data is bad, the structural rule is the detail.
pub(crate) fn arrow_content_error(error: crate::error::Error) -> PyErr {
    geoarrow_parse_error(format!("invalid GeoArrow: {error}"))
}

pub(crate) fn mixed_axes_error() -> PyErr {
    InvalidGeometryError::new_err("mixed Z/M coordinate axes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_message(error: PyErr) -> String {
        error.to_string()
    }

    #[test]
    fn offset_validators_accept_monotonic_windows() {
        pyo3::Python::initialize();
        ensure_i32_offsets_monotonic(&[0, 2, 5], 0, 2, 5).unwrap();
        ensure_i64_offsets_monotonic(&[0, 2, 5], 0, 2, 5).unwrap();
    }

    #[test]
    fn offset_validators_reject_descending_pairs() {
        pyo3::Python::initialize();
        for message in [
            error_message(
                ensure_i32_offsets_monotonic(&[0, 2, 1], 0, 2, 2)
                    .expect_err("descending i32 offsets"),
            ),
            error_message(
                ensure_i64_offsets_monotonic(&[0, 2, 1], 0, 2, 2)
                    .expect_err("descending i64 offsets"),
            ),
        ] {
            assert!(message.contains("Arrow offsets must be ordered"));
        }
    }

    #[test]
    fn offset_validators_reject_negative_and_conversion_overflow_offsets() {
        pyo3::Python::initialize();
        for message in [
            error_message(
                ensure_i32_offsets_monotonic(&[0, -1], 0, 1, 0).expect_err("negative i32 offset"),
            ),
            error_message(
                ensure_i64_offsets_monotonic(&[0, -1], 0, 1, 0).expect_err("negative i64 offset"),
            ),
            error_message(
                ensure_i32_offsets_monotonic(&[0, i32::MIN], 0, 1, 0)
                    .expect_err("i32 conversion overflow"),
            ),
            error_message(
                ensure_i64_offsets_monotonic(&[0, i64::MIN], 0, 1, 0)
                    .expect_err("i64 conversion overflow"),
            ),
        ] {
            assert!(message.contains("Arrow offsets must be non-negative"));
        }
    }

    #[test]
    fn offset_validators_preserve_negative_before_ordering_precedence() {
        pyo3::Python::initialize();
        for message in [
            error_message(
                ensure_i32_offsets_monotonic(&[2, 1, -1], 0, 2, 2)
                    .expect_err("negative descending i32 offsets"),
            ),
            error_message(
                ensure_i64_offsets_monotonic(&[2, 1, -1], 0, 2, 2)
                    .expect_err("negative descending i64 offsets"),
            ),
        ] {
            assert!(message.contains("Arrow offsets must be non-negative"));
        }
    }

    #[test]
    fn offset_validators_reject_terminals_past_the_child() {
        pyo3::Python::initialize();
        for message in [
            error_message(
                ensure_i32_offsets_monotonic(&[0, 3], 0, 1, 2)
                    .expect_err("i32 terminal past child"),
            ),
            error_message(
                ensure_i64_offsets_monotonic(&[0, 3], 0, 1, 2)
                    .expect_err("i64 terminal past child"),
            ),
        ] {
            assert!(message.contains("Arrow offset terminal (3) exceeds child length (2)"));
        }
    }

    #[test]
    fn offset_validators_validate_empty_windows() {
        pyo3::Python::initialize();
        ensure_i32_offsets_monotonic(&[2], 0, 0, 2).unwrap();
        ensure_i64_offsets_monotonic(&[2], 0, 0, 2).unwrap();
        for (index, message) in [
            error_message(
                ensure_i32_offsets_monotonic(&[2], 0, 0, 1)
                    .expect_err("i32 terminal past child in empty window"),
            ),
            error_message(
                ensure_i64_offsets_monotonic(&[2], 0, 0, 1)
                    .expect_err("i64 terminal past child in empty window"),
            ),
            error_message(
                ensure_i32_offsets_monotonic(&[-1], 0, 0, 0)
                    .expect_err("negative empty i32 window"),
            ),
            error_message(
                ensure_i64_offsets_monotonic(&[-1], 0, 0, 0)
                    .expect_err("negative empty i64 window"),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let expected = if index < 2 {
                "Arrow offset terminal (2) exceeds child length (1)"
            } else {
                "Arrow offsets must be non-negative"
            };
            assert!(message.contains(expected));
        }
    }
}
