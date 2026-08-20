use std::sync::Arc;

use pyo3::buffer::PyBuffer;

use crate::py::arrow::{
    ArrowValidity, Bound, GeometryError, InvalidGeometryError, PyAny, PyAnyMethods as _, PyErr,
    PyResult, Python, geoarrow_parse_error, validity_bitmap_from_missing,
};

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
    arrow_validity_window(py, array, 0, array.len()?)
}

/// Admit validity only for the logical window `[start, start + len)`. This is
/// deliberately separate from [`arrow_validity`]: nested GeoArrow import
/// projects child rows before it touches their bitmap, so a one-row parent
/// slice never retains validity for every physical child row.
pub(crate) fn arrow_validity_window(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    start: usize,
    len: usize,
) -> PyResult<ArrowValidity> {
    // Empty arrays never need a validity bitmap (there are no rows to probe),
    // even when `null_count == -1` (unknown). Only a non-empty array with a
    // non-zero null count consults the bitmap.
    let array_len = array.len()?;
    let end = start
        .checked_add(len)
        .filter(|&end| end <= array_len)
        .ok_or_else(|| geoarrow_parse_error("Arrow validity window exceeds array length"))?;
    if len == 0 {
        return Ok(ArrowValidity {
            bitmap: None,
            offset: 0,
        });
    }
    // Native struct-child views may carry an ancestor-OR missing mask that is
    // not present on the raw child validity bitmap. Prefer that combined mask
    // (logical window, offset 0) so parent nulls cannot resurrect geometry.
    if let Some(missing) = crate::py::arrow_c::native_arrow_effective_missing(array) {
        return missing
            .get(start..end)
            .map(arrow_validity_from_missing_mask)
            .ok_or_else(|| geoarrow_parse_error("Arrow validity window exceeds array length"));
    }
    let null_count = array.getattr("null_count")?.extract::<i64>()?;
    if null_count == 0 {
        return Ok(ArrowValidity {
            bitmap: None,
            offset: 0,
        });
    }
    // Native admission has already retained exactly this physical byte window
    // and recorded its bit alignment. Direct PyArrow arrays still point at a
    // parent bitmap after slicing, so retain only the bytes covering the
    // visible bits and rebase the bit offset to that owned window.
    let (bitmap, offset) =
        if let Some(offset) = crate::py::arrow_c::native_arrow_validity_offset(array) {
            let bit_offset = offset
                .checked_add(start)
                .ok_or_else(|| geoarrow_parse_error("Arrow validity bitmap length overflows"))?;
            let byte_start = bit_offset / 8;
            let bits = len
                .checked_add(bit_offset % 8)
                .ok_or_else(|| geoarrow_parse_error("Arrow validity bitmap length overflows"))?;
            (
                required_arrow_buffer_span(
                    py,
                    array,
                    0,
                    "validity bitmap",
                    byte_start,
                    byte_start.checked_add(bits.div_ceil(8)).ok_or_else(|| {
                        geoarrow_parse_error("Arrow validity bitmap length overflows")
                    })?,
                )?,
                bit_offset % 8,
            )
        } else {
            let physical_offset = arrow_array_offset(array)?
                .checked_add(start)
                .ok_or_else(|| geoarrow_parse_error("Arrow validity bitmap length overflows"))?;
            let byte_start = physical_offset / 8;
            let bits = len
                .checked_add(physical_offset % 8)
                .ok_or_else(|| geoarrow_parse_error("Arrow validity bitmap length overflows"))?;
            let byte_len = bits.div_ceil(8);
            (
                required_arrow_buffer_span(
                    py,
                    array,
                    0,
                    "validity bitmap",
                    byte_start,
                    byte_start.checked_add(byte_len).ok_or_else(|| {
                        geoarrow_parse_error("Arrow validity bitmap length overflows")
                    })?,
                )?,
                physical_offset % 8,
            )
        };
    let validity = ArrowValidity {
        bitmap: Some(bitmap),
        offset,
    };
    // P02 describes an Arrow array's full logical bitmap. A nested sub-window
    // cannot compare its local count to that array-wide metadata, but must
    // still retain exactly the bits it will dereference.
    if null_count > 0 && start == 0 && len == array_len {
        let mask = (0..len).map(|index| !validity.is_valid(index));
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

/// Monotonic check for offsets already converted to absolute `usize` child
/// indices (list / large_list unified path).
pub(crate) fn ensure_usize_offsets_monotonic(
    offsets: &[usize],
    window: usize,
    count: usize,
    child_len: usize,
) -> PyResult<()> {
    ensure_arrow_offsets_len(offsets.len(), window, count)?;
    let mut terminal = offsets[window];
    let mut unordered = false;
    for index in 1..=count {
        let next = offsets[window + index];
        unordered |= terminal > next;
        terminal = next;
    }
    if unordered {
        return Err(geoarrow_parse_error("Arrow offsets must be ordered"));
    }
    ensure_offset_terminal_within_child(terminal, child_len)
}

/// Reject a terminal offset that indexes past the child array / data buffer.
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

/// Admitted Arrow buffer: always an owned snapshot of producer bytes.
///
/// Arrow only *recommends* immutability of exported buffers; an ABI-conforming
/// producer may still mutate. Shared Rust `&[u8]` over producer memory is
/// therefore a data race (reproduced with genuine mutable PyArrow buffers on
/// both the address-pin and native Arrow-C lanes). Admission copies into
/// exclusive Rust storage; decode never holds a shared view of foreign bytes.
#[derive(Clone, Debug)]
pub(crate) struct AdmittedBuffer {
    storage: std::sync::Arc<[u8]>,
    range: std::ops::Range<usize>,
}

impl AdmittedBuffer {
    pub(crate) fn from_vec(bytes: Vec<u8>) -> Self {
        let len = bytes.len();
        Self {
            storage: std::sync::Arc::from(bytes),
            range: 0..len,
        }
    }

    /// Share an existing owned Arc buffer span (native admission path).
    pub(crate) fn from_arc_range(
        storage: std::sync::Arc<[u8]>,
        range: std::ops::Range<usize>,
    ) -> Self {
        debug_assert!(range.end <= storage.len());
        Self { storage, range }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        self.storage.get(self.range.clone()).unwrap_or(&[])
    }

    pub(crate) fn into_owned(self) -> Vec<u8> {
        self.as_slice().to_vec()
    }

    pub(crate) fn subrange(&self, range: std::ops::Range<usize>) -> PyResult<Self> {
        let len = self.range.end - self.range.start;
        if range.end < range.start || range.end > len {
            return Err(geoarrow_parse_error(
                "Arrow admitted buffer sub-range is out of bounds",
            ));
        }
        Ok(Self {
            storage: std::sync::Arc::clone(&self.storage),
            range: (self.range.start + range.start)..(self.range.start + range.end),
        })
    }

    pub(crate) const fn len(&self) -> usize {
        self.range.end - self.range.start
    }
}

/// Copy `[byte_start, byte_end)` of a Python buffer object into owned bytes
/// via a safe provider path (`to_pybytes` / `PyBuffer::to_vec`). Never forms a
/// long-lived shared Rust slice over producer memory.
fn copy_buffer_span(
    py: Python<'_>,
    buffer: &Bound<'_, PyAny>,
    byte_start: usize,
    byte_end: usize,
) -> PyResult<Vec<u8>> {
    let span_error =
        || geoarrow_parse_error("Arrow buffer is shorter than the requested byte span");
    if byte_end < byte_start {
        return Err(span_error());
    }
    if byte_start == byte_end {
        return Ok(Vec::new());
    }
    let span_len = byte_end - byte_start;
    // Preferred: pyarrow.Buffer.slice then to_pybytes — copies only the visible
    // span (M1), not the full parent allocation.
    if let Ok(sliced) = buffer.call_method1("slice", (byte_start, span_len))
        && let Ok(pybytes) = sliced.call_method0("to_pybytes")
    {
        let bytes = pybytes.extract::<Vec<u8>>()?;
        if bytes.len() != span_len {
            return Err(span_error());
        }
        return Ok(bytes);
    }
    // Fallback: full to_pybytes then slice.
    if let Ok(pybytes) = buffer.call_method0("to_pybytes") {
        let full = pybytes.extract::<Vec<u8>>()?;
        return full
            .get(byte_start..byte_end)
            .map(<[u8]>::to_vec)
            .ok_or_else(span_error);
    }
    if let Ok(buf) = PyBuffer::<u8>::get(buffer) {
        if byte_end > buf.len_bytes() {
            return Err(span_error());
        }
        // PyBuffer_ToContiguous into exclusive storage — does not leave a
        // shared Rust view of producer memory for the decode lifetime.
        let full = buf.to_vec(py)?;
        return full
            .get(byte_start..byte_end)
            .map(<[u8]>::to_vec)
            .ok_or_else(span_error);
    }
    Err(geoarrow_parse_error(
        "Arrow buffer could not be copied into owned storage",
    ))
}

/// Subrange of a native admitted buffer (already owned at capsule admission).
fn copy_native_buffer_span(
    array: &Bound<'_, PyAny>,
    index: usize,
    byte_start: usize,
    byte_end: usize,
) -> PyResult<Option<AdmittedBuffer>> {
    let Some(admitted) = crate::py::arrow_c::try_native_admitted_buffer(array, index) else {
        return Ok(None);
    };
    if byte_end < byte_start || byte_end > admitted.len() {
        return Err(geoarrow_parse_error(
            "Arrow admitted buffer range is out of bounds",
        ));
    }
    Ok(Some(admitted.subrange(byte_start..byte_end)?))
}

/// Admit `[byte_start, byte_end)` of Binary/LargeBinary data (buffer index 2)
/// as an owned byte snapshot. Never returns a shared view of producer memory.
pub(crate) fn arrow_binary_data_span_admitted(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    byte_start: usize,
    byte_end: usize,
) -> PyResult<AdmittedBuffer> {
    let span_error =
        || geoarrow_parse_error("Arrow WKB data buffer is shorter than declared offsets");
    if byte_end < byte_start {
        return Err(span_error());
    }
    // Empty visible span: short-circuit before buffer lookup. Zero-sized Arrow
    // data buffers may carry a null pointer (empty Binary/LargeBinary arrays).
    if byte_start == byte_end {
        return Ok(AdmittedBuffer::from_vec(Vec::new()));
    }
    arrow_buffer_span_admitted(py, array, 2, "WKB data", byte_start, byte_end)
}

/// Copy one physical byte window from a required Arrow buffer. Direct PyArrow
/// slices retain their parent buffers, so callers that already know the
/// visible window must use this rather than materializing the parent.
pub(crate) fn arrow_buffer_span_admitted(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    index: usize,
    name: &str,
    byte_start: usize,
    byte_end: usize,
) -> PyResult<AdmittedBuffer> {
    if let Some(admitted) = copy_native_buffer_span(array, index, byte_start, byte_end)? {
        return Ok(admitted);
    }
    let Some(buffer) = arrow_buffer_object(array, index)? else {
        return Err(geoarrow_parse_error(format!(
            "Arrow {name} buffer is required but missing"
        )));
    };
    Ok(AdmittedBuffer::from_vec(copy_buffer_span(
        py, &buffer, byte_start, byte_end,
    )?))
}

fn required_arrow_buffer_span(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    index: usize,
    name: &str,
    byte_start: usize,
    byte_end: usize,
) -> PyResult<Vec<u8>> {
    arrow_buffer_span_admitted(py, array, index, name, byte_start, byte_end)
        .map(AdmittedBuffer::into_owned)
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
    // Native admitted buffer: length of the owned snapshot.
    if let Some(admitted) = crate::py::arrow_c::try_native_admitted_buffer(array, 2) {
        return Ok(admitted.len());
    }
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
    py: Python<'_>,
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
    // Prefer typed float64 admission Arc (no dual byte+f64 retention).
    let cell_start = byte_start / 8;
    let cell_count = (byte_end - byte_start) / 8;
    if let Some(result) =
        crate::py::arrow_c::try_native_f64_values_arc(array, cell_start, cell_count)
    {
        return result.map(Some);
    }
    // Snapshot producer bytes into owned storage, then decode. Never retain a
    // shared Rust view of Arrow buffers (producer mutability is not forbidden).
    if let Some(admitted) = copy_native_buffer_span(array, 1, byte_start, byte_end)? {
        return owned_f64_arc_from_le_bytes(admitted.as_slice()).map(Some);
    }
    let Some(buffer) = arrow_buffer_object(array, 1)? else {
        return Err(geoarrow_parse_error(
            "Arrow values buffer is required but missing",
        ));
    };
    let bytes = copy_buffer_span(py, &buffer, byte_start, byte_end)?;
    owned_f64_arc_from_le_bytes(&bytes).map(Some)
}

/// Decode little-endian f64 cells from an owned byte snapshot into `Arc<[f64]>`.
fn owned_f64_arc_from_le_bytes(bytes: &[u8]) -> PyResult<Arc<[f64]>> {
    if !bytes.len().is_multiple_of(8) {
        return Err(geoarrow_parse_error(
            "Arrow values buffer length is not a multiple of 8",
        ));
    }
    let (chunks, rest) = bytes.as_chunks::<8>();
    debug_assert!(rest.is_empty());
    let values: Vec<f64> = chunks
        .iter()
        .map(|chunk| f64::from_le_bytes(*chunk))
        .collect();
    Ok(Arc::from(values))
}

/// Snapshot exactly `count` i32 offset entries beginning at physical offset
/// slot `start`. Variable-width Arrow arrays retain a shared parent offsets
/// buffer when sliced, so callers must not fall back to [`arrow_i32_offsets`]
/// merely to inspect their visible window.
pub(crate) fn arrow_i32_offsets_window(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    start: usize,
    count: usize,
) -> PyResult<Vec<i32>> {
    arrow_buffer_values_span(py, array, 1, "offsets", 4, start, count, |chunk| {
        i32::from_le_bytes(chunk.try_into().expect("chunk has 4 bytes"))
    })
}

/// i64 / LargeList sibling of [`arrow_i32_offsets_window`].
pub(crate) fn arrow_i64_offsets_window(
    py: Python<'_>,
    array: &Bound<'_, PyAny>,
    start: usize,
    count: usize,
) -> PyResult<Vec<i64>> {
    arrow_buffer_values_span(py, array, 1, "offsets", 8, start, count, |chunk| {
        i64::from_le_bytes(chunk.try_into().expect("chunk has 8 bytes"))
    })
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
    if let Some(admitted) = copy_native_buffer_span(array, index, byte_start, byte_end)? {
        return Ok(admitted
            .as_slice()
            .chunks_exact(width)
            .map(decode)
            .collect());
    }
    let Some(buffer) = arrow_buffer_object(array, index)? else {
        return Err(geoarrow_parse_error(format!(
            "Arrow {name} buffer is required but missing"
        )));
    };
    // `copy_buffer_span` prefers `Buffer.slice(start, len).to_pybytes()`;
    // use it for offset windows too. `PyBuffer::to_vec` would first copy the
    // physical parent and silently restore the one-row-slice cliff.
    let bytes = copy_buffer_span(py, &buffer, byte_start, byte_end).map_err(|_| span_error())?;
    Ok(bytes.chunks_exact(width).map(decode).collect())
}

pub(crate) fn i32_offset_to_usize(value: i32) -> PyResult<usize> {
    usize::try_from(value).map_err(|_| geoarrow_parse_error("Arrow offsets must be non-negative"))
}

/// Absolute child index at `index` from a pre-converted offsets buffer.
pub(crate) fn usize_offset_at(offsets: &[usize], index: usize) -> PyResult<usize> {
    offsets
        .get(index)
        .copied()
        .ok_or_else(|| geoarrow_parse_error("Arrow offsets buffer is shorter than declared"))
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
        crate::test_support::initialize_python();
        ensure_i32_offsets_monotonic(&[0, 2, 5], 0, 2, 5).unwrap();
        ensure_i64_offsets_monotonic(&[0, 2, 5], 0, 2, 5).unwrap();
    }

    #[test]
    fn offset_validators_reject_descending_pairs() {
        crate::test_support::initialize_python();
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
        crate::test_support::initialize_python();
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
        crate::test_support::initialize_python();
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
        crate::test_support::initialize_python();
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
        crate::test_support::initialize_python();
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
