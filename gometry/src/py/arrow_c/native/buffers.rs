use super::binary_view::{
    rebase_owned_binary_view_offsets, scan_binary_view_data_ranges_owned,
    validate_binary_view_prefixes,
};
use super::layout::{ArrowFormat, NullCount};
use super::node::owned_or_raw_row_valid;
use super::*;

/// Snapshot every schema-sized buffer once for this native node, then run
/// content validation **only on the owned copies** (design-1: never
/// `from_raw_parts` over producer memory for null_count / BinaryView scans).
///
/// Float64 *values* buffers (index 1) are decoded into `Arc<Vec<f64>>` at
/// admission so coordinate import reuses that Arc instead of retaining
/// LE bytes **and** materializing a second `Arc<Vec<f64>>`.
#[derive(Clone)]
pub(super) enum OwnedBuffer {
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    Bytes(Arc<Vec<u8>>),
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    F64(Arc<Vec<f64>>),
}

/// Owned Arrow buffers. Ordinary formats have a fixed small dense layout;
/// BinaryView's variadic payload slots are deliberately sparse because an
/// imported selected range may reference only a few of thousands of producer
/// fragments. Keeping absent payloads out of this representation makes
/// admission proportional to selected descriptors, not `n_buffers`.
#[derive(Clone)]
pub(super) enum OwnedBufferSlots {
    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    Dense(Arc<Vec<Option<OwnedBuffer>>>),
    BinaryView {
        validity: OwnedBuffer,
        views: OwnedBuffer,
        data: Arc<HashMap<usize, OwnedBuffer>>,
        n_buffers: usize,
    },
}

impl OwnedBufferSlots {
    pub(super) fn get(&self, index: usize) -> Option<&OwnedBuffer> {
        match self {
            Self::Dense(slots) => slots.get(index).and_then(Option::as_ref),
            Self::BinaryView {
                validity,
                views,
                data,
                n_buffers,
            } => match index {
                0 => Some(validity),
                1 => Some(views),
                index if index >= 2 && index < n_buffers.wrapping_sub(1) => {
                    data.get(&index.wrapping_sub(2))
                },
                _ => None,
            },
        }
    }
}

/// Snapshot producer buffers under the live shell, then validate content on
/// owned storage.
#[expect(
    clippy::iter_over_hash_type,
    reason = "range iteration only fills a keyed owned-buffer map; descriptor order remains the public order"
)]
pub(super) fn snapshot_native_buffers(array: &mut CaptureLayout) -> PyResult<OwnedBufferSlots> {
    if matches!(array.format, ArrowFormat::BinaryView) {
        // Snapshot only the descriptor control buffers. Unrelated variadic payload
        // slots never participate in validation or decoding for this visible
        // range, so neither a dense slot vector nor a full sizes-table walk is
        // justified here.
        let (validity_start, validity_len) = visible_validity_range(array)?;
        let validity = snapshot_one_buffer_range(array, 0, validity_start, validity_len)?;
        let views = snapshot_one_buffer_range(
            array,
            1,
            checked_byte_span(array.offset, 16, "Arrow binary-view offset overflows")?,
            checked_byte_span(
                array.length,
                16,
                "Arrow binary-view buffer length overflows",
            )?,
        )?;
        // Phase 2: content-scan owned descriptors and snapshot just the
        // referenced 8-byte variadic-size entries → sparse ranges.
        let ranges = scan_binary_view_data_ranges_owned(array, &validity, &views)?;
        let views = rebase_owned_binary_view_offsets(array, &validity, &views, &ranges)?;
        // Phase 3: each referenced payload retains only its selected envelope
        // (not the physical parent prefix), after the owned descriptors have
        // been rebased to that local origin.
        let mut data = HashMap::new();
        data.reserve(ranges.len());
        for (&data_index, range) in &ranges {
            let len = range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| PyTypeError::new_err("Arrow binary-view data range is invalid"))?;
            data.insert(
                data_index,
                snapshot_one_buffer_range(array, data_index.wrapping_add(2), range.start, len)?,
            );
        }
        validate_binary_view_prefixes(&validity, &views, &data, array)?;
        let slots = OwnedBufferSlots::BinaryView {
            validity,
            views,
            data: arc_new(data),
            n_buffers: array.n_buffers,
        };
        validate_owned_null_count(array, &slots)?;
        return Ok(slots);
    }

    let mut snapshots = Vec::with_capacity(array.n_buffers);
    snapshots.push(Some(snapshot_one_buffer_range(
        array,
        0,
        visible_validity_range(array)?.0,
        visible_validity_range(array)?.1,
    )?));
    match array.format {
        ArrowFormat::Float64 => snapshots.push(Some(snapshot_one_buffer_range(
            array,
            1,
            checked_byte_span(array.offset, 8, "Arrow float64 offset overflows")?,
            checked_byte_span(array.length, 8, "Arrow float64 values length overflows")?,
        )?)),
        ArrowFormat::Binary
        | ArrowFormat::LargeBinary
        | ArrowFormat::List
        | ArrowFormat::LargeList => {
            let (offsets, start, end) = snapshot_rebased_offsets(array)?;
            snapshots.push(Some(offsets));
            if matches!(array.format, ArrowFormat::Binary | ArrowFormat::LargeBinary) {
                let len = end
                    .checked_sub(start)
                    .ok_or_else(|| arrow::geoarrow_parse_error("Arrow offsets must be ordered"))?;
                snapshots.push(Some(snapshot_one_buffer_range(array, 2, start, len)?));
            } else {
                array.child_window = Some((
                    start,
                    end.checked_sub(start).ok_or_else(|| {
                        arrow::geoarrow_parse_error("Arrow offsets must be ordered")
                    })?,
                ));
            }
        },
        ArrowFormat::Struct | ArrowFormat::FixedSizeList(_) => {},
        ArrowFormat::BinaryView => unreachable!("handled above"),
    }

    let slots = OwnedBufferSlots::Dense(arc_from_vec(snapshots));
    // Content validation on OWNED validity only (P02 null_count match).
    validate_owned_null_count(array, &slots)?;
    Ok(slots)
}

pub(super) fn snapshot_one_buffer_range(
    array: &CaptureLayout,
    index: usize,
    byte_start: usize,
    len: usize,
) -> PyResult<OwnedBuffer> {
    if len == 0 {
        // Typed empty for float values so decode reuses Arc<Vec<f64>> without
        // a second LE→f64 materialization path.
        if matches!(array.format, ArrowFormat::Float64) && index == 1 {
            return Ok(OwnedBuffer::F64(arc_from_vec(Vec::new())));
        }
        return Ok(OwnedBuffer::Bytes(arc_from_vec(Vec::new())));
    }
    // SAFETY: layout validation + capture shell pin buffer table.
    let source = unsafe { array.buffer_ptr(index) };
    if source.is_null() {
        // Validity is optional. Every non-empty payload/offset buffer is
        // required; returning an empty snapshot here would let a later reader
        // index it and panic instead of reporting malformed Arrow input.
        if index == 0 {
            return Ok(OwnedBuffer::Bytes(arc_from_vec(Vec::new())));
        }
        return Err(PyTypeError::new_err("Arrow required buffer is missing"));
    }
    // SAFETY: layout-validated buffer table under live shell; producer
    // quiescent for this finite capture; no nested Python.
    let source = unsafe { source.cast::<u8>().add(byte_start) };
    // SAFETY: `source` spans the layout-validated selected range and the
    // borrowed shell pins its producer allocation until this snapshot finishes.
    let foreign = unsafe { ForeignArrowBuffer::new(source, len, array.shell) };
    // SAFETY: ForeignArrowBuffer::snapshot contract (same as construction).
    let bytes = unsafe { foreign.snapshot() };
    // Float64 values: decode LE f64 once at admission; drop the byte copy.
    if matches!(array.format, ArrowFormat::Float64) && index == 1 {
        return owned_f64_from_le_bytes(&bytes).map(OwnedBuffer::F64);
    }
    Ok(OwnedBuffer::Bytes(arc_from_vec(bytes)))
}

fn visible_validity_range(array: &CaptureLayout) -> PyResult<(usize, usize)> {
    let start = array.offset / 8;
    let bits = array
        .length
        .checked_add(array.offset % 8)
        .ok_or_else(|| PyTypeError::new_err("Arrow validity bitmap length overflows"))?;
    Ok((start, bits.div_ceil(8)))
}

/// Snapshot exactly the visible `(length + 1)` offset entries, then rebase
/// them in owned storage. The returned first/last offsets are the selected
/// child or byte span; no producer offset is inspected after this copy.
fn snapshot_rebased_offsets(array: &CaptureLayout) -> PyResult<(OwnedBuffer, usize, usize)> {
    let width = array
        .format
        .offset_width()
        .ok_or_else(|| PyTypeError::new_err("Arrow offsets require a variable-width format"))?;
    let slots = array
        .length
        .checked_add(1)
        .ok_or_else(|| PyTypeError::new_err("Arrow offset count overflows"))?;
    let start = checked_byte_span(array.offset, width, "Arrow offset start overflows")?;
    let len = checked_byte_span(slots, width, "Arrow offset window length overflows")?;
    // Arrow's canonical empty variable-width representation is one zero
    // offset. Some ABI-conforming producers omit that otherwise-empty buffer;
    // it carries no payload and is unambiguous only at physical offset zero.
    // A sliced empty array still has a meaningful start slot and therefore
    // must provide it for validation.
    let raw = if array.length == 0
        && array.offset == 0
        // SAFETY: `CaptureLayout` pins the producer buffer table for this
        // admission snapshot.
        && unsafe { array.buffer_ptr(1) }.is_null()
    {
        arc_from_vec(vec![0; width])
    } else {
        let OwnedBuffer::Bytes(raw) = snapshot_one_buffer_range(array, 1, start, len)? else {
            unreachable!("offset snapshots are byte buffers")
        };
        raw
    };
    let read = |index: usize| -> PyResult<i64> {
        let at = index
            .checked_mul(width)
            .ok_or_else(|| PyTypeError::new_err("Arrow offset index overflows"))?;
        let end = at
            .checked_add(width)
            .ok_or_else(|| PyTypeError::new_err("Arrow offset index overflows"))?;
        let bytes = raw
            .get(at..end)
            .ok_or_else(|| PyTypeError::new_err("Arrow offsets buffer is shorter than declared"))?;
        Ok(if width == 4 {
            let bytes = bytes
                .first_chunk::<4>()
                .ok_or_else(|| PyTypeError::new_err("Arrow offset is truncated"))?;
            i64::from(i32::from_le_bytes(*bytes))
        } else {
            let bytes = bytes
                .first_chunk::<8>()
                .ok_or_else(|| PyTypeError::new_err("Arrow offset width is invalid"))?;
            i64::from_le_bytes(*bytes)
        })
    };
    let first = usize::try_from(read(0)?)
        .map_err(|_| arrow::geoarrow_parse_error("Arrow offsets must be non-negative"))?;
    let mut rebased = raw.to_vec();
    let mut last = first;
    for (index, window) in rebased.chunks_exact_mut(width).enumerate() {
        let value = usize::try_from(read(index)?)
            .map_err(|_| arrow::geoarrow_parse_error("Arrow offsets must be non-negative"))?;
        if value < last {
            return Err(arrow::geoarrow_parse_error("Arrow offsets must be ordered"));
        }
        let local = value
            .checked_sub(first)
            .ok_or_else(|| arrow::geoarrow_parse_error("Arrow offsets must be ordered"))?;
        last = value;
        if width == 4 {
            let local = i32::try_from(local)
                .map_err(|_| arrow::geoarrow_parse_error("Arrow offset exceeds i32 range"))?;
            window.copy_from_slice(&local.to_le_bytes());
        } else {
            let local = i64::try_from(local)
                .map_err(|_| arrow::geoarrow_parse_error("Arrow offset exceeds i64 range"))?;
            window.copy_from_slice(&local.to_le_bytes());
        }
    }
    Ok((OwnedBuffer::Bytes(arc_from_vec(rebased)), first, last))
}

#[expect(
    clippy::rc_buffer,
    reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
)]
fn owned_f64_from_le_bytes(bytes: &[u8]) -> PyResult<Arc<Vec<f64>>> {
    if !bytes.len().is_multiple_of(8) {
        return Err(PyTypeError::new_err(
            "Arrow float64 values length is not a multiple of 8",
        ));
    }
    let (chunks, rest) = bytes.as_chunks::<8>();
    debug_assert!(rest.is_empty());
    let mut values = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        values.push(f64::from_le_bytes(*chunk));
    }
    Ok(arc_from_vec(values))
}

/// P02: Known(n>0) null_count must match the owned validity bitmap.
fn validate_owned_null_count(array: &CaptureLayout, buffers: &OwnedBufferSlots) -> PyResult<()> {
    let NullCount::Known(expected) = array.null_count else {
        return Ok(());
    };
    if expected == 0 {
        return Ok(());
    }
    let mut actual = 0_usize;
    for row in 0..array.length {
        if !owned_or_raw_row_valid(array, buffers, row)? {
            actual = actual.wrapping_add(1);
        }
    }
    if actual != expected {
        return Err(arrow::geoarrow_parse_error(format!(
            "Arrow null_count ({expected}) does not match validity bitmap ({actual} null rows)"
        )));
    }
    Ok(())
}

/// Checked byte span for every `from_raw_parts` length: reject `usize`
/// overflow and any result greater than `isize::MAX` (Rust slice UB).
pub(in crate::py::arrow_c) fn checked_byte_span(
    count: usize,
    width: usize,
    overflow: &'static str,
) -> PyResult<usize> {
    let bytes = count
        .checked_mul(width)
        .ok_or_else(|| PyOverflowError::new_err(overflow))?;
    if bytes > isize::MAX as usize {
        return Err(PyOverflowError::new_err(overflow));
    }
    Ok(bytes)
}
