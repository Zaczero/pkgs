use super::buffers::{OwnedBuffer, checked_byte_span, snapshot_one_buffer_range};
use super::layout::NullCount;
use super::*;

/// Validate the four-byte BinaryView inline prefix against the selected owned
/// payload.  The descriptor and data are already copied, so this check never
/// re-borrows producer memory and remains safe for sparse high-index buffers.
pub(super) fn validate_binary_view_prefixes(
    validity: &OwnedBuffer,
    views: &OwnedBuffer,
    data: &HashMap<usize, OwnedBuffer>,
    array: &CaptureLayout,
) -> PyResult<()> {
    let OwnedBuffer::Bytes(validity) = validity else {
        return Ok(());
    };
    let OwnedBuffer::Bytes(views) = views else {
        return Ok(());
    };
    let validity_buffer = OwnedBuffer::Bytes(Arc::clone(validity));
    let (descriptors, _) = views.as_chunks::<16>();
    for (row, view) in descriptors.iter().take(array.length).enumerate() {
        if !owned_binary_view_row_is_valid(array, &validity_buffer, row)? {
            continue;
        }
        let length = i32::from_le_bytes([view[0], view[1], view[2], view[3]]);
        if length <= 12 {
            continue;
        }
        let buffer_index =
            usize::try_from(i32::from_le_bytes([view[8], view[9], view[10], view[11]]))
                .map_err(|_| PyTypeError::new_err("Arrow binary-view buffer index is negative"))?;
        let byte_offset =
            usize::try_from(i32::from_le_bytes([view[12], view[13], view[14], view[15]]))
                .map_err(|_| PyTypeError::new_err("Arrow binary-view byte offset is negative"))?;
        // `views` has already been rebased to the selected data envelope, so
        // the descriptor's offset is directly an index into the owned payload.
        let local = byte_offset;
        let prefix_end = local
            .checked_add(4)
            .ok_or_else(|| PyTypeError::new_err("Arrow binary-view prefix range overflows"))?;
        let payload = match data.get(&buffer_index) {
            Some(OwnedBuffer::Bytes(bytes)) => bytes
                .get(local..prefix_end)
                .ok_or_else(|| PyTypeError::new_err("Arrow binary-view prefix is out of bounds"))?,
            _ => {
                return Err(PyTypeError::new_err(
                    "Arrow binary-view data buffer is missing",
                ));
            },
        };
        if payload != &view[4..8] {
            return Err(PyTypeError::new_err(
                "Arrow binary-view prefix does not match referenced data",
            ));
        }
    }
    Ok(())
}

/// Decode one entry of an **owned** BinaryView variadic-sizes table.
fn binary_view_declared_data_size(sizes: &[u8], buffer_index: usize) -> PyResult<usize> {
    let byte_offset = checked_byte_span(
        buffer_index,
        8,
        "Arrow binary-view variadic-sizes index overflows",
    )?;
    let end = byte_offset
        .checked_add(8)
        .ok_or_else(|| PyTypeError::new_err("Arrow binary-view variadic-sizes index overflows"))?;
    let bytes = sizes
        .get(byte_offset..end)
        .ok_or_else(|| PyTypeError::new_err("Arrow binary-view buffer index out of range"))?
        .first_chunk::<8>()
        .ok_or_else(|| PyTypeError::new_err("Arrow binary-view variadic size is truncated"))?;
    usize::try_from(i64::from_le_bytes(*bytes)).map_err(|_| {
        PyTypeError::new_err("Arrow binary-view variadic data buffer size is negative or too large")
    })
}

/// BinaryView selected-span scan on owned validity/views (design-1). Each
/// referenced descriptor snapshots its own 8-byte variadic-size entry; data
/// buffers record the smallest enclosing selected range, never unrelated
/// producer fragments or a dense sizes table.
pub(super) fn scan_binary_view_data_ranges_owned(
    array: &CaptureLayout,
    validity: &OwnedBuffer,
    views: &OwnedBuffer,
) -> PyResult<HashMap<usize, std::ops::Range<usize>>> {
    let mut ranges = HashMap::new();
    if array.length == 0 {
        return Ok(ranges);
    }
    // CaptureLayout admits BinaryView only with at least three buffers.
    let max_data_buffers = array.n_buffers.wrapping_sub(3);
    let OwnedBuffer::Bytes(views) = views else {
        return Ok(ranges);
    };
    if views.is_empty() {
        return Ok(ranges);
    }
    let sizes_index = array.n_buffers.wrapping_sub(1);
    let mut declared_sizes = HashMap::new();
    let (descriptors, _) = views.as_chunks::<16>();
    if descriptors.len() < array.length {
        return Err(PyTypeError::new_err(
            "Arrow binary-view buffer is shorter than declared",
        ));
    }
    for (row, view) in descriptors.iter().take(array.length).enumerate() {
        if !owned_binary_view_row_is_valid(array, validity, row)? {
            continue;
        }
        let length = i32::from_le_bytes([view[0], view[1], view[2], view[3]]);
        let length = usize::try_from(length)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view length is negative"))?;
        if length <= 12 {
            if view.iter().skip(4).skip(length).any(|&b| b != 0) {
                return Err(PyTypeError::new_err(
                    "Arrow binary-view inline padding must be zero",
                ));
            }
            continue;
        }
        let buffer_index = i32::from_le_bytes([view[8], view[9], view[10], view[11]]);
        let buffer_index = usize::try_from(buffer_index)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view buffer index is negative"))?;
        if buffer_index >= max_data_buffers {
            return Err(PyTypeError::new_err(
                "Arrow binary-view buffer index out of range",
            ));
        }
        let byte_offset = i32::from_le_bytes([view[12], view[13], view[14], view[15]]);
        let byte_offset = usize::try_from(byte_offset)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view byte offset is negative"))?;
        let span_end = byte_offset
            .checked_add(length)
            .ok_or_else(|| PyTypeError::new_err("Arrow binary-view byte range overflows"))?;
        checked_byte_span(span_end, 1, "Arrow binary-view data length overflows")?;
        // The sizes table is advisory for unreferenced fragments. Snapshot and
        // validate precisely the selected descriptor's entry, not the whole
        // `n_buffers - 3` table.
        let declared_size = if let Some(size) = declared_sizes.get(&buffer_index) {
            *size
        } else {
            declared_sizes.reserve(1);
            let byte_start = checked_byte_span(
                buffer_index,
                8,
                "Arrow binary-view variadic-sizes index overflows",
            )?;
            let OwnedBuffer::Bytes(size) =
                snapshot_one_buffer_range(array, sizes_index, byte_start, 8)?
            else {
                unreachable!("variadic sizes are byte buffers")
            };
            let declared_size = binary_view_declared_data_size(&size, 0)?;
            declared_sizes.insert(buffer_index, declared_size);
            declared_size
        };
        if span_end > declared_size {
            return Err(PyTypeError::new_err(
                "Arrow binary-view byte range exceeds declared data buffer size",
            ));
        }
        // Prefix match deferred until data buffers are snapshotted (owned).
        if !ranges.contains_key(&buffer_index) {
            ranges.reserve(1);
        }
        ranges
            .entry(buffer_index)
            .and_modify(|range: &mut std::ops::Range<usize>| {
                range.start = range.start.min(byte_offset);
                range.end = range.end.max(span_end);
            })
            .or_insert(byte_offset..span_end);
    }
    Ok(ranges)
}

/// Rebase non-inline BinaryView descriptor offsets to their selected owned
/// data envelope. This happens only after every descriptor has been checked
/// against the declared producer size, and before the producer payloads are
/// captured, so later generic decode sees a self-contained Arrow view.
pub(super) fn rebase_owned_binary_view_offsets(
    array: &CaptureLayout,
    validity: &OwnedBuffer,
    views: &OwnedBuffer,
    ranges: &HashMap<usize, std::ops::Range<usize>>,
) -> PyResult<OwnedBuffer> {
    let OwnedBuffer::Bytes(views) = views else {
        return Ok(views.clone());
    };
    let mut rebased = views.to_vec();
    let (views, remainder) = rebased.as_chunks_mut::<16>();
    debug_assert!(remainder.is_empty());
    for (row, view) in views.iter_mut().enumerate() {
        // Null BinaryView descriptors are explicitly inert: their control
        // bytes may be arbitrary and must neither establish a range nor be
        // parsed/rebased (the same rule as the owned range scan).
        if !owned_binary_view_row_is_valid(array, validity, row)? {
            continue;
        }
        let length = i32::from_le_bytes([view[0], view[1], view[2], view[3]]);
        let length = usize::try_from(length)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view length is negative"))?;
        if length <= 12 {
            continue;
        }
        let buffer_index = i32::from_le_bytes([view[8], view[9], view[10], view[11]]);
        let buffer_index = usize::try_from(buffer_index)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view buffer index is negative"))?;
        let byte_offset = i32::from_le_bytes([view[12], view[13], view[14], view[15]]);
        let byte_offset = usize::try_from(byte_offset)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view byte offset is negative"))?;
        let range = ranges.get(&buffer_index).ok_or_else(|| {
            PyTypeError::new_err("Arrow binary-view data range is missing for a referenced view")
        })?;
        let local = byte_offset
            .checked_sub(range.start)
            .ok_or_else(|| PyTypeError::new_err("Arrow binary-view data range is invalid"))?;
        let local = i32::try_from(local)
            .map_err(|_| PyTypeError::new_err("Arrow binary-view offset exceeds i32 range"))?;
        view[12..16].copy_from_slice(&local.to_le_bytes());
    }
    Ok(OwnedBuffer::Bytes(arc_from_vec(rebased)))
}

/// Owned-bitmap BinaryView validity (no producer slice).
fn owned_binary_view_row_is_valid(
    array: &CaptureLayout,
    validity: &OwnedBuffer,
    row: usize,
) -> PyResult<bool> {
    match array.null_count {
        NullCount::Known(0) => Ok(true),
        NullCount::Known(n) if n == array.length => Ok(false),
        NullCount::Known(_) | NullCount::Unknown => {
            let OwnedBuffer::Bytes(bitmap) = validity else {
                if matches!(array.null_count, NullCount::Known(n) if n > 0) {
                    return Err(PyTypeError::new_err(
                        "Arrow binary-view validity bitmap is required when null_count > 0",
                    ));
                }
                return Ok(true);
            };
            if bitmap.is_empty() {
                return Ok(true);
            }
            let bit = (array.offset % 8).checked_add(row).ok_or_else(|| {
                PyTypeError::new_err("Arrow binary-view validity index overflows")
            })?;
            let byte_index = bit / 8;
            let byte = bitmap.get(byte_index).copied().ok_or_else(|| {
                PyTypeError::new_err("Arrow binary-view validity bitmap is shorter than declared")
            })?;
            Ok((byte & (1 << (bit % 8))) != 0)
        },
    }
}
