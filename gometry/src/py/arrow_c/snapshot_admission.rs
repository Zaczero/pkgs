//! Copy the logical Arrow window before validating owned buffers.

use std::collections::BTreeMap;
use std::ffi::c_void;
use std::ops::Range;
#[cfg(test)]
use std::ptr;

use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::{ArrayData, BufferSpec, layout};
use arrow_schema::{DataType, UnionMode};

use super::ArrowArray;
use super::native::NativeArrowImportError;
use super::native::NativeArrowImportError::{Overflow, Type};

type Result<T> = std::result::Result<T, NativeArrowImportError>;

fn malformed(message: impl Into<String>) -> NativeArrowImportError {
    NativeArrowImportError::geoarrow(message)
}

fn span(start: usize, length: usize, width: usize) -> Result<Range<usize>> {
    let start = start
        .checked_mul(width)
        .ok_or(Overflow("Arrow offset window length overflows"))?;
    let end = length
        .checked_mul(width)
        .and_then(|length| start.checked_add(length))
        .filter(|&end| isize::try_from(end).is_ok())
        .ok_or(Overflow("Arrow offset window length overflows"))?;
    Ok(start..end)
}

/// # Safety
///
/// The producer keeps each requested buffer span readable and quiescent.
unsafe fn copy_bytes(pointer: *const c_void, range: Range<usize>) -> Result<Buffer> {
    if range.is_empty() {
        return Ok(Buffer::from_vec(Vec::<u8>::new()));
    }
    if pointer.is_null() {
        return Err(Type("Arrow data buffer is null"));
    }
    // SAFETY: span checked the byte extent; the producer supplies its allocation.
    let bytes =
        unsafe { std::slice::from_raw_parts(pointer.cast::<u8>().add(range.start), range.len()) };
    Ok(Buffer::from_slice_ref(bytes))
}

fn offsets(bytes: &Buffer, width: usize) -> Result<Vec<usize>> {
    bytes
        .as_slice()
        .chunks_exact(width)
        .map(|value| {
            let signed = if width == 4 {
                i64::from(i32::from_ne_bytes(
                    value.try_into().expect("four-byte offset"),
                ))
            } else {
                i64::from_ne_bytes(value.try_into().expect("eight-byte offset"))
            };
            usize::try_from(signed).map_err(|_| malformed("Arrow offsets must be non-negative"))
        })
        .collect()
}

fn offset_buffer(values: &[usize], width: usize) -> Result<Buffer> {
    let mut output = Vec::with_capacity(span(0, values.len(), width)?.len());
    for &value in values {
        if width == 4 {
            output.extend(
                i32::try_from(value)
                    .map_err(|_| Overflow("Arrow offset exceeds i32"))?
                    .to_ne_bytes(),
            );
        } else {
            output.extend(
                i64::try_from(value)
                    .map_err(|_| Overflow("Arrow offset exceeds i64"))?
                    .to_ne_bytes(),
            );
        }
    }
    Ok(Buffer::from_vec(output))
}

/// # Safety
///
/// `root` and its schema-directed pointer tables stay live and quiescent.
/// Every buffer span selected by the logical offsets is readable. The schema
/// has already passed the bounded owned-schema capture.
pub(super) unsafe fn snapshot_array(
    root: *const ArrowArray,
    data_type: &DataType,
) -> Result<ArrayData> {
    // SAFETY: the caller retains the entire producer tree through capture.
    unsafe { capture(root, data_type, None) }
}

#[expect(
    clippy::wildcard_enum_match_arm,
    clippy::indexing_slicing,
    reason = "Arrow's layout owns primitive buffer widths; only nested and variable-size types need specialized capture"
)]
/// # Safety
///
/// The root owner retains every schema-directed pointer and selected buffer
/// span until this recursive capture returns.
unsafe fn capture(
    pointer: *const ArrowArray,
    data_type: &DataType,
    window: Option<Range<usize>>,
) -> Result<ArrayData> {
    if pointer.is_null() || !pointer.is_aligned() {
        return Err(malformed("Arrow child array pointer is null or unaligned"));
    }
    // SAFETY: the root owner pins this live array shell.
    let array = unsafe { &*pointer };
    let raw_length = usize::try_from(array.length)
        .map_err(|_| Type("Arrow array length is negative or too large"))?;
    let raw_offset = usize::try_from(array.offset)
        .map_err(|_| Type("Arrow array offset is negative or too large"))?;
    let requested = window.unwrap_or(0..raw_length);
    if requested.start > requested.end || requested.end > raw_length {
        return Err(malformed(
            "Arrow selected descendant span exceeds child length",
        ));
    }
    let offset = raw_offset
        .checked_add(requested.start)
        .ok_or(Overflow("Arrow array offset overflows"))?;
    let length = requested.len();
    let end = offset
        .checked_add(length)
        .ok_or(Overflow("Arrow array end overflows"))?;
    let null_count = if array.null_count == -1 {
        None
    } else {
        let count = usize::try_from(array.null_count)
            .map_err(|_| Type("Arrow array null_count is below -1"))?;
        if count > raw_length {
            return Err(Type("Arrow array null_count exceeds length"));
        }
        Some(count)
    };
    if matches!(data_type, DataType::FixedSizeBinary(width) | DataType::FixedSizeList(_, width) if *width < 0)
    {
        return Err(Type("Arrow fixed-size width is negative"));
    }
    let layout = layout(data_type);
    let n_buffers =
        usize::try_from(array.n_buffers).map_err(|_| Type("Arrow array n_buffers is negative"))?;
    let n_children = usize::try_from(array.n_children)
        .map_err(|_| Type("Arrow array n_children is negative"))?;
    let minimum = layout
        .buffers
        .len()
        .saturating_add(usize::from(layout.can_contain_null_mask))
        .saturating_add(usize::from(layout.variadic));
    if (layout.variadic && n_buffers < minimum) || (!layout.variadic && n_buffers != minimum) {
        return Err(Type("Arrow array buffer count does not match schema"));
    }
    let fields: Vec<&DataType> = match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => vec![field.data_type()],
        DataType::Struct(fields) => fields.iter().map(|field| field.data_type()).collect(),
        DataType::Union(fields, _) => fields.iter().map(|(_, field)| field.data_type()).collect(),
        DataType::RunEndEncoded(ends, values) => vec![ends.data_type(), values.data_type()],
        _ => Vec::new(),
    };
    if n_children < fields.len() {
        return Err(malformed(
            "Arrow array n_children is smaller than schema child count",
        ));
    }
    if n_children != fields.len() {
        return Err(Type("Arrow array n_children does not match schema"));
    }
    span(0, n_buffers, size_of::<*const c_void>())?;
    span(0, n_children, size_of::<*mut ArrowArray>())?;
    if n_children != 0 && (array.children.is_null() || !array.children.is_aligned()) {
        return Err(malformed("Arrow array child table is null or unaligned"));
    }
    let children = if n_children == 0 {
        &[][..]
    } else {
        // SAFETY: the producer supplies the checked child-table span.
        unsafe { std::slice::from_raw_parts(array.children, n_children) }
    };
    if children.iter().any(|child| child.is_null()) {
        return Err(malformed("Arrow child array pointer is null"));
    }
    if n_buffers != 0 && (array.buffers.is_null() || !array.buffers.is_aligned()) {
        return Err(Type("Arrow array buffer table is null or unaligned"));
    }
    let buffers = if n_buffers == 0 {
        &[][..]
    } else {
        // SAFETY: the producer supplies the checked pointer-table span.
        unsafe { std::slice::from_raw_parts(array.buffers, n_buffers) }
    };
    let nulls = if layout.can_contain_null_mask {
        let validity = *buffers.first().expect("validated validity slot");
        if validity.is_null() {
            if null_count.is_some_and(|count| count != 0) {
                return Err(malformed("Arrow null_count requires a validity bitmap"));
            }
            None
        } else {
            let last_byte = end
                .checked_add(7)
                .ok_or(Overflow("Arrow bitmap span overflows"))?
                / 8;
            // SAFETY: only the selected bitmap bytes are requested.
            let bytes = unsafe {
                copy_bytes(
                    validity,
                    span(offset / 8, last_byte.saturating_sub(offset / 8), 1)?,
                )
            }?;
            let bits = BooleanBuffer::new(bytes, offset % 8, length);
            let actual = length.saturating_sub(bits.count_set_bits());
            if requested == (0..raw_length) && null_count.is_some_and(|count| count != actual) {
                return Err(malformed(format!(
                    "Arrow null_count ({}) does not match validity bitmap ({actual} null rows)",
                    array.null_count
                )));
            }
            Some(NullBuffer::new(BooleanBuffer::collect_bool(
                length,
                |index| bits.value(index),
            )))
        }
    } else {
        None
    };
    let mut owned_buffers = Vec::new();
    let mut owned_children = Vec::new();
    let offset_width = match data_type {
        DataType::Binary | DataType::Utf8 | DataType::List(_) | DataType::Map(..) => Some(4),
        DataType::LargeBinary | DataType::LargeUtf8 | DataType::LargeList(_) => Some(8),
        _ => None,
    };
    if let Some(width) = offset_width {
        let count = length
            .checked_add(1)
            .ok_or(Overflow("Arrow offset window length overflows"))?;
        // SAFETY: the producer supplies the selected offset window, not unused descendants.
        let bytes = unsafe {
            copy_bytes(
                *buffers.get(1).expect("offset slot"),
                span(offset, count, width)?,
            )
        }?;
        let values = offsets(&bytes, width)?;
        if values
            .array_windows::<2>()
            .any(|[left, right]| left > right)
        {
            return Err(malformed("Arrow offsets must be ordered"));
        }
        let first = *values.first().expect("one terminal offset");
        let last = *values.last().expect("one terminal offset");
        let rebased: Vec<_> = values
            .into_iter()
            .map(|value| value.wrapping_sub(first))
            .collect();
        owned_buffers.push(offset_buffer(&rebased, width)?);
        if let Some(child_type) = fields.first() {
            // SAFETY: the schema-directed child pointer and selected offsets are retained.
            owned_children.push(unsafe {
                capture(
                    *children.first().expect("list child"),
                    child_type,
                    Some(first..last),
                )
            }?);
        } else {
            // SAFETY: the producer supplies the selected values interval.
            owned_buffers.push(unsafe {
                copy_bytes(
                    *buffers.get(2).expect("values slot"),
                    span(first, last.wrapping_sub(first), 1)?,
                )
            }?);
        }
    } else if layout.variadic {
        // SAFETY: the producer supplies the selected view descriptors and referenced data.
        owned_buffers = unsafe { capture_views(buffers, offset, length) }?;
    } else {
        let base = usize::from(layout.can_contain_null_mask);
        for (index, spec) in layout.buffers.iter().enumerate() {
            let pointer = *buffers
                .get(base.saturating_add(index))
                .expect("validated buffer slot");
            let bytes = match spec {
                BufferSpec::FixedWidth { byte_width, .. } => {
                    // SAFETY: the producer supplies this selected fixed-width window.
                    unsafe { copy_bytes(pointer, span(offset, length, *byte_width)?) }?
                },
                BufferSpec::BitMap => {
                    let last = end
                        .checked_add(7)
                        .ok_or(Overflow("Arrow bitmap span overflows"))?
                        / 8;
                    // SAFETY: the producer supplies the selected bitmap window.
                    let bytes = unsafe {
                        copy_bytes(
                            pointer,
                            span(offset / 8, last.saturating_sub(offset / 8), 1)?,
                        )
                    }?;
                    let bits = BooleanBuffer::new(bytes, offset % 8, length);
                    BooleanBuffer::collect_bool(length, |index| bits.value(index)).into_inner()
                },
                BufferSpec::VariableWidth | BufferSpec::AlwaysNull => {
                    unreachable!("variable buffers have specialized capture")
                },
            };
            owned_buffers.push(bytes);
        }
        match data_type {
            DataType::Struct(_) | DataType::Union(_, UnionMode::Sparse) => {
                if let DataType::Union(fields, _) = data_type
                    && owned_buffers
                        .first()
                        .expect("union IDs")
                        .as_slice()
                        .iter()
                        .any(|&id| {
                            !fields
                                .iter()
                                .any(|(candidate, _)| candidate == i8::from_ne_bytes([id]))
                        })
                {
                    return Err(malformed("Arrow union type ID is not in its schema"));
                }
                for (&child, child_type) in children.iter().zip(&fields) {
                    // SAFETY: struct and sparse-union children share the selected row window.
                    owned_children.push(unsafe { capture(child, child_type, Some(offset..end)) }?);
                }
            },
            DataType::FixedSizeList(_, width) => {
                let width = usize::try_from(*width).map_err(|_| Type("negative list width"))?;
                let range = span(offset, length, width)?;
                // SAFETY: list children cover the selected parent rows times their fixed width.
                owned_children.push(unsafe {
                    capture(
                        *children.first().expect("list child"),
                        fields[0],
                        Some(range),
                    )
                }?);
            },
            DataType::ListView(_) | DataType::LargeListView(_) => {
                let width = if matches!(data_type, DataType::ListView(_)) {
                    4
                } else {
                    8
                };
                let mut starts = offsets(owned_buffers.first().expect("view offsets"), width)?;
                let sizes = offsets(owned_buffers.get(1).expect("view sizes"), width)?;
                let low = starts.iter().copied().min().unwrap_or(0);
                let high = starts
                    .iter()
                    .zip(&sizes)
                    .try_fold(low, |high, (&start, &size)| {
                        start
                            .checked_add(size)
                            .map(|end| high.max(end))
                            .ok_or(Overflow("Arrow list-view range overflows"))
                    })?;
                for start in &mut starts {
                    *start = start.wrapping_sub(low);
                }
                *owned_buffers.first_mut().expect("view offsets") = offset_buffer(&starts, width)?;
                // SAFETY: the owned offsets select this child interval.
                owned_children.push(unsafe {
                    capture(
                        *children.first().expect("list-view child"),
                        fields.first().expect("list-view field"),
                        Some(low..high),
                    )
                }?);
            },
            DataType::Union(union, UnionMode::Dense) => {
                let ids = owned_buffers.first().expect("union IDs").as_slice();
                let mut positions = offsets(owned_buffers.get(1).expect("union offsets"), 4)?;
                let mut ranges = vec![None::<Range<usize>>; fields.len()];
                for (&id, &position) in ids.iter().zip(&positions) {
                    let index = union
                        .iter()
                        .position(|(candidate, _)| candidate == i8::from_ne_bytes([id]))
                        .ok_or_else(|| malformed("Arrow union type ID is not in its schema"))?;
                    let end = position
                        .checked_add(1)
                        .ok_or(Overflow("Arrow union offset overflows"))?;
                    let range = ranges.get_mut(index).expect("union field");
                    *range = Some(range.take().map_or(position..end, |previous| {
                        previous.start.min(position)..previous.end.max(end)
                    }));
                }
                for (position, &id) in positions.iter_mut().zip(ids) {
                    let index = union
                        .iter()
                        .position(|(candidate, _)| candidate == i8::from_ne_bytes([id]))
                        .expect("validated union ID");
                    *position = position
                        .wrapping_sub(ranges[index].as_ref().expect("selected union child").start);
                }
                *owned_buffers.get_mut(1).expect("union offsets") = offset_buffer(&positions, 4)?;
                for ((&child, child_type), range) in children.iter().zip(&fields).zip(ranges) {
                    // SAFETY: the union's selected offsets define each child window.
                    owned_children
                        .push(unsafe { capture(child, child_type, Some(range.unwrap_or(0..0))) }?);
                }
            },
            DataType::RunEndEncoded(run_type, value_type) => {
                let &[runs_child, values_child]: &[_; 2] =
                    children.try_into().expect("validated run-end child count");
                // SAFETY: run ends define the mapping from logical rows into physical values.
                let runs = unsafe { capture(runs_child, run_type.data_type(), None) }?;
                let ends: Vec<usize> = match runs.data_type() {
                    DataType::Int16 => runs
                        .buffer::<i16>(0)
                        .iter()
                        .map(|&end| usize::try_from(end).map_err(|_| malformed("negative run end")))
                        .collect::<Result<_>>()?,
                    DataType::Int32 => runs
                        .buffer::<i32>(0)
                        .iter()
                        .map(|&end| usize::try_from(end).map_err(|_| malformed("negative run end")))
                        .collect::<Result<_>>()?,
                    DataType::Int64 => runs
                        .buffer::<i64>(0)
                        .iter()
                        .map(|&end| usize::try_from(end).map_err(|_| malformed("negative run end")))
                        .collect::<Result<_>>()?,
                    _ => return Err(Type("invalid Arrow run-end type")),
                };
                if ends.first().is_some_and(|&end| end == 0)
                    || ends.array_windows::<2>().any(|[left, right]| left >= right)
                {
                    return Err(malformed("Arrow run ends must be positive and ordered"));
                }
                if length != 0 && ends.last().is_none_or(|&last| last < end) {
                    return Err(malformed("Arrow run ends do not cover selected rows"));
                }
                let low = ends.partition_point(|&end| end <= offset);
                let high = if length == 0 {
                    low
                } else {
                    ends.partition_point(|&run_end| run_end < end)
                        .saturating_add(1)
                };
                let selected = ends
                    .get(low..high)
                    .ok_or_else(|| malformed("invalid run-end window"))?;
                let values: Vec<_> = selected
                    .iter()
                    .map(|&run_end| run_end.min(end).wrapping_sub(offset))
                    .collect();
                let run_buffer = if matches!(runs.data_type(), DataType::Int16) {
                    Buffer::from_vec(
                        values
                            .iter()
                            .map(|&value| {
                                i16::try_from(value).map_err(|_| Overflow("run end exceeds i16"))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )
                } else {
                    offset_buffer(
                        &values,
                        if matches!(runs.data_type(), DataType::Int32) {
                            4
                        } else {
                            8
                        },
                    )?
                };
                owned_children.push(
                    ArrayData::builder(runs.data_type().clone())
                        .len(values.len())
                        .buffers(vec![run_buffer])
                        .build()
                        .map_err(|error| malformed(error.to_string()))?,
                );
                // SAFETY: one physical value corresponds to each selected run.
                owned_children.push(unsafe {
                    capture(values_child, value_type.data_type(), Some(low..high))
                }?);
            },
            _ => {},
        }
    }
    match data_type {
        DataType::Dictionary(_, values) => {
            // SAFETY: a dictionary is an owned child of the producer array.
            owned_children.push(unsafe { capture(array.dictionary, values, None) }?);
        },
        _ if !array.dictionary.is_null() => return Err(Type("unexpected Arrow dictionary array")),
        _ => {},
    }
    ArrayData::builder(data_type.clone())
        .len(length)
        .nulls(nulls)
        .buffers(owned_buffers)
        .child_data(owned_children)
        .align_buffers(true)
        .build()
        .map_err(|error| malformed(error.to_string()))
}

/// # Safety
///
/// The producer supplies the descriptor window, size table and referenced data.
#[expect(
    clippy::indexing_slicing,
    reason = "descriptor chunks are exactly 16 bytes and header admission established the buffer-table bounds"
)]
unsafe fn capture_views(
    buffers: &[*const c_void],
    offset: usize,
    length: usize,
) -> Result<Vec<Buffer>> {
    // SAFETY: the caller retains the selected descriptor span.
    let views = unsafe { copy_bytes(buffers[1], span(offset, length, 16)?) }?;
    let mut descriptors = views.as_slice().to_vec();
    let count = buffers.len().wrapping_sub(3);
    let sizes = *buffers.last().expect("view sizes table");
    span(0, count, 8)?;
    let mut ranges: BTreeMap<usize, Range<usize>> = BTreeMap::new();
    for descriptor in descriptors.as_chunks_mut::<16>().0 {
        let size = i32::from_ne_bytes(descriptor[..4].try_into().expect("view length"));
        if size < 0 {
            return Err(malformed("negative Arrow view length"));
        }
        if size <= 12 {
            continue;
        }
        let buffer_index =
            u32::from_ne_bytes(descriptor[8..12].try_into().expect("view buffer index")) as usize;
        let start = u32::from_ne_bytes(descriptor[12..].try_into().expect("view offset")) as usize;
        if buffer_index >= count {
            return Err(malformed("Arrow view buffer index is out of range"));
        }
        if sizes.is_null() {
            return Err(Type("Arrow view buffer-size table is null"));
        }
        let size_offset = span(buffer_index, 1, 8)?.start;
        // SAFETY: the selected index is within the producer's checked size-table span.
        let buffer_size = unsafe {
            sizes
                .cast::<u8>()
                .add(size_offset)
                .cast::<i64>()
                .read_unaligned()
        };
        let available = usize::try_from(buffer_size)
            .map_err(|_| malformed("negative Arrow view buffer size"))?;
        let range = span(
            start,
            usize::try_from(size).expect("positive view length"),
            1,
        )?;
        if range.end > available {
            return Err(malformed("Arrow view exceeds its data buffer"));
        }
        ranges
            .entry(buffer_index)
            .and_modify(|previous| {
                previous.start = previous.start.min(range.start);
                previous.end = previous.end.max(range.end);
            })
            .or_insert(range);
    }
    let mut owned = Vec::with_capacity(ranges.len().saturating_add(1));
    let mut remap = BTreeMap::new();
    for (index, (&buffer_index, range)) in ranges.iter().enumerate() {
        // SAFETY: all selected descriptor spans were checked against this buffer's size.
        owned.push(unsafe { copy_bytes(buffers[buffer_index.saturating_add(2)], range.clone()) }?);
        remap.insert(
            buffer_index,
            (
                u32::try_from(index).map_err(|_| Overflow("too many Arrow view buffers"))?,
                range.start,
            ),
        );
    }
    for descriptor in descriptors.as_chunks_mut::<16>().0 {
        if i32::from_ne_bytes(descriptor[..4].try_into().expect("view length")) <= 12 {
            continue;
        }
        let old =
            u32::from_ne_bytes(descriptor[8..12].try_into().expect("view buffer index")) as usize;
        let offset = u32::from_ne_bytes(descriptor[12..].try_into().expect("view offset"));
        let &(index, start) = remap.get(&old).expect("selected view buffer");
        descriptor[8..12].copy_from_slice(&index.to_ne_bytes());
        descriptor[12..].copy_from_slice(
            &offset
                .wrapping_sub(u32::try_from(start).expect("view offset is u32"))
                .to_ne_bytes(),
        );
    }
    owned.insert(0, Buffer::from_vec(descriptors));
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shell(buffers: &[*const c_void], length: i64, null_count: i64) -> ArrowArray {
        ArrowArray {
            length,
            null_count,
            offset: 0,
            n_buffers: i64::try_from(buffers.len()).expect("small buffer count"),
            n_children: 0,
            buffers: buffers.as_ptr(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }

    #[test]
    fn rejects_inconsistent_null_count_before_import() {
        let validity = [1_u8];
        let value = [42_i64];
        let buffers = [validity.as_ptr().cast(), value.as_ptr().cast()];
        let array = shell(&buffers, 1, 1);
        // SAFETY: the declared buffers remain live.
        let error = unsafe { snapshot_array(&raw const array, &DataType::Int64) }
            .expect_err("lying null count");
        assert!(matches!(error, NativeArrowImportError::GeoArrow(_)));
    }

    #[test]
    fn rejects_unrepresentable_offsets_before_reading_buffers() {
        let buffers = [ptr::null(); 3];
        let array = shell(&buffers, i64::MAX, 0);
        // SAFETY: the table is live; the unrepresentable span is rejected before data access.
        let error = unsafe { snapshot_array(&raw const array, &DataType::Binary) }
            .expect_err("unrepresentable offsets");
        assert!(matches!(error, Overflow(_)));
    }

    #[test]
    fn empty_primitive_snapshot_has_aligned_owned_storage() {
        let buffers = [ptr::null(); 2];
        let array = shell(&buffers, 0, 0);
        // SAFETY: zero selected rows require no data buffers.
        let result = unsafe { snapshot_array(&raw const array, &DataType::Float64) }
            .expect("empty float snapshot");
        result.validate_full().expect("aligned owned buffers");
        assert!(result.is_empty());
    }

    #[test]
    fn nested_capture_ignores_offsets_outside_selected_descendant_span() {
        use std::sync::Arc;

        use arrow_schema::Field;

        let values = b"abcdef";
        let child_offsets = [0_i32, 4, 2, 6];
        let child_buffers = [
            ptr::null(),
            child_offsets.as_ptr().cast(),
            values.as_ptr().cast(),
        ];
        let child = shell(&child_buffers, 3, 0);
        let children = [ptr::from_ref(&child).cast_mut()];
        let parent_offsets = [0_i32, 2, 3];
        let parent_buffers = [ptr::null(), parent_offsets.as_ptr().cast()];
        let mut parent = shell(&parent_buffers, 1, 0);
        parent.offset = 1;
        parent.n_children = 1;
        parent.children = children.as_ptr().cast_mut();
        let dtype = DataType::List(Arc::new(Field::new("item", DataType::Binary, true)));
        // SAFETY: every selected buffer span and child pointer stays live.
        let result = unsafe { snapshot_array(&raw const parent, &dtype) }.expect("selected span");
        let child = result.child_data().first().expect("selected child");
        assert_eq!(child.len(), 1);
        assert_eq!(child.buffer::<i32>(0), &[0, 4]);
        assert_eq!(child.buffers().get(1).expect("values").as_slice(), b"cdef");
    }
}
