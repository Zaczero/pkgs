use super::buffers::{OwnedBuffer, OwnedBufferSlots, snapshot_native_buffers};
use super::layout::{ArrowFormat, NullCount};
use super::*;

impl NativeArrowClassification {
    pub(crate) fn new(
        encoding: GeometryEncoding,
        wkb_offset_width: WkbOffsetWidth,
        crs: Option<String>,
        epoch: Option<f64>,
    ) -> Self {
        Self {
            encoding,
            wkb_offset_width,
            crs: crs.map(arrow_c::arc_from_string),
            epoch,
        }
    }

    pub(crate) fn from_schema(classified: &ClassifiedGeometrySchema) -> (Self, Option<usize>) {
        (
            Self {
                encoding: classified.encoding,
                wkb_offset_width: classified.wkb_offset_width,
                crs: classified.crs.as_deref().map(arc_from_str),
                epoch: classified.epoch,
            },
            classified.struct_child,
        )
    }
}

impl NativeNode {
    pub(super) fn child(&self, index: usize) -> Option<&Self> {
        self.children.get(index)
    }

    pub(super) fn missing_at(&self, index: usize) -> bool {
        debug_assert!(self.validity_offset < 8);
        self.effective_missing
            .as_ref()
            .is_some_and(|missing| missing.is_missing(index))
    }

    pub(super) fn missing_count(&self) -> usize {
        self.effective_missing
            .as_ref()
            .map_or(0, storage::ValidityMask::missing_count)
    }

    pub(super) fn present_count(&self) -> usize {
        self.effective_missing
            .as_ref()
            .map_or(self.length, storage::ValidityMask::present_count)
    }

    pub(super) fn bytes_at(&self, index: usize) -> Option<&[u8]> {
        if index >= self.n_buffers {
            return None;
        }
        match self.buffers.get(index)? {
            OwnedBuffer::Bytes(bytes) => Some(bytes.as_slice()),
            OwnedBuffer::F64(_) => None,
        }
    }

    #[expect(
        clippy::rc_buffer,
        reason = "the sharing handle must remain Arc<Vec<T>> so capacity stays part of heap accounting"
    )]
    pub(super) fn f64_values(&self) -> Option<&Arc<Vec<f64>>> {
        match self.buffers.get(1) {
            Some(OwnedBuffer::F64(values)) => Some(values),
            _ => None,
        }
    }

    pub(super) fn offset_at(&self, index: usize) -> Option<usize> {
        let width = self.format.offset_width()?;
        let start = index.checked_mul(width)?;
        let end = start.checked_add(width)?;
        let bytes = self.bytes_at(1)?.get(start..end)?;
        let value = if width == 4 {
            let bytes = bytes.first_chunk::<4>()?;
            i64::from(i32::from_le_bytes(*bytes))
        } else {
            let bytes = bytes.first_chunk::<8>()?;
            i64::from_le_bytes(*bytes)
        };
        usize::try_from(value).ok()
    }

    pub(super) fn list_range(&self, index: usize) -> Option<std::ops::Range<usize>> {
        if !matches!(self.format, ArrowFormat::List | ArrowFormat::LargeList) {
            return None;
        }
        let end_index = index.checked_add(1)?;
        if end_index > self.length {
            return None;
        }
        let start = self.offset_at(index)?;
        let end = self.offset_at(end_index)?;
        if start > end {
            return None;
        }
        let child_len = self.children.first().map_or(0, |child| child.length);
        if end > child_len {
            return None;
        }
        Some(start..end)
    }
}

/// Recursively snapshot one array node and its children while the array shell
/// is live; formats come from the already-owned schema tree.
pub(super) fn build_owned_node_admitted(
    validated: &mut CaptureLayout,
    schema: &AdmittedArrowSchema,
    effective_missing: Option<storage::ValidityMask>,
) -> PyResult<NativeNode> {
    let buffers = snapshot_native_buffers(validated)?;
    // Own-row nulls from owned validity, OR'd with any ancestor mask.
    let effective_missing = merge_owned_row_missing(validated, &buffers, effective_missing)?;
    let mut field_names = Vec::with_capacity(validated.n_children);
    let mut children = Vec::with_capacity(validated.n_children);
    for (index, child_schema) in schema.children.iter().enumerate() {
        let name = child_schema.name().to_owned();
        field_names.push(name);
        // SAFETY: children live under capture shell for this construction.
        let child_array_ptr = unsafe { validated.child_ptr(index)? };
        let child_format = ArrowFormat::parse(child_schema.format())?;
        let child_schema_n = child_schema.children.len();
        let is_struct_field = matches!(validated.format, ArrowFormat::Struct);
        // SAFETY: same capture shell as parent.
        let mut child_validated = unsafe {
            if is_struct_field {
                CaptureLayout::struct_child(
                    validated.shell,
                    child_array_ptr,
                    child_format,
                    validated.offset,
                    validated.length,
                    child_schema_n,
                )?
            } else if matches!(validated.format, ArrowFormat::List | ArrowFormat::LargeList) {
                let (child_offset, child_length) = validated.child_window.ok_or_else(|| {
                    PyTypeError::new_err(
                        "Arrow list offsets were not captured before child admission",
                    )
                })?;
                CaptureLayout::struct_child(
                    validated.shell,
                    child_array_ptr,
                    child_format,
                    child_offset,
                    child_length,
                    child_schema_n,
                )?
            } else if let ArrowFormat::FixedSizeList(size) = validated.format {
                let child_offset = validated.offset.checked_mul(size).ok_or_else(|| {
                    PyTypeError::new_err("Arrow fixed-size-list child offset overflows")
                })?;
                let child_length = validated.length.checked_mul(size).ok_or_else(|| {
                    PyTypeError::new_err("Arrow fixed-size-list child length overflows")
                })?;
                CaptureLayout::struct_child(
                    validated.shell,
                    child_array_ptr,
                    child_format,
                    child_offset,
                    child_length,
                    child_schema_n,
                )?
            } else {
                CaptureLayout::new(
                    validated.shell,
                    child_array_ptr,
                    child_format,
                    child_schema_n,
                )?
            }
        };
        // Project the parent-effective mask onto every child carrier. A
        // Struct maps rows 1:1; list and fixed-size-list parents expand a
        // missing outer row to its selected child range. Child-own nulls
        // merge only after the child's snapshot in the recursive call.
        let child_missing = project_ancestor_missing(
            validated,
            &buffers,
            effective_missing.as_ref(),
            child_validated.length,
        )?;
        children.push(build_owned_node_admitted(
            &mut child_validated,
            child_schema,
            child_missing,
        )?);
    }
    Ok(NativeNode {
        length: validated.length,
        validity_offset: validated.offset % 8,
        n_buffers: validated.n_buffers,
        format: validated.format,
        buffers,
        children: arc_from_vec(children),
        schema_name: arrow_c::arc_from_str(schema.name()),
        field_names: arc_from_vec(field_names),
        effective_missing,
    })
}

/// Project an already-owned effective parent mask onto its selected child.
///
/// The relation is structural, not a Struct special case: Struct preserves a
/// row index, variable lists expand it through their rebased owned offsets,
/// and fixed-size lists expand it through their declared width.  Child-own
/// nulls merge later via [`merge_owned_row_missing`] after the child has been
/// snapshotted — never through producer memory.
pub(super) fn project_ancestor_missing(
    parent: &CaptureLayout,
    parent_buffers: &OwnedBufferSlots,
    parent_effective: Option<&storage::ValidityMask>,
    child_length: usize,
) -> PyResult<Option<storage::ValidityMask>> {
    let Some(parent_missing) = parent_effective else {
        return Ok(None);
    };
    if parent_missing.len() != parent.length {
        return Err(PyTypeError::new_err(
            "Arrow effective parent missing length does not match parent",
        ));
    }
    match parent.format {
        ArrowFormat::Struct => {
            if child_length != parent.length {
                return Err(PyTypeError::new_err(
                    "Arrow struct child visible length does not match parent",
                ));
            }
            Ok(Some(parent_missing.clone()))
        },
        ArrowFormat::List | ArrowFormat::LargeList => {
            let OwnedBuffer::Bytes(offsets) = parent_buffers.get(1).ok_or_else(|| {
                PyTypeError::new_err("Arrow list offsets are required for child mask projection")
            })?
            else {
                return Err(PyTypeError::new_err(
                    "Arrow list offsets must be owned bytes for child mask projection",
                ));
            };
            let width = parent
                .format
                .offset_width()
                .expect("list formats have an offset width");
            let read_offset = |row: usize| -> PyResult<usize> {
                let start = row
                    .checked_mul(width)
                    .ok_or_else(|| PyTypeError::new_err("Arrow list offset index overflows"))?;
                let end = start
                    .checked_add(width)
                    .ok_or_else(|| PyTypeError::new_err("Arrow list offset index overflows"))?;
                let raw = offsets.get(start..end).ok_or_else(|| {
                    PyTypeError::new_err("Arrow list offsets are shorter than declared")
                })?;
                let value = if width == 4 {
                    let raw = raw
                        .first_chunk::<4>()
                        .ok_or_else(|| PyTypeError::new_err("Arrow list offset is truncated"))?;
                    i64::from(i32::from_le_bytes(*raw))
                } else {
                    let raw = raw.first_chunk::<8>().ok_or_else(|| {
                        PyTypeError::new_err("Arrow list offset width is invalid")
                    })?;
                    i64::from_le_bytes(*raw)
                };
                usize::try_from(value)
                    .map_err(|_| arrow::geoarrow_parse_error("Arrow offsets must be non-negative"))
            };
            let mut missing = storage::ValidityBuilder::with_capacity(child_length);
            for (row, is_missing) in parent_missing.iter().enumerate() {
                if !is_missing {
                    continue;
                }
                let start = read_offset(row)?;
                let end = read_offset(row.wrapping_add(1))?;
                if end < start {
                    return Err(arrow::geoarrow_parse_error(
                        "Arrow list offsets must be ordered",
                    ));
                }
                if start < missing.len() || end > child_length {
                    return Err(arrow::geoarrow_parse_error(
                        "Arrow list child offsets exceed the selected child length",
                    ));
                }
                missing.extend_present(start.wrapping_sub(missing.len()));
                missing.extend_missing(end.wrapping_sub(start));
            }
            missing.extend_present(child_length.wrapping_sub(missing.len()));
            Ok(missing.finish())
        },
        ArrowFormat::FixedSizeList(width) => {
            let expected = parent.length.checked_mul(width).ok_or_else(|| {
                PyTypeError::new_err("Arrow fixed-size-list child length overflows")
            })?;
            if child_length != expected {
                return Err(PyTypeError::new_err(
                    "Arrow fixed-size-list child visible length does not match parent",
                ));
            }
            let mut missing = storage::ValidityBuilder::with_capacity(child_length);
            for is_missing in parent_missing.iter() {
                if is_missing {
                    missing.extend_missing(width);
                } else {
                    missing.extend_present(width);
                }
            }
            Ok(missing.finish())
        },
        ArrowFormat::Binary
        | ArrowFormat::LargeBinary
        | ArrowFormat::BinaryView
        | ArrowFormat::Float64 => Ok(None),
    }
}

/// OR this node's owned validity nulls into an ancestor missing mask.
/// Runs only after [`snapshot_native_buffers`] so content is owned.
pub(super) fn merge_owned_row_missing(
    array: &CaptureLayout,
    buffers: &OwnedBufferSlots,
    ancestor: Option<storage::ValidityMask>,
) -> PyResult<Option<storage::ValidityMask>> {
    let len = array.length;
    if ancestor.as_ref().is_some_and(|mask| mask.len() != len) {
        return Err(PyTypeError::new_err(
            "Arrow ancestor missing length does not match child length",
        ));
    }
    if matches!(array.null_count, NullCount::Known(0)) {
        return Ok(ancestor);
    }
    let Some(OwnedBuffer::Bytes(bitmap)) = buffers.get(0) else {
        if matches!(array.null_count, NullCount::Known(n) if n > 0) {
            return Err(PyTypeError::new_err(
                "Arrow validity bitmap is required when null_count > 0",
            ));
        }
        return Ok(ancestor);
    };
    if bitmap.is_empty() {
        return Ok(ancestor);
    }
    let bit_offset = array.offset % 8;
    let end = bit_offset
        .checked_add(len)
        .ok_or_else(|| PyTypeError::new_err("Arrow validity index overflows"))?;
    if end.div_ceil(8) > bitmap.len() {
        return Err(PyTypeError::new_err(
            "Arrow validity bitmap is shorter than declared",
        ));
    }
    let own = storage::ValidityMask::from_bitmap(
        crate::geometry::SharedSlice::from_arc_vec(Arc::clone(bitmap)),
        bit_offset,
        len,
    );
    Ok(union_many([ancestor.as_ref(), own.as_ref()]))
}

pub(super) fn owned_or_raw_row_valid(
    array: &CaptureLayout,
    buffers: &OwnedBufferSlots,
    row: usize,
) -> PyResult<bool> {
    match array.null_count {
        NullCount::Known(0) => Ok(true),
        NullCount::Known(_) | NullCount::Unknown => {
            let Some(OwnedBuffer::Bytes(bitmap)) = buffers.get(0) else {
                if matches!(array.null_count, NullCount::Known(n) if n > 0) {
                    return Err(PyTypeError::new_err(
                        "Arrow validity bitmap is required when null_count > 0",
                    ));
                }
                return Ok(true);
            };
            if bitmap.is_empty() {
                return Ok(true);
            }
            let bit = array
                .offset
                .rem_euclid(8)
                .checked_add(row)
                .ok_or_else(|| PyTypeError::new_err("Arrow validity index overflows"))?;
            let byte_index = bit / 8;
            let byte = bitmap.get(byte_index).copied().ok_or_else(|| {
                PyTypeError::new_err("Arrow validity bitmap is shorter than declared")
            })?;
            Ok((byte & (1 << (bit % 8))) != 0)
        },
    }
}
