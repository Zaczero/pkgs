#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::mem::size_of;
use std::ptr;

use crate::geometry::SharedSlice;
use crate::py::arrow_c::{
    ArrayPrivate, ArrowArray, ArrowSchema, CoordSeq, CoordinateAxes, GeometryEncoding,
    GeometryError, Map, NativeArrowExportError, NativeArrowExportResult, Number, PyResult,
    SchemaNode, SchemaPrivate, Value, release_array, release_schema,
};

impl SchemaNode {
    pub(crate) fn materialize_schema(&self) -> NativeArrowExportResult<GometryArrowSchema> {
        let Self {
            format,
            name,
            metadata,
            children,
        } = self;
        let mut owned_children = Vec::new();
        crate::try_reserve_vec(&mut owned_children, children.len())?;
        for child in children {
            owned_children.push(child.materialize_schema()?);
        }
        let children = owned_children;
        let mut child_ptrs = Vec::new();
        crate::try_reserve_vec(&mut child_ptrs, children.len())?;
        for child in &children {
            child_ptrs.push(ptr::from_ref(child.as_ref()).cast_mut());
        }
        let mut private = Box::try_new(SchemaPrivate {
            format: schema_text(format)?,
            name: schema_text(name)?,
            metadata: metadata
                .as_deref()
                .map(encode_schema_metadata)
                .transpose()?,
            children,
            child_ptrs,
        })
        .map_err(crate::error::Error::from)?;
        let mut schema = Box::try_new(ArrowSchema {
            format: private.format.as_ptr().cast(),
            name: private.name.as_ptr().cast(),
            metadata: private
                .metadata
                .as_ref()
                .map_or(ptr::null(), |metadata| metadata.as_ptr().cast()),
            // Arrow C Data Interface nullable flag (value 2) marks a nullable field. The
            // previous Python builders emitted nullable coordinate/list
            // fields, so retain that storage-type contract on capsule import.
            flags: 2,
            n_children: private.children.len() as i64,
            children: if private.child_ptrs.is_empty() {
                ptr::null_mut()
            } else {
                private.child_ptrs.as_mut_ptr()
            },
            dictionary: ptr::null_mut(),
            release: Some(release_schema),
            private_data: ptr::null_mut(),
        })
        .map_err(crate::error::Error::from)?;
        schema.private_data = Box::into_raw(private).cast();
        Ok(GometryArrowSchema(schema))
    }
}

pub(crate) struct GometryArrowSchema(Box<ArrowSchema>);

impl GometryArrowSchema {
    pub(crate) fn into_box(self) -> Box<ArrowSchema> {
        let this = std::mem::ManuallyDrop::new(self);
        // SAFETY: ownership leaves the guard exactly once; its destructor is disabled.
        unsafe { ptr::read(ptr::from_ref(&this.0)) }
    }

    pub(crate) fn as_ref(&self) -> &ArrowSchema {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn as_mut(&mut self) -> &mut ArrowSchema {
        &mut self.0
    }
}

impl Drop for GometryArrowSchema {
    fn drop(&mut self) {
        // SAFETY: the guard owns the shell and the callback is idempotent.
        unsafe {
            if let Some(release) = self.0.release {
                release(self.0.as_mut());
            }
        }
    }
}

fn schema_text(text: &str) -> NativeArrowExportResult<Vec<u8>> {
    if text.as_bytes().contains(&0) {
        return Err(NativeArrowExportError::geometry(
            "Arrow schema text contains a nul",
        ));
    }
    let mut bytes = Vec::new();
    let capacity = text
        .len()
        .checked_add(1)
        .ok_or_else(|| NativeArrowExportError::overflow("Arrow schema text is too large"))?;
    crate::try_reserve_vec(&mut bytes, capacity)?;
    bytes.extend_from_slice(text.as_bytes());
    bytes.push(0);
    Ok(bytes)
}

pub(crate) fn encode_schema_metadata(
    pairs: &[(String, String)],
) -> NativeArrowExportResult<Vec<u8>> {
    let pair_count = i32::try_from(pairs.len()).map_err(|_| {
        NativeArrowExportError::geometry("Arrow schema metadata has too many entries")
    })?;
    let mut capacity = size_of::<i32>();
    for (key, value) in pairs {
        i32::try_from(key.len()).map_err(|_| {
            NativeArrowExportError::geometry("Arrow schema metadata key is too large")
        })?;
        i32::try_from(value.len()).map_err(|_| {
            NativeArrowExportError::geometry("Arrow schema metadata value is too large")
        })?;
        capacity = capacity
            .checked_add(size_of::<i32>())
            .and_then(|size| size.checked_add(key.len()))
            .and_then(|size| size.checked_add(size_of::<i32>()))
            .and_then(|size| size.checked_add(value.len()))
            .ok_or_else(|| {
                NativeArrowExportError::geometry("Arrow schema metadata size overflows usize")
            })?;
    }
    let mut out = Vec::new();
    crate::try_reserve_vec(&mut out, capacity)?;
    out.extend_from_slice(&pair_count.to_ne_bytes());
    for (key, value) in pairs {
        out.extend_from_slice(&(key.len() as i32).to_ne_bytes());
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(&(value.len() as i32).to_ne_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    Ok(out)
}

/// One buffer slot whose raw pointer is derived from a retained owner.
///
/// A foreign or mismatched pointer cannot enter this type: every non-null
/// slot owns the exact allocation the published pointer addresses.
#[derive(Clone)]
pub(crate) enum OwnedBufferSlot {
    /// Null validity / empty slot (Arrow null buffer pointer).
    Null,
    F64(SharedSlice<f64>),
    I32(crate::geometry::SharedSlice<i32>),
    U8(crate::geometry::SharedSlice<u8>),
}

/// Gometry-built Arrow array shell. Only the builders in this module construct
/// it; foreign shells and ownerless raw-pointer pairs are unrepresentable.
#[repr(transparent)]
pub(crate) struct GometryArrowArray(Box<ArrowArray>);

impl GometryArrowArray {
    pub(crate) fn into_box(self) -> Box<ArrowArray> {
        let this = std::mem::ManuallyDrop::new(self);
        // SAFETY: ownership leaves the guard exactly once; its destructor is disabled.
        unsafe { ptr::read(ptr::from_ref(&this.0)) }
    }

    pub(crate) fn as_mut(&mut self) -> &mut ArrowArray {
        self.0.as_mut()
    }

    #[cfg(test)]
    fn logical_length(&self) -> PyResult<usize> {
        usize::try_from(self.0.length).map_err(|_| {
            GeometryError::new_err("Arrow geometry export length is negative or too large")
        })
    }
}

impl Drop for GometryArrowArray {
    fn drop(&mut self) {
        // SAFETY: the guard owns the live shell; the callback is idempotent.
        unsafe {
            if let Some(release) = self.0.release {
                release(self.0.as_mut());
            }
        }
    }
}

fn checked_i64(value: usize, what: &str) -> PyResult<i64> {
    i64::try_from(value)
        .map_err(|_| GeometryError::new_err(format!("Arrow geometry export {what} exceeds i64")))
}

/// Ensure logical `offset`/`length` cannot address past owned primitive or
/// offsets columns. `U8` data buffers are not length-indexed (binary uses
/// offset values into the byte buffer — validated separately).
fn ensure_owned_buffer_window(
    length: usize,
    offset: usize,
    buffers: &[OwnedBufferSlot],
) -> PyResult<()> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| GeometryError::new_err("Arrow geometry export offset+length overflows"))?;
    for slot in buffers {
        match slot {
            OwnedBufferSlot::Null | OwnedBufferSlot::U8(_) => {},
            OwnedBufferSlot::F64(values) => {
                if end > values.len() {
                    return Err(GeometryError::new_err(
                        "Arrow geometry export window exceeds owned f64 column",
                    ));
                }
            },
            // i32 slots in gometry export are offset columns: consumers read
            // offsets[offset..=offset+length], so index `end` must be in range.
            OwnedBufferSlot::I32(values) => {
                let in_range = if length == 0 {
                    !values.is_empty() && offset < values.len()
                } else {
                    end < values.len()
                };
                if !in_range {
                    return Err(GeometryError::new_err(
                        "Arrow geometry export window exceeds owned offsets column",
                    ));
                }
            },
        }
    }
    Ok(())
}

/// Build an Arrow array whose buffer pointers are derived from owner slots.
///
/// **Module-private:** typed builders (`primitive_f64_array_windowed`,
/// `list_array*`, `binary_array`, `coordinate_array`) are the only construction
/// paths. Length/offset are checked against owned F64/I32 capacities so a
/// free logical window over a short owner cannot publish OOB buffers.
fn make_array(
    length: usize,
    buffers: impl IntoIterator<Item = OwnedBufferSlot>,
    children: impl IntoIterator<Item = GometryArrowArray>,
) -> PyResult<GometryArrowArray> {
    make_array_with_offset(length, 0, buffers, children)
}

/// Arrow C Data Interface array with a non-zero logical `offset`.
///
/// Buffer pointers still address the start of each owned buffer; consumers
/// apply `offset` (the C Data Interface contract). Private data retains the
/// full `Arc` owners for the capsule lifetime — Window export uses this to
/// share parent coordinate/CSR storage without copying.
fn make_array_with_offset(
    length: usize,
    offset: usize,
    buffers: impl IntoIterator<Item = OwnedBufferSlot>,
    children: impl IntoIterator<Item = GometryArrowArray>,
) -> PyResult<GometryArrowArray> {
    let buffers = crate::try_collect_results(buffers.into_iter().map(Ok))?;
    let mut children = crate::try_collect_results(children.into_iter().map(Ok))?;
    ensure_owned_buffer_window(length, offset, &buffers)?;
    let n_buffers = buffers.len();
    let length_i64 = checked_i64(length, "length")?;
    let offset_i64 = checked_i64(offset, "offset")?;
    let n_buffers_i64 = checked_i64(n_buffers, "n_buffers")?;

    let mut buffer_ptrs = crate::try_vec_with_capacity(n_buffers)?;
    for slot in &buffers {
        buffer_ptrs.push(match slot {
            OwnedBufferSlot::Null => ptr::null(),
            OwnedBufferSlot::F64(values) => values.as_ptr().cast(),
            OwnedBufferSlot::I32(values) => values.as_ptr().cast(),
            OwnedBufferSlot::U8(values) => values.as_ptr().cast(),
        });
    }
    let n_children_i64 = checked_i64(children.len(), "n_children")?;
    let mut child_ptrs = crate::try_vec_with_capacity(children.len())?;
    for child in &mut children {
        child_ptrs.push(ptr::from_mut(child.as_mut()));
    }
    let mut private = Box::try_new(ArrayPrivate {
        owners: buffers,
        buffers: buffer_ptrs,
        _children: children,
        child_ptrs,
    })
    .map_err(crate::error::Error::from)?;
    let mut array = Box::try_new(ArrowArray {
        length: length_i64,
        null_count: 0,
        offset: offset_i64,
        n_buffers: n_buffers_i64,
        n_children: n_children_i64,
        buffers: private.buffers.as_ptr(),
        children: if private.child_ptrs.is_empty() {
            ptr::null_mut()
        } else {
            private.child_ptrs.as_mut_ptr()
        },
        dictionary: ptr::null_mut(),
        release: Some(release_array),
        private_data: ptr::null_mut(),
    })
    .map_err(crate::error::Error::from)?;
    array.private_data = Box::into_raw(private).cast();
    Ok(GometryArrowArray(array))
}

/// Attach a top-level validity bitmap to a **gometry-built** array shell.
///
/// Accepts only [`GometryArrowArray`] so a foreign shell cannot enter. Mask
/// length is checked in release builds (not debug-only).
#[cfg(test)]
pub(crate) fn apply_top_level_validity(
    array: &mut GometryArrowArray,
    mask: &crate::array::ValidityMask,
) -> PyResult<()> {
    let array = array.as_mut();
    let logical_len = usize::try_from(array.length).map_err(|_| {
        GeometryError::new_err("Arrow geometry export length is negative or too large")
    })?;
    if mask.len() != logical_len {
        return Err(GeometryError::new_err(
            "missing mask length must match logical Arrow array length",
        ));
    }
    let null_count = mask.missing_count();
    if null_count == 0 {
        return Ok(());
    }
    if array.n_buffers < 1 {
        return Err(GeometryError::new_err(
            "Arrow geometry export has no top-level validity buffer slot",
        ));
    }
    // Arrow C Data Interface: for a non-zero array.offset, validity bit for
    // logical index `i` is at physical bit `offset + i` (consumers apply the
    // offset). Windowed packed LineString/Polygon export retains parent CSR
    // via offset/length — the logical missing mask must be placed at that
    // offset, not at bit 0.
    let offset = usize::try_from(array.offset).map_err(|_| {
        GeometryError::new_err("Arrow geometry export offset is negative or too large")
    })?;
    let total_bits = offset.checked_add(mask.len()).ok_or_else(|| {
        GeometryError::new_err("Arrow geometry export validity span exceeds usize")
    })?;
    let (owned, mask_offset) = mask.arrow_bitmap();
    let owned_bits = owned.len().saturating_mul(8);
    let validity = if mask_offset == offset && owned_bits >= total_bits {
        owned
    } else {
        let mut validity = crate::try_vec_with_capacity(total_bits.div_ceil(8))?;
        validity.resize(total_bits.div_ceil(8), 0);
        for row in 0..mask.len() {
            if mask.is_valid(row) {
                let bit = offset + row;
                validity[bit / 8] |= 1 << (bit % 8);
            }
        }
        crate::geometry::SharedSlice::try_from_vec(validity)?
    };
    let private = array.private_data.cast::<ArrayPrivate>();
    if private.is_null() {
        return Err(GeometryError::new_err(
            "Arrow geometry export is missing private buffer ownership",
        ));
    }
    // SAFETY: `GometryArrowArray` always carries an `ArrayPrivate` box whose
    // `buffers` vector backs `array.buffers`. Replacing its validity owner
    // preserves both vector allocations and retains the published bytes.
    unsafe {
        let private = &mut *private;
        if private.buffers.is_empty() {
            return Err(GeometryError::new_err(
                "Arrow geometry export has no top-level validity buffer slot",
            ));
        }
        private.buffers[0] = validity.as_ptr().cast();
        private.owners[0] = OwnedBufferSlot::U8(validity);
        array.buffers = private.buffers.as_ptr();
    }
    array.null_count = i64::try_from(null_count)
        .map_err(|_| GeometryError::new_err("Arrow geometry export null count exceeds i64"))?;
    Ok(())
}

/// Attach a validity bitmap that was built by the detached planner. The
/// attached egress only publishes its already-owned pointer and records the
/// null count; it does not inspect or rebuild the bitmap.
pub(crate) fn apply_prebuilt_top_level_validity(
    array: &mut GometryArrowArray,
    validity: crate::geometry::SharedSlice<u8>,
    null_count: usize,
) -> PyResult<()> {
    let array = array.as_mut();
    let logical_len = usize::try_from(array.length).map_err(|_| {
        GeometryError::new_err("Arrow geometry export length is negative or too large")
    })?;
    if null_count > logical_len {
        return Err(GeometryError::new_err(
            "Arrow geometry export null count exceeds logical length",
        ));
    }
    if null_count == 0 {
        return Ok(());
    }
    if array.n_buffers < 1 {
        return Err(GeometryError::new_err(
            "Arrow geometry export has no top-level validity buffer slot",
        ));
    }
    let private = array.private_data.cast::<ArrayPrivate>();
    if private.is_null() {
        return Err(GeometryError::new_err(
            "Arrow geometry export is missing private buffer ownership",
        ));
    }
    // SAFETY: this is a gometry-built shell and `private_data` points to its
    // live ArrayPrivate allocation. The plan bitmap owns every published byte.
    unsafe {
        let private = &mut *private;
        if private.buffers.is_empty() {
            return Err(GeometryError::new_err(
                "Arrow geometry export has no top-level validity buffer slot",
            ));
        }
        private.buffers[0] = validity.as_ptr().cast();
        private.owners[0] = OwnedBufferSlot::U8(validity);
        array.buffers = private.buffers.as_ptr();
    }
    array.null_count = i64::try_from(null_count)
        .map_err(|_| GeometryError::new_err("Arrow geometry export null count exceeds i64"))?;
    Ok(())
}

pub(crate) fn empty_array() -> ArrowArray {
    ArrowArray {
        length: 0,
        null_count: 0,
        offset: 0,
        n_buffers: 0,
        n_children: 0,
        buffers: ptr::null(),
        children: ptr::null_mut(),
        dictionary: ptr::null_mut(),
        release: None,
        private_data: ptr::null_mut(),
    }
}

/// Float64 primitive that retains the full `Arc` owner and exposes a logical
/// window via Arrow `offset`/`length` — zero-copy Window export over parent
/// coordinate columns. Identity uses `start = 0`, `len = values.len()`.
///
/// Range is checked in release builds so an out-of-range window cannot export.
pub(crate) fn primitive_f64_array_windowed(
    values: SharedSlice<f64>,
    start: usize,
    len: usize,
) -> PyResult<GometryArrowArray> {
    let end = start
        .checked_add(len)
        .ok_or_else(|| GeometryError::new_err("Arrow primitive f64 window overflows"))?;
    if end > values.len() {
        return Err(GeometryError::new_err(
            "Arrow primitive f64 window exceeds owned column",
        ));
    }
    make_array_with_offset(
        len,
        start,
        [OwnedBufferSlot::Null, OwnedBufferSlot::F64(values)],
        Vec::new(),
    )
}

pub(crate) fn coordinate_schema(axes: CoordinateAxes) -> NativeArrowExportResult<SchemaNode> {
    let mut children = Vec::new();
    crate::try_reserve_vec(&mut children, 4)?;
    children.extend([
        SchemaNode {
            format: "g",
            name: "x",
            metadata: None,
            children: Vec::new(),
        },
        SchemaNode {
            format: "g",
            name: "y",
            metadata: None,
            children: Vec::new(),
        },
    ]);
    if axes.has_z() {
        children.push(SchemaNode {
            format: "g",
            name: "z",
            metadata: None,
            children: Vec::new(),
        });
    }
    if axes.has_m() {
        children.push(SchemaNode {
            format: "g",
            name: "m",
            metadata: None,
            children: Vec::new(),
        });
    }
    Ok(SchemaNode {
        format: "+s",
        name: "",
        metadata: None,
        children,
    })
}

pub(crate) fn coordinate_array(seq: &CoordSeq) -> PyResult<GometryArrowArray> {
    // Retain parent column Arcs and expose the sequence window via Arrow
    // offset/length. Identity and Windowed `CoordSeq` views share storage;
    // Gather materialization always produces a full-window seq first.
    let columns = seq.column_arcs();
    let start = columns.window.start;
    let len = columns.window.end - columns.window.start;
    let m_parts = columns.m_parts();
    let mut children = crate::try_vec_with_capacity(4)?;
    children.push(primitive_f64_array_windowed(columns.xs, start, len)?);
    children.push(primitive_f64_array_windowed(columns.ys, start, len)?);
    if let Some(zs) = columns.zs {
        children.push(primitive_f64_array_windowed(zs, start, len)?);
    }
    if let Some((ms, m_range)) = m_parts {
        children.push(primitive_f64_array_windowed(
            ms,
            m_range.start,
            m_range.end - m_range.start,
        )?);
    }
    make_array(len, [OwnedBufferSlot::Null], children)
}

/// List array over a full CSR offsets buffer with a logical row window.
///
/// Parent offsets and the child coordinate run stay shared; only the Arrow
/// shell records `offset`/`length`. Used for packed Identity (offset 0) and
/// Window (offset = row start) export without rebasing or gathering.
///
/// Offsets must be nonempty, non-negative, non-decreasing over the visible
/// window, and the window terminal must not exceed the child length.
#[cfg(test)]
pub(crate) fn list_array_windowed(
    offsets: crate::geometry::SharedSlice<i32>,
    child: GometryArrowArray,
    start: usize,
    length: usize,
) -> PyResult<GometryArrowArray> {
    if offsets.is_empty() {
        return Err(GeometryError::new_err(
            "Arrow list offsets must contain at least one entry",
        ));
    }
    let child_len = child.logical_length()?;
    // Full offset-chain admission (window, monotonic, non-negative, terminal
    // ≤ child length) — same contract as the import side.
    crate::py::arrow::ensure_i32_offsets_monotonic(&offsets, start, length, child_len)?;
    make_array_with_offset(
        length,
        start,
        [OwnedBufferSlot::Null, OwnedBufferSlot::I32(offsets)],
        [child],
    )
}

/// Materialize a list shell after its offsets have already been admitted by
/// the detached export planner. No offset scan or native packing work belongs
/// in the attached capsule egress.
pub(crate) fn admitted_list_array_windowed(
    offsets: crate::geometry::SharedSlice<i32>,
    child: GometryArrowArray,
    start: usize,
    length: usize,
) -> PyResult<GometryArrowArray> {
    make_array_with_offset(
        length,
        start,
        [OwnedBufferSlot::Null, OwnedBufferSlot::I32(offsets)],
        [child],
    )
}

/// Binary array from owned offsets + data.
///
/// Offsets must be nonempty; **every** entry (not only the terminal) must be
/// non-negative and non-decreasing, and the terminal must lie within `data`.
/// Intermediate OOB/negative values (e.g. `[100, 2]` or `[-5, 0]`) are rejected.
#[cfg(test)]
pub(crate) fn binary_array(
    offsets: crate::geometry::SharedSlice<i32>,
    data: crate::geometry::SharedSlice<u8>,
) -> PyResult<GometryArrowArray> {
    if offsets.is_empty() {
        return Err(GeometryError::new_err(
            "Arrow binary offsets must contain at least one entry",
        ));
    }
    let length = offsets.len() - 1;
    crate::py::arrow::ensure_i32_offsets_monotonic(&offsets, 0, length, data.len())?;
    make_array(
        length,
        [
            OwnedBufferSlot::Null,
            OwnedBufferSlot::I32(offsets),
            OwnedBufferSlot::U8(data),
        ],
        Vec::new(),
    )
}

pub(crate) fn admitted_binary_array(
    offsets: crate::geometry::SharedSlice<i32>,
    data: crate::geometry::SharedSlice<u8>,
) -> PyResult<GometryArrowArray> {
    let length = offsets.len().checked_sub(1).ok_or_else(|| {
        GeometryError::new_err("Arrow binary offsets must contain at least one entry")
    })?;
    make_array(
        length,
        [
            OwnedBufferSlot::Null,
            OwnedBufferSlot::I32(offsets),
            OwnedBufferSlot::U8(data),
        ],
        Vec::new(),
    )
}

pub(crate) fn extension_schema(
    encoding: GeometryEncoding,
    storage: SchemaNode,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<SchemaNode> {
    let extension_name = encoding.extension_name();
    let mut metadata = Vec::new();
    crate::try_reserve_vec(&mut metadata, 2)?;
    let text = |value: &str| {
        crate::boundary::try_string_from_str(value)
            .map_err(|()| crate::error::Error::from(crate::error::ResourceError::Allocation))
    };
    metadata.push((text("ARROW:extension:name")?, text(extension_name)?));
    if let Some(value) = extension_metadata_json(crs, epoch)? {
        metadata.push((text("ARROW:extension:metadata")?, value));
    }
    Ok(SchemaNode {
        metadata: Some(metadata),
        ..storage
    })
}

pub(crate) fn extension_metadata_json(
    crs: Option<&str>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<Option<String>> {
    let mut metadata = Map::new();
    if let Some(crs) = crs {
        let projjson = crate::crs::to_projjson(crs)?;
        let projjson = serde_json::from_str::<Value>(&projjson).map_err(|error| {
            NativeArrowExportError::geoarrow(format!("invalid PROJJSON generated by PROJ: {error}"))
        })?;
        metadata.insert("crs".to_owned(), projjson);
        metadata.insert("crs_type".to_owned(), Value::String("projjson".to_owned()));
    }
    if let Some(epoch) = epoch {
        let value = Number::from_f64(epoch)
            .ok_or_else(|| NativeArrowExportError::geometry("coordinate epoch must be finite"))?;
        metadata.insert("epoch".to_owned(), Value::Number(value));
    }
    if metadata.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(&metadata).map(Some).map_err(|error| {
            NativeArrowExportError::geometry(format!("failed to encode GeoArrow metadata: {error}"))
        })
    }
}

pub(crate) fn list_schema(child: SchemaNode) -> NativeArrowExportResult<SchemaNode> {
    let mut children = Vec::new();
    crate::try_reserve_vec(&mut children, 1)?;
    children.push(SchemaNode {
        name: "item",
        ..child
    });
    Ok(SchemaNode {
        format: "+l",
        name: "",
        metadata: None,
        children,
    })
}

pub(crate) const fn wkb_schema() -> SchemaNode {
    SchemaNode {
        format: "z",
        name: "",
        metadata: None,
        children: Vec::new(),
    }
}

#[cfg(test)]
mod owner_export_tests {
    #[test]
    fn failed_parent_publication_releases_child_tree_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use super::{OwnedBufferSlot, make_array};
        use crate::test_support::{allocation_count, fail_allocations_after};
        static RELEASES: AtomicUsize = AtomicUsize::new(0);
        unsafe extern "C" fn counted_release(array: *mut crate::py::arrow_c::ArrowArray) {
            RELEASES.fetch_add(1, Ordering::Relaxed);
            // SAFETY: this callback wraps the production callback on an owned child.
            unsafe {
                crate::py::arrow_c::release_array(array);
            }
        }
        ensure_python();
        let fixture = || {
            let mut child = make_array(0, [OwnedBufferSlot::Null], Vec::new()).unwrap();
            child.as_mut().release = Some(counted_release);
            ([OwnedBufferSlot::Null], [child])
        };
        let (buffers, children) = fixture();
        let (parent, allocations) = allocation_count(|| make_array(0, buffers, children));
        drop(parent.unwrap());
        assert_eq!(RELEASES.load(Ordering::Relaxed), 1);
        assert!(allocations > 0);
        for successful in 0..allocations {
            RELEASES.store(0, Ordering::Relaxed);
            let (buffers, children) = fixture();
            let result = fail_allocations_after(successful, || make_array(0, buffers, children));
            assert!(result.is_err(), "allocation {successful} was not rejected");
            drop(result);
            assert_eq!(
                RELEASES.load(Ordering::Relaxed),
                1,
                "allocation {successful}"
            );
        }
    }

    use super::*;

    fn ensure_python() {
        crate::test_support::initialize_python();
    }

    #[test]
    fn primitive_window_rejects_out_of_range() {
        ensure_python();
        let values = SharedSlice::try_from_vec(vec![1.0_f64, 2.0, 3.0]).unwrap();
        primitive_f64_array_windowed(values.clone(), 0, 3).unwrap();
        assert!(primitive_f64_array_windowed(values, 2, 2).is_err());
    }

    #[test]
    fn list_window_rejects_empty_and_oob_offsets() {
        ensure_python();
        let empty: crate::geometry::SharedSlice<i32> =
            crate::geometry::SharedSlice::try_from_vec(vec![]).unwrap();
        let child = make_array(0, [OwnedBufferSlot::Null], Vec::new()).unwrap();
        assert!(list_array_windowed(empty, child, 0, 0).is_err());

        let offsets: crate::geometry::SharedSlice<i32> =
            crate::geometry::SharedSlice::try_from_vec(vec![0, 1, 2]).unwrap();
        let child = make_array(0, [OwnedBufferSlot::Null], Vec::new()).unwrap();
        assert!(list_array_windowed(offsets, child, 0, 3).is_err());
    }

    #[test]
    fn list_rejects_non_monotonic_and_terminal_past_child() {
        ensure_python();
        let child = make_array(2, [OwnedBufferSlot::Null], Vec::new()).unwrap();
        // Decreasing: 0, 2, 1
        let offsets: crate::geometry::SharedSlice<i32> =
            crate::geometry::SharedSlice::try_from_vec(vec![0, 2, 1]).unwrap();
        assert!(list_array_windowed(offsets, child, 0, 2).is_err());

        let child = make_array(2, [OwnedBufferSlot::Null], Vec::new()).unwrap();
        // Terminal 5 > child length 2
        let offsets: crate::geometry::SharedSlice<i32> =
            crate::geometry::SharedSlice::try_from_vec(vec![0, 5]).unwrap();
        assert!(list_array_windowed(offsets, child, 0, 1).is_err());
    }

    #[test]
    fn binary_rejects_empty_offsets_and_overlong_terminal() {
        ensure_python();
        let empty: crate::geometry::SharedSlice<i32> =
            crate::geometry::SharedSlice::try_from_vec(vec![]).unwrap();
        let data: crate::geometry::SharedSlice<u8> =
            crate::geometry::SharedSlice::try_from_vec(vec![1_u8, 2]).unwrap();
        assert!(binary_array(empty, data.clone()).is_err());

        let offsets: crate::geometry::SharedSlice<i32> =
            crate::geometry::SharedSlice::try_from_vec(vec![0, 9]).unwrap();
        assert!(binary_array(offsets, data).is_err());

        let offsets: crate::geometry::SharedSlice<i32> =
            crate::geometry::SharedSlice::try_from_vec(vec![0, 2]).unwrap();
        let data: crate::geometry::SharedSlice<u8> =
            crate::geometry::SharedSlice::try_from_vec(vec![1_u8, 2]).unwrap();
        binary_array(offsets, data).unwrap();
    }

    #[test]
    fn binary_rejects_intermediate_oob_and_negative_offsets() {
        ensure_python();
        let data: crate::geometry::SharedSlice<u8> =
            crate::geometry::SharedSlice::try_from_vec(vec![1_u8, 2, 3, 4]).unwrap();
        // Intermediate 100 > data.len(); terminal 2 is fine — still reject.
        let offsets: crate::geometry::SharedSlice<i32> =
            crate::geometry::SharedSlice::try_from_vec(vec![100, 2]).unwrap();
        assert!(binary_array(offsets, data.clone()).is_err());
        // Negative intermediate with non-negative terminal.
        let offsets: crate::geometry::SharedSlice<i32> =
            crate::geometry::SharedSlice::try_from_vec(vec![-5, 0]).unwrap();
        assert!(binary_array(offsets, data.clone()).is_err());
        // Non-monotonic positive chain.
        let offsets: crate::geometry::SharedSlice<i32> =
            crate::geometry::SharedSlice::try_from_vec(vec![0, 3, 1]).unwrap();
        assert!(binary_array(offsets, data).is_err());
    }

    #[test]
    fn make_array_rejects_length_past_owned_f64_column() {
        ensure_python();
        // Same residual the skeptic named: free length over a short Arc.
        let short = SharedSlice::try_from_vec(vec![1.0_f64]).unwrap();
        assert!(
            make_array_with_offset(
                1000,
                0,
                vec![OwnedBufferSlot::Null, OwnedBufferSlot::F64(short)],
                Vec::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn validity_mask_length_checked_in_release() {
        ensure_python();
        let mut array = make_array(2, [OwnedBufferSlot::Null], Vec::new()).unwrap();
        let short = crate::array::ValidityMask::from_vec(1, vec![true])
            .unwrap()
            .unwrap();
        assert!(apply_top_level_validity(&mut array, &short).is_err());
        let mask = crate::array::ValidityMask::from_vec(2, vec![false, true])
            .unwrap()
            .unwrap();
        apply_top_level_validity(&mut array, &mask).unwrap();
    }

    #[test]
    fn aligned_validity_window_shares_arc_with_arrow_owner() {
        ensure_python();
        let parent = crate::array::ValidityMask::from_vec(8, vec![
            false, false, false, true, false, true, false, false,
        ])
        .unwrap()
        .unwrap();
        let window = parent.window(3, 3).unwrap();
        let (bitmap, offset) = window.arrow_bitmap();
        assert_eq!(offset, 3);
        assert_eq!(bitmap.as_ref(), &[0b1101_0111]);

        let mut array = make_array_with_offset(3, 3, [OwnedBufferSlot::Null], Vec::new()).unwrap();
        apply_top_level_validity(&mut array, &window).unwrap();
        assert_eq!(array.0.null_count, 2);
        // SAFETY: `apply_top_level_validity` populated the buffer array with
        // one entry, so `*buffers` reads an initialized pointer. The first
        // exported pointer is the original Arc allocation, not a repacked
        // bitmap, and `ArrayPrivate` retains its own Arc clone.
        let exported = unsafe { *array.0.buffers }.cast::<u8>();
        assert_eq!(exported, bitmap.as_ptr());
        drop((parent, window, bitmap));
        // SAFETY: `ArrayPrivate` keeps the allocation alive past the drop above.
        assert_eq!(unsafe { *exported }, 0b1101_0111);
    }
}
