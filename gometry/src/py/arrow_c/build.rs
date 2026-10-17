use std::ffi::c_void;
use std::fmt::Write as _;
use std::ptr;
use std::sync::Arc;

use serde_json::Number;

use crate::geometry::{SharedSlice, storage};
use crate::py::arrow_c::{
    ARROW_FLAG_NULLABLE, ArrayPrivate, ArrowArray, ArrowSchema, CoordSeq, CoordinateAxes,
    GeometryEncoding, GeometryError, NativeArrowExportError, NativeArrowExportResult, PyResult,
    SchemaNode, SchemaPrivate, release_array, release_schema,
};
use crate::py::geoarrow::CRS_DEFINITION_METADATA_KEY;

impl SchemaNode {
    pub(crate) fn materialize_schema(&self) -> NativeArrowExportResult<GometryArrowSchema> {
        let Self {
            format,
            name,
            metadata,
            nullable,
            children,
        } = self;
        let mut owned_children = Vec::with_capacity(children.len());
        for child in children {
            owned_children.push(child.materialize_schema()?);
        }
        let children = owned_children;
        let mut child_ptrs = Vec::with_capacity(children.len());
        for child in &children {
            child_ptrs.push(ptr::from_ref(child.as_ref()).cast_mut());
        }
        let mut private = Box::new(SchemaPrivate {
            format: schema_text(format)?,
            name: schema_text(name)?,
            metadata: metadata
                .as_deref()
                .map(encode_schema_metadata)
                .transpose()?,
            children,
            child_ptrs,
        });
        let mut schema = Box::new(ArrowSchema {
            format: private.format.as_ptr().cast(),
            name: private.name.as_ptr().cast(),
            metadata: private
                .metadata
                .as_ref()
                .map_or(ptr::null(), |metadata| metadata.as_ptr().cast()),
            // The Arrow C Data Interface nullable flag marks a nullable field.
            flags: if *nullable { ARROW_FLAG_NULLABLE } else { 0 },
            n_children: private.children.len() as i64,
            children: if private.child_ptrs.is_empty() {
                ptr::null_mut()
            } else {
                private.child_ptrs.as_mut_ptr()
            },
            dictionary: ptr::null_mut(),
            release: Some(release_schema),
            private_data: ptr::null_mut(),
        });
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
    let capacity = text.len().saturating_add(1);
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(text.as_bytes());
    bytes.push(0);
    Ok(bytes)
}

#[expect(
    clippy::checked_conversions,
    reason = "key and value lengths are admitted as i32 before their C Data Interface writes"
)]
pub(crate) fn encode_schema_metadata(
    pairs: &[(String, String)],
) -> NativeArrowExportResult<Vec<u8>> {
    let pair_count = i32::try_from(pairs.len()).map_err(|_| {
        NativeArrowExportError::geometry("Arrow schema metadata has too many entries")
    })?;
    for (key, value) in pairs {
        i32::try_from(key.len()).map_err(|_| {
            NativeArrowExportError::geometry("Arrow schema metadata key is too large")
        })?;
        i32::try_from(value.len()).map_err(|_| {
            NativeArrowExportError::geometry("Arrow schema metadata value is too large")
        })?;
    }
    let mut capacity = size_of::<i32>();
    for (key, value) in pairs {
        capacity = capacity
            .checked_add(size_of::<i32>())
            .and_then(|capacity| capacity.checked_add(key.len()))
            .and_then(|capacity| capacity.checked_add(size_of::<i32>()))
            .and_then(|capacity| capacity.checked_add(value.len()))
            .filter(|capacity| *capacity <= isize::MAX as usize)
            .ok_or_else(|| {
                NativeArrowExportError::overflow(
                    "Arrow schema metadata envelope exceeds addressable size",
                )
            })?;
    }
    let mut out = Vec::with_capacity(capacity);
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
    U64(SharedSlice<u64>),
    U64Arc(Arc<[u64]>),
    U8(crate::geometry::SharedSlice<u8>),
}

impl OwnedBufferSlot {
    fn as_ptr(&self) -> *const c_void {
        match self {
            Self::Null => ptr::null(),
            Self::F64(values) => values.as_ptr().cast(),
            Self::I32(values) => values.as_ptr().cast(),
            Self::U64(values) => values.as_ptr().cast(),
            Self::U64Arc(values) => values.as_ptr().cast(),
            Self::U8(values) => values.as_ptr().cast(),
        }
    }
}

impl ArrayPrivate {
    fn replace_first_buffer(&mut self, owner: OwnedBufferSlot) -> bool {
        let Some((pointer, owner_slot)) = self.buffers.first_mut().zip(self.owners.first_mut())
        else {
            return false;
        };
        *pointer = owner.as_ptr();
        *owner_slot = owner;
        true
    }
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
        .map_err(|_| GeometryError::new_err(format!("Arrow export {what} exceeds i64")))
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
        .ok_or_else(|| GeometryError::new_err("Arrow array export offset+length overflows"))?;
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
            OwnedBufferSlot::U64(values) => {
                if end > values.len() {
                    return Err(GeometryError::new_err(
                        "Arrow cell export window exceeds owned uint64 column",
                    ));
                }
            },
            OwnedBufferSlot::U64Arc(values) => {
                if end > values.len() {
                    return Err(GeometryError::new_err(
                        "Arrow cell export window exceeds owned uint64 column",
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
                        "Arrow array export window exceeds owned offsets column",
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
    let buffers: Vec<_> = buffers.into_iter().collect();
    let mut children: Vec<_> = children.into_iter().collect();
    ensure_owned_buffer_window(length, offset, &buffers)?;
    let n_buffers = buffers.len();
    let length_i64 = checked_i64(length, "length")?;
    let offset_i64 = checked_i64(offset, "offset")?;
    let n_buffers_i64 = checked_i64(n_buffers, "n_buffers")?;

    let mut buffer_ptrs = Vec::with_capacity(n_buffers);
    for slot in &buffers {
        buffer_ptrs.push(slot.as_ptr());
    }
    let n_children_i64 = checked_i64(children.len(), "n_children")?;
    let mut child_ptrs = Vec::with_capacity(children.len());
    for child in &mut children {
        child_ptrs.push(ptr::from_mut(child.as_mut()));
    }
    let mut private = Box::new(ArrayPrivate {
        owners: buffers,
        buffers: buffer_ptrs,
        _children: children,
        child_ptrs,
    });
    let mut array = Box::new(ArrowArray {
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
    });
    array.private_data = Box::into_raw(private).cast();
    Ok(GometryArrowArray(array))
}

/// Attach a top-level validity bitmap to a **gometry-built** array shell.
///
/// Accepts only [`GometryArrowArray`] so a foreign shell cannot enter. Mask
/// length is checked in release builds (not debug-only).
pub(crate) fn apply_top_level_validity(
    array: &mut GometryArrowArray,
    mask: &storage::ValidityMask,
) -> PyResult<()> {
    let array = array.as_mut();
    let logical_len = usize::try_from(array.length).map_err(|_| {
        GeometryError::new_err("Arrow array export length is negative or too large")
    })?;
    if mask.len() != logical_len {
        return Err(GeometryError::new_err(
            "validity mask length must match logical Arrow array length",
        ));
    }
    let null_count = mask.missing_count();
    if null_count == 0 {
        return Ok(());
    }
    if array.n_buffers < 1 {
        return Err(GeometryError::new_err(
            "Arrow array export has no top-level validity buffer slot",
        ));
    }
    // Arrow C Data Interface: for a non-zero array.offset, validity bit for
    // logical index `i` is at physical bit `offset + i` (consumers apply the
    // offset). Windowed packed LineString/Polygon export retains parent CSR
    // via offset/length — the logical missing mask must be placed at that
    // offset, not at bit 0.
    let offset = usize::try_from(array.offset).map_err(|_| {
        GeometryError::new_err("Arrow array export offset is negative or too large")
    })?;
    let total_bits = offset
        .checked_add(mask.len())
        .ok_or_else(|| GeometryError::new_err("Arrow array export validity span exceeds usize"))?;
    let (owned, mask_offset) = mask.arrow_bitmap();
    let owned_bits = owned.len().saturating_mul(8);
    let validity = if mask_offset == offset && owned_bits >= total_bits {
        owned
    } else {
        let mut validity = vec![0; total_bits.div_ceil(8)];
        for row in 0..mask.len() {
            if mask.is_valid(row) {
                let bit = offset.wrapping_add(row);
                *validity
                    .get_mut(bit / 8)
                    .expect("validity bit lies within its reserved bitmap") |= 1 << (bit % 8);
            }
        }
        crate::geometry::SharedSlice::from_vec(validity)
    };
    let private = array.private_data.cast::<ArrayPrivate>();
    if private.is_null() {
        return Err(GeometryError::new_err(
            "Arrow array export is missing private buffer ownership",
        ));
    }
    // SAFETY: `GometryArrowArray` always carries an `ArrayPrivate` box whose
    // `buffers` vector backs `array.buffers`. Replacing its validity owner
    // preserves both vector allocations and retains the published bytes.
    unsafe {
        let private = &mut *private;
        if !private.replace_first_buffer(OwnedBufferSlot::U8(validity)) {
            return Err(GeometryError::new_err(
                "Arrow array export has no top-level validity buffer slot",
            ));
        }
        array.buffers = private.buffers.as_ptr();
    }
    array.null_count = i64::try_from(null_count)
        .map_err(|_| GeometryError::new_err("Arrow array export null count exceeds i64"))?;
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
        if !private.replace_first_buffer(OwnedBufferSlot::U8(validity)) {
            return Err(GeometryError::new_err(
                "Arrow geometry export has no top-level validity buffer slot",
            ));
        }
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

pub(crate) fn primitive_u64_array_windowed(
    values: SharedSlice<u64>,
    start: usize,
    len: usize,
) -> PyResult<GometryArrowArray> {
    make_array_with_offset(
        len,
        start,
        [OwnedBufferSlot::Null, OwnedBufferSlot::U64(values)],
        Vec::new(),
    )
}

pub(crate) fn primitive_u64_arc_array_windowed(
    values: Arc<[u64]>,
    start: usize,
    len: usize,
) -> PyResult<GometryArrowArray> {
    make_array_with_offset(
        len,
        start,
        [OwnedBufferSlot::Null, OwnedBufferSlot::U64Arc(values)],
        Vec::new(),
    )
}

pub(crate) fn utf8_array(
    offsets: SharedSlice<i32>,
    values: SharedSlice<u8>,
) -> PyResult<GometryArrowArray> {
    let length = offsets.len().checked_sub(1).ok_or_else(|| {
        GeometryError::new_err("Arrow UTF-8 offsets must contain at least one entry")
    })?;
    let final_offset = offsets.last().copied();
    if final_offset.and_then(|offset| usize::try_from(offset).ok()) != Some(values.len()) {
        return Err(GeometryError::new_err(
            "Arrow UTF-8 offsets do not match the owned values buffer",
        ));
    }
    make_array(
        length,
        [
            OwnedBufferSlot::Null,
            OwnedBufferSlot::I32(offsets),
            OwnedBufferSlot::U8(values),
        ],
        Vec::new(),
    )
}

pub(crate) const fn primitive_u64_schema(nullable: bool) -> SchemaNode {
    SchemaNode {
        format: "L",
        name: "",
        metadata: None,
        nullable,
        children: Vec::new(),
    }
}

pub(crate) const fn utf8_schema() -> SchemaNode {
    SchemaNode {
        format: "u",
        name: "",
        metadata: None,
        nullable: true,
        children: Vec::new(),
    }
}

pub(crate) fn coordinate_schema(axes: CoordinateAxes, nullable: bool) -> SchemaNode {
    let mut children = Vec::with_capacity(4);
    children.extend([
        SchemaNode {
            format: "g",
            name: "x",
            metadata: None,
            nullable,
            children: Vec::new(),
        },
        SchemaNode {
            format: "g",
            name: "y",
            metadata: None,
            nullable,
            children: Vec::new(),
        },
    ]);
    if axes.has_z() {
        children.push(SchemaNode {
            format: "g",
            name: "z",
            metadata: None,
            nullable,
            children: Vec::new(),
        });
    }
    if axes.has_m() {
        children.push(SchemaNode {
            format: "g",
            name: "m",
            metadata: None,
            nullable,
            children: Vec::new(),
        });
    }
    SchemaNode {
        format: "+s",
        name: "",
        metadata: None,
        nullable,
        children,
    }
}

pub(crate) fn coordinate_array(seq: &CoordSeq) -> PyResult<GometryArrowArray> {
    // Retain parent column Arcs and expose the sequence window via Arrow
    // offset/length. Identity and Windowed `CoordSeq` views share storage;
    // Gather materialization always produces a full-window seq first.
    let columns = seq.column_arcs();
    let start = columns.window.start;
    let len = columns.window.len();
    let m_parts = columns.m_parts();
    let mut children = Vec::with_capacity(4);
    children.push(primitive_f64_array_windowed(columns.xs, start, len)?);
    children.push(primitive_f64_array_windowed(columns.ys, start, len)?);
    if let Some(zs) = columns.zs {
        children.push(primitive_f64_array_windowed(zs, start, len)?);
    }
    if let Some((ms, m_range)) = m_parts {
        children.push(primitive_f64_array_windowed(
            ms,
            m_range.start,
            m_range.len(),
        )?);
    }
    make_array(len, [OwnedBufferSlot::Null], children)
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
    let mut metadata = Vec::with_capacity(2);
    metadata.push(("ARROW:extension:name".to_owned(), extension_name.to_owned()));
    if let Some(value) = extension_metadata_json(crs, epoch)? {
        metadata.push(("ARROW:extension:metadata".to_owned(), value));
    }
    Ok(SchemaNode {
        metadata: Some(metadata),
        ..storage
    })
}

/// zmij's finite `f64` formatter, used by `serde_json::Number` Display, writes
/// at most 24 bytes.
const ZMIJ_F64_DISPLAY: usize = 24;
const CRS_PREFIX: &str = "{\"crs\":";
const CRS_TYPE_FIELD: &str = ",\"crs_type\":\"projjson\"";
const CRS_DEFINITION_FIELD_PREFIX: &str = ",\"";
const CRS_DEFINITION_FIELD_SUFFIX: &str = "\":";
const EPOCH_FIELD: &str = ",\"epoch\":";
const EPOCH_ONLY_PREFIX: &str = "{\"epoch\":";
const OBJECT_END: &str = "}";

pub(crate) fn extension_metadata_json(
    crs: Option<&str>,
    epoch: Option<f64>,
) -> NativeArrowExportResult<Option<String>> {
    if crs.is_none() && epoch.is_none() {
        return Ok(None);
    }
    let projjson = crs.map(crate::crs::to_projjson).transpose()?;
    let crs_definition = crs.map(|crs| {
        serde_json::to_string(crs).expect("a CRS definition string is JSON-serializable")
    });
    let epoch = epoch
        .map(|epoch| {
            Number::from_f64(epoch)
                .ok_or_else(|| NativeArrowExportError::geometry("coordinate epoch must be finite"))
        })
        .transpose()?;
    Ok(Some(assemble_extension_metadata(
        projjson,
        crs_definition.as_deref(),
        epoch,
    )))
}

/// Wrap trusted PROJJSON bytes in the GeoArrow envelope. Does not parse or
/// rebuild a `serde_json::Value` tree. The namespaced definition records the
/// producer's CRS label beside GeoArrow's standardized PROJJSON value.
fn assemble_extension_metadata(
    projjson: Option<String>,
    crs_definition: Option<&str>,
    epoch: Option<Number>,
) -> String {
    debug_assert!(
        projjson.is_some() || epoch.is_some(),
        "absent CRS and epoch is returned as None before assembly"
    );
    let crs_definition_field_len = CRS_DEFINITION_FIELD_PREFIX
        .len()
        .wrapping_add(CRS_DEFINITION_METADATA_KEY.len())
        .wrapping_add(CRS_DEFINITION_FIELD_SUFFIX.len());
    let extra = match (projjson.is_some(), epoch.is_some()) {
        (true, false) => CRS_PREFIX
            .len()
            .wrapping_add(CRS_TYPE_FIELD.len())
            .wrapping_add(crs_definition_field_len)
            .wrapping_add(
                crs_definition
                    .expect("a CRS definition accompanies PROJJSON")
                    .len(),
            )
            .wrapping_add(OBJECT_END.len()),
        (true, true) => CRS_PREFIX
            .len()
            .wrapping_add(CRS_TYPE_FIELD.len())
            .wrapping_add(crs_definition_field_len)
            .wrapping_add(
                crs_definition
                    .expect("a CRS definition accompanies PROJJSON")
                    .len(),
            )
            .wrapping_add(EPOCH_FIELD.len())
            .wrapping_add(ZMIJ_F64_DISPLAY)
            .wrapping_add(OBJECT_END.len()),
        (false, true) => EPOCH_ONLY_PREFIX
            .len()
            .wrapping_add(ZMIJ_F64_DISPLAY)
            .wrapping_add(OBJECT_END.len()),
        (false, false) => 0,
    };
    let needed = projjson.as_deref().map_or(0, str::len).wrapping_add(extra);
    let mut out = String::with_capacity(needed);
    if let Some(projjson) = projjson {
        out.push_str(CRS_PREFIX);
        out.push_str(&projjson);
        out.push_str(CRS_TYPE_FIELD);
        out.push_str(CRS_DEFINITION_FIELD_PREFIX);
        out.push_str(CRS_DEFINITION_METADATA_KEY);
        out.push_str(CRS_DEFINITION_FIELD_SUFFIX);
        out.push_str(crs_definition.expect("a CRS definition accompanies PROJJSON"));
        if let Some(epoch) = epoch {
            out.push_str(EPOCH_FIELD);
            write!(out, "{epoch}").expect("writing into a reserved String cannot fail");
        }
    } else if let Some(epoch) = epoch {
        out.push_str(EPOCH_ONLY_PREFIX);
        write!(out, "{epoch}").expect("writing into a reserved String cannot fail");
    }
    out.push_str(OBJECT_END);
    out
}

pub(crate) fn list_schema(child: SchemaNode, nullable: bool) -> SchemaNode {
    let children = vec![SchemaNode {
        name: "item",
        ..child
    }];
    SchemaNode {
        format: "+l",
        name: "",
        metadata: None,
        nullable,
        children,
    }
}

pub(crate) const fn wkb_schema(nullable: bool) -> SchemaNode {
    SchemaNode {
        format: "z",
        name: "",
        metadata: None,
        nullable,
        children: Vec::new(),
    }
}

#[cfg(test)]
mod owner_export_tests {
    use super::*;

    #[test]
    fn primitive_window_rejects_out_of_range() {
        let values = SharedSlice::from_vec(vec![1.0_f64, 2.0, 3.0]);
        primitive_f64_array_windowed(values.clone(), 0, 3).unwrap();
        assert!(primitive_f64_array_windowed(values, 2, 2).is_err());
    }

    #[test]
    fn make_array_rejects_length_past_owned_f64_column() {
        // Same residual the skeptic named: free length over a short Arc.
        let short = SharedSlice::from_vec(vec![1.0_f64]);
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
        let mut array = make_array(2, [OwnedBufferSlot::Null], Vec::new()).unwrap();
        let short = storage::ValidityMask::from_vec(1, vec![true]).unwrap();
        assert!(apply_top_level_validity(&mut array, &short).is_err());
        let mask = storage::ValidityMask::from_vec(2, vec![false, true]).unwrap();
        apply_top_level_validity(&mut array, &mask).unwrap();
    }

    #[test]
    fn aligned_validity_window_shares_arc_with_arrow_owner() {
        let parent = storage::ValidityMask::from_vec(8, vec![
            false, false, false, true, false, true, false, false,
        ])
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

    #[test]
    fn extension_metadata_json_is_absent_without_crs_or_epoch() {
        assert!(extension_metadata_json(None, None).unwrap().is_none());
    }

    #[test]
    fn extension_metadata_json_rejects_nonfinite_epoch() {
        for epoch in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = extension_metadata_json(None, Some(epoch)).unwrap_err();
            assert!(
                matches!(&error, NativeArrowExportError::Geometry(message) if message.contains("finite")),
                "{error:?}"
            );
        }
    }

    #[test]
    fn extension_metadata_json_epoch_only_is_semantic_json() {
        let text = extension_metadata_json(None, Some(2010.0))
            .unwrap()
            .expect("epoch metadata must be present");
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value.get("crs"), None);
        assert_eq!(value.get("crs_type"), None);
        assert_eq!(value["epoch"].as_f64(), Some(2010.0));
    }

    #[test]
    fn assemble_extension_metadata_embeds_projjson_bytes() {
        let projjson = "{\"type\":\"GeographicCRS\",\"name\":\"fixture\"}".to_owned();
        let definition = serde_json::to_string("EPSG:4326").unwrap();
        let text = assemble_extension_metadata(Some(projjson.clone()), Some(&definition), None);
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["crs_type"], "projjson");
        assert_eq!(
            value["crs"],
            serde_json::from_str::<serde_json::Value>(&projjson).unwrap()
        );
        assert_eq!(value["gometry:crs_definition"], "EPSG:4326");
        assert!(text.contains(&projjson), "PROJJSON bytes must be retained");
        let with_epoch = assemble_extension_metadata(
            Some(projjson),
            Some(&definition),
            Number::from_f64(2010.5),
        );
        let value: serde_json::Value = serde_json::from_str(&with_epoch).unwrap();
        assert_eq!(value["epoch"].as_f64(), Some(2010.5));
        assert_eq!(value["crs_type"], "projjson");
    }

    #[test]
    fn assemble_extension_metadata_epoch_extremes_use_one_envelope_allocation() {
        for epoch in [
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            -0.0,
        ] {
            let number = Number::from_f64(epoch).expect("finite epoch");
            let reserved = number.clone();
            let (text, allocations) = crate::test_support::allocation_count(|| {
                assemble_extension_metadata(None, None, Some(reserved))
            });
            assert_eq!(allocations, 1, "epoch {epoch:?} allocated {allocations}");
            let token = text
                .strip_prefix(EPOCH_ONLY_PREFIX)
                .and_then(|body| body.strip_suffix(OBJECT_END))
                .expect("epoch-only envelope uses the reserved prefix and suffix");
            assert!(
                token.len() <= ZMIJ_F64_DISPLAY,
                "Display outgrew the zmij buffer: {token:?} ({} bytes)",
                token.len()
            );
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert!(value["epoch"].is_number(), "{text}");
            let repeated = assemble_extension_metadata(None, None, Some(number));
            assert_eq!(repeated, text);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn extension_metadata_json_wraps_cached_projjson() {
        let projjson = crate::crs::to_projjson("EPSG:4326").expect("cached EPSG:4326 PROJJSON");
        let text = extension_metadata_json(Some("EPSG:4326"), Some(2010.0))
            .unwrap()
            .expect("CRS metadata must be present");
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["crs_type"], "projjson");
        assert_eq!(
            value["crs"],
            serde_json::from_str::<serde_json::Value>(&projjson).unwrap()
        );
        assert_eq!(value["gometry:crs_definition"], "EPSG:4326");
        assert_eq!(value["epoch"].as_f64(), Some(2010.0));
        assert!(
            text.contains(&projjson),
            "trusted PROJJSON bytes must appear as the crs value"
        );
    }
}
