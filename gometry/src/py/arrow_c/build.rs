#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ptr;
use std::sync::Arc;

use crate::py::arrow_c::{
    ArrayPrivate, ArrowArray, ArrowSchema, CString, CoordSeq, CoordinateAxes, GeometryEncoding,
    GeometryError, Map, Number, ParseError, PyResult, SchemaNode, SchemaPrivate, Value,
    release_array, release_schema,
};

impl SchemaNode {
    pub(crate) fn into_schema(self) -> Box<ArrowSchema> {
        let mut private = Box::new(SchemaPrivate {
            format: CString::new(self.format).expect("schema format has no nul"),
            name: CString::new(self.name).expect("schema name has no nul"),
            metadata: self.metadata.map(encode_schema_metadata),
            children: self.children.into_iter().map(Self::into_schema).collect(),
            child_ptrs: Vec::new(),
        });
        private.child_ptrs = private
            .children
            .iter_mut()
            .map(|child| ptr::from_mut(child.as_mut()))
            .collect();
        let schema = ArrowSchema {
            format: private.format.as_ptr(),
            name: private.name.as_ptr(),
            metadata: private
                .metadata
                .as_ref()
                .map_or(ptr::null(), |metadata| metadata.as_ptr().cast()),
            flags: 0,
            n_children: private.children.len() as i64,
            children: if private.child_ptrs.is_empty() {
                ptr::null_mut()
            } else {
                private.child_ptrs.as_mut_ptr()
            },
            dictionary: ptr::null_mut(),
            release: Some(release_schema),
            private_data: Box::into_raw(private).cast(),
        };
        Box::new(schema)
    }
}

pub(crate) fn encode_schema_metadata(pairs: Vec<(String, String)>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(pairs.len() as i32).to_ne_bytes());
    for (key, value) in pairs {
        out.extend_from_slice(&(key.len() as i32).to_ne_bytes());
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(&(value.len() as i32).to_ne_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    out
}

/// One buffer slot whose raw pointer is derived from a retained owner.
///
/// A foreign or mismatched pointer cannot enter this type: every non-null
/// slot owns the exact allocation the published pointer addresses.
#[derive(Clone)]
pub(crate) enum OwnedBufferSlot {
    /// Null validity / empty slot (Arrow null buffer pointer).
    Null,
    F64(Arc<[f64]>),
    I32(Arc<[i32]>),
    U8(Arc<[u8]>),
}

/// Gometry-built Arrow array shell. Only the builders in this module construct
/// it; foreign shells and ownerless raw-pointer pairs are unrepresentable.
#[repr(transparent)]
pub(crate) struct GometryArrowArray(Box<ArrowArray>);

impl GometryArrowArray {
    pub(crate) fn into_box(self) -> Box<ArrowArray> {
        self.0
    }

    pub(crate) fn as_mut(&mut self) -> &mut ArrowArray {
        self.0.as_mut()
    }

    fn logical_length(&self) -> PyResult<usize> {
        usize::try_from(self.0.length).map_err(|_| {
            GeometryError::new_err("Arrow geometry export length is negative or too large")
        })
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
    buffers: Vec<OwnedBufferSlot>,
    children: Vec<GometryArrowArray>,
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
    buffers: Vec<OwnedBufferSlot>,
    children: Vec<GometryArrowArray>,
) -> PyResult<GometryArrowArray> {
    ensure_owned_buffer_window(length, offset, &buffers)?;
    let n_buffers = buffers.len();
    let length_i64 = checked_i64(length, "length")?;
    let offset_i64 = checked_i64(offset, "offset")?;
    let n_buffers_i64 = checked_i64(n_buffers, "n_buffers")?;

    let mut f64_buffers = Vec::new();
    let mut i32_buffers = Vec::new();
    let mut u8_buffers = Vec::new();
    let mut buffer_ptrs = Vec::with_capacity(n_buffers);
    for slot in buffers {
        match slot {
            OwnedBufferSlot::Null => buffer_ptrs.push(ptr::null()),
            OwnedBufferSlot::F64(values) => {
                buffer_ptrs.push(values.as_ptr().cast());
                f64_buffers.push(values);
            },
            OwnedBufferSlot::I32(values) => {
                buffer_ptrs.push(values.as_ptr().cast());
                i32_buffers.push(values);
            },
            OwnedBufferSlot::U8(values) => {
                buffer_ptrs.push(values.as_ptr().cast());
                u8_buffers.push(values);
            },
        }
    }

    let children: Vec<Box<ArrowArray>> = children
        .into_iter()
        .map(GometryArrowArray::into_box)
        .collect();
    let mut private = Box::new(ArrayPrivate {
        _f64_buffers: f64_buffers,
        _i32_buffers: i32_buffers,
        u8_buffers,
        buffers: buffer_ptrs,
        children,
        child_ptrs: Vec::new(),
    });
    private.child_ptrs = private
        .children
        .iter_mut()
        .map(|child| ptr::from_mut(child.as_mut()))
        .collect();
    let n_children_i64 = checked_i64(private.children.len(), "n_children")?;
    let array = ArrowArray {
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
        private_data: Box::into_raw(private).cast(),
    };
    Ok(GometryArrowArray(Box::new(array)))
}

/// Attach a top-level validity bitmap to a **gometry-built** array shell.
///
/// Accepts only [`GometryArrowArray`] so a foreign shell cannot enter. Mask
/// length is checked in release builds (not debug-only).
pub(crate) fn apply_top_level_validity(
    array: &mut GometryArrowArray,
    mask: &[bool],
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
    let null_count = mask.iter().filter(|&&missing| missing).count();
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
    let mut validity = vec![0_u8; total_bits.div_ceil(8)];
    for (row, &missing) in mask.iter().enumerate() {
        if !missing {
            let bit = offset + row;
            validity[bit / 8] |= 1 << (bit % 8);
        }
    }
    let validity: Arc<[u8]> = validity.into();
    let private = array.private_data.cast::<ArrayPrivate>();
    if private.is_null() {
        return Err(GeometryError::new_err(
            "Arrow geometry export is missing private buffer ownership",
        ));
    }
    // SAFETY: `GometryArrowArray` always carries an `ArrayPrivate` box whose
    // `buffers` vector backs `array.buffers`. Appending the validity Arc may
    // move that vector, so the exported buffer pointer is refreshed.
    unsafe {
        let private = &mut *private;
        if private.buffers.is_empty() {
            return Err(GeometryError::new_err(
                "Arrow geometry export has no top-level validity buffer slot",
            ));
        }
        private.u8_buffers.push(validity);
        private.buffers[0] = private
            .u8_buffers
            .last()
            .expect("validity buffer was just pushed")
            .as_ptr()
            .cast();
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
    values: Arc<[f64]>,
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
        vec![OwnedBufferSlot::Null, OwnedBufferSlot::F64(values)],
        Vec::new(),
    )
}

pub(crate) fn coordinate_schema(axes: CoordinateAxes) -> SchemaNode {
    let mut children = vec![
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
    ];
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
    SchemaNode {
        format: "+s",
        name: "",
        metadata: None,
        children,
    }
}

pub(crate) fn coordinate_array(seq: &CoordSeq) -> PyResult<GometryArrowArray> {
    // Retain parent column Arcs and expose the sequence window via Arrow
    // offset/length. Identity and Windowed `CoordSeq` views share storage;
    // Gather materialization always produces a full-window seq first.
    let columns = seq.column_arcs();
    let start = columns.window.start;
    let len = columns.window.end - columns.window.start;
    let mut children = vec![
        primitive_f64_array_windowed(columns.xs, start, len)?,
        primitive_f64_array_windowed(columns.ys, start, len)?,
    ];
    if let Some(zs) = columns.zs {
        children.push(primitive_f64_array_windowed(zs, start, len)?);
    }
    if let Some(ms) = columns.ms {
        children.push(primitive_f64_array_windowed(ms, start, len)?);
    }
    make_array(len, vec![OwnedBufferSlot::Null], children)
}

pub(crate) fn list_array(
    offsets: Arc<[i32]>,
    child: GometryArrowArray,
) -> PyResult<GometryArrowArray> {
    let length = offsets.len().saturating_sub(1);
    list_array_windowed(offsets, child, 0, length)
}

/// List array over a full CSR offsets buffer with a logical row window.
///
/// Parent offsets and the child coordinate run stay shared; only the Arrow
/// shell records `offset`/`length`. Used for packed Identity (offset 0) and
/// Window (offset = row start) export without rebasing or gathering.
///
/// Offsets must be nonempty, non-negative, non-decreasing over the visible
/// window, and the window terminal must not exceed the child length.
pub(crate) fn list_array_windowed(
    offsets: Arc<[i32]>,
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
        vec![OwnedBufferSlot::Null, OwnedBufferSlot::I32(offsets)],
        vec![child],
    )
}

/// Binary array from owned offsets + data.
///
/// Offsets must be nonempty; **every** entry (not only the terminal) must be
/// non-negative and non-decreasing, and the terminal must lie within `data`.
/// Intermediate OOB/negative values (e.g. `[100, 2]` or `[-5, 0]`) are rejected.
pub(crate) fn binary_array(offsets: Arc<[i32]>, data: Arc<[u8]>) -> PyResult<GometryArrowArray> {
    if offsets.is_empty() {
        return Err(GeometryError::new_err(
            "Arrow binary offsets must contain at least one entry",
        ));
    }
    let length = offsets.len() - 1;
    crate::py::arrow::ensure_i32_offsets_monotonic(&offsets, 0, length, data.len())?;
    make_array(
        length,
        vec![
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
) -> PyResult<SchemaNode> {
    let extension_name = encoding.extension_name();
    let mut metadata = vec![("ARROW:extension:name".to_owned(), extension_name.to_owned())];
    if let Some(value) = extension_metadata_json(crs, epoch)? {
        metadata.push(("ARROW:extension:metadata".to_owned(), value));
    }
    Ok(SchemaNode {
        metadata: Some(metadata),
        ..storage
    })
}

pub(crate) fn extension_metadata_json(
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Option<String>> {
    let mut metadata = Map::new();
    if let Some(crs) = crs {
        let projjson = crate::crs::to_projjson(crs)?;
        let projjson = serde_json::from_str::<Value>(&projjson).map_err(|error| {
            ParseError::new_err(format!("invalid PROJJSON generated by PROJ: {error}"))
        })?;
        metadata.insert("crs".to_owned(), projjson);
        metadata.insert("crs_type".to_owned(), Value::String("projjson".to_owned()));
    }
    if let Some(epoch) = epoch {
        let value = Number::from_f64(epoch)
            .ok_or_else(|| GeometryError::new_err("coordinate epoch must be finite"))?;
        metadata.insert("epoch".to_owned(), Value::Number(value));
    }
    if metadata.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(&metadata).map(Some).map_err(|error| {
            GeometryError::new_err(format!("failed to encode GeoArrow metadata: {error}"))
        })
    }
}

pub(crate) fn list_schema(child: SchemaNode) -> SchemaNode {
    SchemaNode {
        format: "+l",
        name: "",
        metadata: None,
        children: vec![SchemaNode {
            name: "item",
            ..child
        }],
    }
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
    use super::*;

    fn ensure_python() {
        crate::test_support::initialize_python();
    }

    #[test]
    fn primitive_window_rejects_out_of_range() {
        ensure_python();
        let values: Arc<[f64]> = Arc::from([1.0_f64, 2.0, 3.0]);
        primitive_f64_array_windowed(Arc::clone(&values), 0, 3).unwrap();
        assert!(primitive_f64_array_windowed(values, 2, 2).is_err());
    }

    #[test]
    fn list_window_rejects_empty_and_oob_offsets() {
        ensure_python();
        let empty: Arc<[i32]> = Arc::from([]);
        let child = make_array(0, vec![OwnedBufferSlot::Null], Vec::new()).unwrap();
        assert!(list_array_windowed(empty, child, 0, 0).is_err());

        let offsets: Arc<[i32]> = Arc::from([0, 1, 2]);
        let child = make_array(0, vec![OwnedBufferSlot::Null], Vec::new()).unwrap();
        assert!(list_array_windowed(offsets, child, 0, 3).is_err());
    }

    #[test]
    fn list_rejects_non_monotonic_and_terminal_past_child() {
        ensure_python();
        let child = make_array(2, vec![OwnedBufferSlot::Null], Vec::new()).unwrap();
        // Decreasing: 0, 2, 1
        let offsets: Arc<[i32]> = Arc::from([0, 2, 1]);
        assert!(list_array_windowed(offsets, child, 0, 2).is_err());

        let child = make_array(2, vec![OwnedBufferSlot::Null], Vec::new()).unwrap();
        // Terminal 5 > child length 2
        let offsets: Arc<[i32]> = Arc::from([0, 5]);
        assert!(list_array_windowed(offsets, child, 0, 1).is_err());
    }

    #[test]
    fn binary_rejects_empty_offsets_and_overlong_terminal() {
        ensure_python();
        let empty: Arc<[i32]> = Arc::from([]);
        let data: Arc<[u8]> = Arc::from([1_u8, 2]);
        assert!(binary_array(empty, Arc::clone(&data)).is_err());

        let offsets: Arc<[i32]> = Arc::from([0, 9]);
        assert!(binary_array(offsets, data).is_err());

        let offsets: Arc<[i32]> = Arc::from([0, 2]);
        let data: Arc<[u8]> = Arc::from([1_u8, 2]);
        binary_array(offsets, data).unwrap();
    }

    #[test]
    fn binary_rejects_intermediate_oob_and_negative_offsets() {
        ensure_python();
        let data: Arc<[u8]> = Arc::from([1_u8, 2, 3, 4]);
        // Intermediate 100 > data.len(); terminal 2 is fine — still reject.
        let offsets: Arc<[i32]> = Arc::from([100, 2]);
        assert!(binary_array(Arc::clone(&offsets), Arc::clone(&data)).is_err());
        // Negative intermediate with non-negative terminal.
        let offsets: Arc<[i32]> = Arc::from([-5, 0]);
        assert!(binary_array(offsets, Arc::clone(&data)).is_err());
        // Non-monotonic positive chain.
        let offsets: Arc<[i32]> = Arc::from([0, 3, 1]);
        assert!(binary_array(offsets, data).is_err());
    }

    #[test]
    fn make_array_rejects_length_past_owned_f64_column() {
        ensure_python();
        // Same residual the skeptic named: free length over a short Arc.
        let short: Arc<[f64]> = Arc::from([1.0_f64]);
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
        let mut array = make_array(2, vec![OwnedBufferSlot::Null], Vec::new()).unwrap();
        assert!(apply_top_level_validity(&mut array, &[false]).is_err());
        apply_top_level_validity(&mut array, &[false, true]).unwrap();
    }
}
