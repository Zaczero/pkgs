use pyo3::types::PyDict;

use crate::collections::{HashMap, HashMapExt as _};
use crate::py::arrow::{
    AdmittedBuffer, ArrowListLevel, ArrowStorage, ArrowValidity, Bound, Crs, EmptyKind, Frame,
    LineSeq, PyAny, PyAnyMethods as _, PyDictMethods as _, PyErr, PyGeometry, PyGeometryArray,
    PyResult, Python, Shape, WkbOffsetWidth, arrow_array_offset, arrow_binary_data_buffer_len,
    arrow_binary_data_span_admitted, arrow_buffer_span_admitted, arrow_coordinate_values,
    arrow_i32_offsets_window, arrow_i64_offsets_window, arrow_polygon_from_ring_range,
    arrow_validity, arrow_validity_window, coordinate_span, crs_arc, crs_arc_str,
    ensure_i32_offsets_monotonic, ensure_i64_offsets_monotonic,
    ensure_offset_terminal_within_child, geoarrow_parse_error, i32_offset_to_usize,
    i64_offset_to_usize, io, push_geometry_level_missing, reject_inner_nulls_in_range,
};
use crate::py::wire_crs::guard_embedded_crs_conflict;

pub(crate) fn append_arrow_multilinestrings(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    let len = storage.len()?;
    // Geometry-level (outer) nulls → missing rows; inner list nulls are D05.
    let validity = arrow_validity(py, storage)?;
    let geometry_level = ArrowListLevel::read(py, storage)?;
    geometry_level.ensure(0, len)?;
    let lines = storage.getattr("values")?;
    // Inner line slots: GeoArrow forbids nulls here (outer geometry nulls only).
    let line_start = geometry_level.endpoint(0)?;
    let line_end = geometry_level.endpoint(len)?;
    let line_count = line_end
        .checked_sub(line_start)
        .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?;
    let line_validity = arrow_validity_window(py, &lines, line_start, line_count)?;
    let line_level = ArrowListLevel::read_selected(py, &lines, line_start, line_count)?;
    // Follow geometry → line → coordinate offsets at the slice endpoints to the
    // visible coordinate span.
    let (base, span) = coordinate_span(
        line_level.endpoint(geometry_level.endpoint(0)?)?,
        line_level.endpoint(geometry_level.endpoint(len)?)?,
    )?;
    let coordinates = arrow_coordinate_values(py, &lines.getattr("values")?, base, span)?;
    // Empty line members (equal offsets) are legal; protection is fallible
    // `try_reserve` on the decoded total, not a magic input-relative ratio.
    let crs = crs.map(crs_arc_str);
    for index in 0..len {
        if !validity.is_valid(index) {
            push_geometry_level_missing(geometries, missing_rows, *row, crs.clone());
            *row += 1;
            continue;
        }
        let range = geometry_level.range(index)?;
        line_level.ensure(range.start, range.len())?;
        // Reject null line members before reading offsets/payload (empty
        // offset-equal members stay legal).
        reject_inner_nulls_in_range(
            &line_validity,
            range
                .start
                .checked_sub(line_start)
                .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?,
            range.len(),
        )?;
        let mut linework = crate::try_vec_with_capacity(range.len())?;
        for line_index in range {
            let line = line_level.range(line_index)?;
            let line = coordinates.coordseq(line.start, line.end, *row)?;
            linework.push(LineSeq::try_new(line).map_err(PyErr::from)?);
        }
        // Empty multiparts keep SCHEMA axes (Z/M/ZM); bare `Vec::new()` is XY-only.
        let shape = if linework.is_empty() {
            Shape::typed_empty(EmptyKind::MultiLineString, coordinates.axes())
        } else {
            Shape::MultiLineString(linework)
        };
        geometries.push(PyGeometry::from_shape_crs(shape, crs.clone()));
        *row += 1;
    }
    Ok(())
}

pub(crate) fn append_arrow_polygons(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    let len = storage.len()?;
    let validity = arrow_validity(py, storage)?;
    let polygon_level = ArrowListLevel::read(py, storage)?;
    polygon_level.ensure(0, len)?;
    let rings = storage.getattr("values")?;
    let ring_start = polygon_level.endpoint(0)?;
    let ring_end = polygon_level.endpoint(len)?;
    let ring_count = ring_end
        .checked_sub(ring_start)
        .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?;
    let ring_validity = arrow_validity_window(py, &rings, ring_start, ring_count)?;
    let ring_level = ArrowListLevel::read_selected(py, &rings, ring_start, ring_count)?;
    // Follow polygon → ring → coordinate offsets at the slice endpoints.
    let (base, span) = coordinate_span(
        ring_level.endpoint(polygon_level.endpoint(0)?)?,
        ring_level.endpoint(polygon_level.endpoint(len)?)?,
    )?;
    let coordinates = arrow_coordinate_values(py, &rings.getattr("values")?, base, span)?;
    let crs = crs.map(crs_arc_str);
    for index in 0..len {
        if !validity.is_valid(index) {
            push_geometry_level_missing(geometries, missing_rows, *row, crs.clone());
            *row += 1;
            continue;
        }
        let range = polygon_level.range(index)?;
        ring_level.ensure(range.start, range.len())?;
        reject_inner_nulls_in_range(
            &ring_validity,
            range
                .start
                .checked_sub(ring_start)
                .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?,
            range.len(),
        )?;
        let shape = arrow_polygon_from_ring_range(&coordinates, &ring_level, range, *row)?;
        geometries.push(PyGeometry::from_shape_crs(shape, crs.clone()));
        *row += 1;
    }
    Ok(())
}

pub(crate) fn append_arrow_multipolygons(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    let len = storage.len()?;
    let validity = arrow_validity(py, storage)?;
    let multipolygon_level = ArrowListLevel::read(py, storage)?;
    multipolygon_level.ensure(0, len)?;
    let polygons = storage.getattr("values")?;
    let polygon_start = multipolygon_level.endpoint(0)?;
    let polygon_end = multipolygon_level.endpoint(len)?;
    let polygon_count = polygon_end
        .checked_sub(polygon_start)
        .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?;
    let polygon_validity = arrow_validity_window(py, &polygons, polygon_start, polygon_count)?;
    let polygon_level = ArrowListLevel::read_selected(py, &polygons, polygon_start, polygon_count)?;
    let rings = polygons.getattr("values")?;
    let ring_start = polygon_level.endpoint(polygon_start)?;
    let ring_end = polygon_level.endpoint(polygon_end)?;
    let ring_count = ring_end
        .checked_sub(ring_start)
        .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?;
    let ring_validity = arrow_validity_window(py, &rings, ring_start, ring_count)?;
    let ring_level = ArrowListLevel::read_selected(py, &rings, ring_start, ring_count)?;
    // Follow multipolygon → polygon → ring → coordinate offsets at the slice
    // endpoints to the visible coordinate span.
    let coordinate_endpoint = |position: usize| -> PyResult<usize> {
        ring_level.endpoint(polygon_level.endpoint(multipolygon_level.endpoint(position)?)?)
    };
    let (base, span) = coordinate_span(coordinate_endpoint(0)?, coordinate_endpoint(len)?)?;
    let coordinates = arrow_coordinate_values(py, &rings.getattr("values")?, base, span)?;
    // Rings are structurally validated by `Ring::closed` (min vertices); empty
    // multipolygon members drop. Protection is fallible `try_reserve`, not a
    // magic input-relative structure ratio.
    let crs = crs.map(crs_arc_str);
    for index in 0..len {
        if !validity.is_valid(index) {
            push_geometry_level_missing(geometries, missing_rows, *row, crs.clone());
            *row += 1;
            continue;
        }
        let range = multipolygon_level.range(index)?;
        polygon_level.ensure(range.start, range.len())?;
        reject_inner_nulls_in_range(
            &polygon_validity,
            range
                .start
                .checked_sub(polygon_start)
                .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?,
            range.len(),
        )?;
        let mut items = crate::try_vec_with_capacity(range.len())?;
        for polygon_index in range {
            let ring_range = polygon_level.range(polygon_index)?;
            ring_level.ensure(ring_range.start, ring_range.len())?;
            reject_inner_nulls_in_range(
                &ring_validity,
                ring_range
                    .start
                    .checked_sub(ring_start)
                    .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?,
                ring_range.len(),
            )?;
            match arrow_polygon_from_ring_range(&coordinates, &ring_level, ring_range, *row)? {
                Shape::Polygon(polygon) => items.push(polygon),
                // Nested empty polygon members drop (WKB/WKT normalization);
                // axes still come from the multipolygon schema when all drop.
                Shape::Empty(EmptyKind::Polygon, _) => {},
                other => {
                    return Err(geoarrow_parse_error(format!(
                        "Arrow multipolygon member decoded as unexpected shape {other:?}"
                    )));
                },
            }
        }
        let shape = if items.is_empty() {
            Shape::typed_empty(EmptyKind::MultiPolygon, coordinates.axes())
        } else {
            Shape::MultiPolygon(items)
        };
        geometries.push(PyGeometry::from_shape_crs(shape, crs.clone()));
        *row += 1;
    }
    Ok(())
}

/// One admitted WKB Arrow window: single offsets materialization + admitted
/// data span (owned Arc snapshot) + the Arrow validity bitmap (not
/// expanded to `Vec<bool>`).
///
/// Decode walks adjacent admitted offsets straight into present shapes and a
/// missing-row index list — no `ranges` Vec, no `Vec<Option<WkbGeometry>>`.
pub(crate) struct AdmittedWkbPlan {
    /// `len + 1` offsets rebased to the owned visible data window.
    offsets: Vec<usize>,
    /// Contiguous owned data window covering every row payload.
    data: AdmittedBuffer,
    /// Geometry-level validity (bitmap + bit offset); no per-row `Vec<bool>`.
    validity: ArrowValidity,
    len: usize,
}

/// Present-only WKB decode result: shapes and SRIDs align 1:1; missing rows
/// are only the local indices in `missing_local`.
pub(crate) struct WkbPresentDecode {
    shapes: Vec<Shape>,
    /// Embedded EWKB SRID per present shape (`None` = plain WKB / SRID 0).
    srids: Vec<Option<u32>>,
    /// Local (0..chunk_len) indices of geometry-level nulls.
    missing_local: Vec<usize>,
    /// Full logical chunk length (present + missing).
    chunk_len: usize,
}

impl AdmittedWkbPlan {
    /// Layout admission + one owned data copy. This is the sole Arrow offset
    /// materialization for pure offset-based WKB import (no prior ensure walk).
    /// WKB syntax is the second validation layer in [`Self::decode_present`].
    pub(crate) fn admit(
        py: Python<'_>,
        storage: &Bound<'_, PyAny>,
        wkb_offset_width: WkbOffsetWidth,
    ) -> PyResult<Self> {
        let len = storage.len()?;
        let validity = if len == 0 {
            ArrowValidity {
                bitmap: None,
                offset: 0,
            }
        } else {
            arrow_validity(py, storage)?
        };
        let offset = arrow_array_offset(storage)?;
        // D18: even length-0 Binary/LargeBinary retains one start offset that
        // must be non-negative — validate before any early empty return.
        let offsets = if matches!(wkb_offset_width, WkbOffsetWidth::Int64) {
            let slots = len
                .checked_add(1)
                .ok_or_else(|| geoarrow_parse_error("Arrow offset count overflows"))?;
            let raw = arrow_i64_offsets_window(py, storage, offset, slots)?;
            ensure_i64_offsets_monotonic(&raw, 0, len, usize::MAX)?;
            if len == 0 {
                return Ok(Self {
                    offsets: vec![0],
                    data: AdmittedBuffer::from_vec(Vec::new()),
                    validity,
                    len: 0,
                });
            }
            let data_len = arrow_binary_data_buffer_len(storage)?;
            let terminal = i64_offset_to_usize(raw[len])?;
            ensure_offset_terminal_within_child(terminal, data_len)?;
            (0..=len)
                .map(|idx| i64_offset_to_usize(raw[idx]))
                .collect::<PyResult<Vec<_>>>()?
        } else {
            let slots = len
                .checked_add(1)
                .ok_or_else(|| geoarrow_parse_error("Arrow offset count overflows"))?;
            let raw = arrow_i32_offsets_window(py, storage, offset, slots)?;
            ensure_i32_offsets_monotonic(&raw, 0, len, usize::MAX)?;
            if len == 0 {
                return Ok(Self {
                    offsets: vec![0],
                    data: AdmittedBuffer::from_vec(Vec::new()),
                    validity,
                    len: 0,
                });
            }
            let data_len = arrow_binary_data_buffer_len(storage)?;
            let terminal = i32_offset_to_usize(raw[len])?;
            ensure_offset_terminal_within_child(terminal, data_len)?;
            (0..=len)
                .map(|idx| i32_offset_to_usize(raw[idx]))
                .collect::<PyResult<Vec<_>>>()?
        };
        let data_base = offsets[0];
        let data_end = offsets[len];
        // Native capsules expose the owned admission snapshot for the visible
        // span — no second Python-buffer fetch of multi-MB WKB payloads.
        let data = arrow_binary_data_span_admitted(py, storage, data_base, data_end)?;
        let offsets = offsets
            .into_iter()
            .map(|value| {
                value
                    .checked_sub(data_base)
                    .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            offsets,
            data,
            validity,
            len,
        })
    }

    /// Walk adjacent admitted offsets: nulls → `missing_local`, present rows
    /// → parse WKB into `shapes`/`srids` (no `Option` wall, no ranges Vec).
    ///
    /// Batch decode shares the exact final-Arc coordinate decoder (one grammar
    /// path — no pre-scan).
    pub(crate) fn decode_present(self) -> crate::error::Result<WkbPresentDecode> {
        let Self {
            offsets,
            data,
            validity,
            len,
        } = self;
        let data_bytes = data.as_slice();
        let mut shapes = Vec::with_capacity(len);
        let mut srids = Vec::with_capacity(len);
        let mut missing_local = Vec::new();
        let arena = io::WkbCoordArena::new();
        for index in 0..len {
            if !validity.is_valid(index) {
                missing_local.push(index);
                continue;
            }
            let start = offsets[index];
            let end = offsets[index + 1];
            if start > end {
                return Err(io::IoError::wkb("Arrow WKB offsets are out of bounds"));
            }
            if end > data_bytes.len() {
                return Err(io::IoError::wkb("Arrow WKB offsets are out of bounds"));
            }
            let parsed = io::parse_wkb_batch(&data_bytes[start..end], &arena)?;
            shapes.push(parsed.shape);
            srids.push(parsed.srid);
        }
        debug_assert_eq!(shapes.len() + missing_local.len(), len);
        Ok(WkbPresentDecode {
            shapes,
            srids,
            missing_local,
            chunk_len: len,
        })
    }
}

pub(crate) fn append_arrow_wkb(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    crs: Option<&str>,
    wkb_offset_width: WkbOffsetWidth,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    if matches!(wkb_offset_width, WkbOffsetWidth::View) {
        return append_arrow_wkb_binary_view(py, storage, crs, geometries, row, missing_rows);
    }
    let plan = AdmittedWkbPlan::admit(py, storage, wkb_offset_width)?;
    if plan.len == 0 {
        return Ok(());
    }
    let decoded = py.detach(move || plan.decode_present())?;
    // Expand present-only decode into the mixed-encoding PyGeometry sink
    // (legacy append path); pure-WKB import uses `import_arrow_wkb_shapes`.
    push_present_wkb_geometries(decoded, crs, geometries, row, missing_rows)
}

/// Expand a present-only WKB decode into the row-aligned `PyGeometry` sink
/// used by mixed-encoding Arrow import.
fn push_present_wkb_geometries(
    decoded: WkbPresentDecode,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    let base_crs = crs.map(crs_arc_str);
    let mut srid_cache = io::SridCrsCache::default();
    let mut present = decoded.shapes.into_iter().zip(decoded.srids);
    let mut missing = decoded.missing_local.into_iter().peekable();
    for local in 0..decoded.chunk_len {
        if missing.peek() == Some(&local) {
            missing.next();
            missing_rows.push(*row);
            geometries.push(PyGeometry::from_shape_crs(
                crate::PyGeometryArray::missing_placeholder(),
                base_crs.clone(),
            ));
        } else {
            let (shape, srid) = present
                .next()
                .expect("present count matches chunk_len - missing");
            let embedded = srid_cache.resolve(srid)?;
            guard_embedded_crs_conflict(embedded.as_deref(), crs, "EWKB SRID")?;
            let row_crs = base_crs.clone().or_else(|| embedded.map(crs_arc));
            geometries.push(PyGeometry::from_shape_crs(shape, row_crs));
        }
        *row += 1;
    }
    Ok(())
}

/// Stream admitted WKB chunks straight into array-owned shapes (no
/// per-row `PyGeometry` wall). Offset-based chunks admit once (layout + data)
/// and decode rows in final order (missing placeholders included) so a second
/// scatter pass is never needed.
pub(crate) fn import_arrow_wkb_shapes(
    py: Python<'_>,
    storages: &[ArrowStorage],
    explicit_crs: Option<Crs>,
    epoch: Option<f64>,
) -> PyResult<PyGeometryArray> {
    use crate::py::wire_crs::SridFrameAdmission;

    // Final-order shapes with kind-preserving missing placeholders (Stage 3).
    // Null rows establish no CRS and are never parsed. Numeric SRID admission
    // replaces SharedRowCrs clone/compare. No scatter_present_rows.
    let mut rows = crate::array::StreamingShapes::new();
    let mut frame = SridFrameAdmission::new(explicit_crs.clone(), None);
    let mut missing_rows: Vec<usize> = Vec::new();
    let mut row = 0_usize;

    for storage in storages {
        let storage_array = storage.storage.bind(py);
        let storage_crs = storage.crs.as_deref();
        // Storage-level CRS (Arrow metadata) acts like an explicit frame when
        // the caller did not pass crs=.
        if explicit_crs.is_none()
            && let Some(crs) = storage_crs
        {
            frame.set_storage_crs(Some(crs_arc_str(crs)));
        }
        if matches!(storage.wkb_offset_width, WkbOffsetWidth::View) {
            row += stream_binary_view_chunk(
                py,
                storage_array,
                storage_crs,
                row,
                &mut rows,
                &mut frame,
                &mut missing_rows,
            )?;
            continue;
        }
        // Single admit: layout validation + offsets + data. No prior ensure walk.
        let plan = AdmittedWkbPlan::admit(py, storage_array, storage.wkb_offset_width)?;
        let decoded = py.detach(move || plan.decode_present())?;
        row += stream_present_wkb_decode(
            decoded,
            storage_crs,
            row,
            &mut rows,
            &mut frame,
            &mut missing_rows,
        )?;
    }

    let crs = frame.finish()?;
    crate::guard_epoch_frame(epoch, crs.as_ref())?;
    let array = rows.finish(Frame::new(crs, epoch)?);
    if let Some(mask) = crate::array::sparse_missing_mask(row, &missing_rows) {
        Ok(array.with_missing_mask(Some(mask)))
    } else {
        Ok(array)
    }
}

/// Fold one offset-based admitted decode into the final-order streaming sink;
/// returns the chunk length advanced. Missing → kind-preserving placeholder.
fn stream_present_wkb_decode(
    decoded: WkbPresentDecode,
    storage_crs: Option<&str>,
    row_base: usize,
    rows: &mut crate::array::StreamingShapes,
    frame: &mut crate::py::wire_crs::SridFrameAdmission,
    missing_rows: &mut Vec<usize>,
) -> PyResult<usize> {
    let mut present = decoded.shapes.into_iter().zip(decoded.srids);
    let mut missing = decoded.missing_local.into_iter().peekable();
    let chunk_len = decoded.chunk_len;
    for local in 0..chunk_len {
        let absolute = row_base + local;
        if missing.peek() == Some(&local) {
            missing.next();
            crate::try_push(missing_rows, absolute)?;
            rows.try_push_missing()?;
            continue;
        }
        let (shape, srid) = present
            .next()
            .expect("present count matches chunk_len - missing");
        frame.admit_srid(srid, absolute, "Arrow import", "EWKB SRID")?;
        if storage_crs.is_some() && srid.is_some() {
            frame.guard_storage_srid(srid, storage_crs, absolute)?;
        }
        rows.try_push(shape)?;
    }
    Ok(chunk_len)
}

/// BinaryView WKB chunk → present-only stream; returns chunk length advanced.
///
/// Decodes WKB straight into `Shape` + raw SRID (no `PyGeometry` round-trip).
fn stream_binary_view_chunk(
    py: Python<'_>,
    storage_array: &Bound<'_, PyAny>,
    storage_crs: Option<&str>,
    row_base: usize,
    rows: &mut crate::array::StreamingShapes,
    frame: &mut crate::py::wire_crs::SridFrameAdmission,
    missing_rows: &mut Vec<usize>,
) -> PyResult<usize> {
    let decoded = decode_binary_view_wkb_present(py, storage_array)?;
    stream_present_wkb_decode(decoded, storage_crs, row_base, rows, frame, missing_rows)
}

/// Admit BinaryView descriptors + referenced buffers, parse WKB in final
/// order into the same `WkbPresentDecode` shape as offset-based WKB.
#[expect(
    clippy::iter_over_hash_type,
    reason = "buffer-map iteration only builds keyed borrow lookups; decoded rows retain descriptor order"
)]
fn decode_binary_view_wkb_present(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
) -> PyResult<WkbPresentDecode> {
    if is_direct_pyarrow_binary_view(storage)? {
        return decode_direct_pyarrow_binary_view_wkb_present(py, storage);
    }
    let (rows, data_buffers) = admit_binary_view_rows(py, storage)?;
    let len = rows.len();
    if len == 0 {
        return Ok(WkbPresentDecode {
            shapes: Vec::new(),
            srids: Vec::new(),
            missing_local: Vec::new(),
            chunk_len: 0,
        });
    }
    py.detach(move || {
        // Keep admitted owned snapshots alive through detach and resolve their
        // slices once. Parsing borrows referenced buffers (no full into_owned).
        let mut resolved: HashMap<usize, &[u8]> = HashMap::new();
        for (index, buf) in &data_buffers {
            resolved.insert(*index, buf.as_slice());
        }
        let mut shapes = Vec::with_capacity(len);
        let mut srids = Vec::with_capacity(len);
        let mut missing_local = Vec::new();
        let arena = io::WkbCoordArena::new();
        for (idx, row) in rows.into_iter().enumerate() {
            match row {
                WkbRowBytes::Missing => missing_local.push(idx),
                WkbRowBytes::Inline { bytes, len } => {
                    let parsed = io::parse_wkb_batch(&bytes[..len], &arena)?;
                    shapes.push(parsed.shape);
                    srids.push(parsed.srid);
                },
                WkbRowBytes::Span {
                    index, start, end, ..
                } => {
                    let data = resolved
                        .get(&index)
                        .expect("descriptor references a loaded binary-view buffer");
                    let parsed = io::parse_wkb_batch(
                        data.get(start..end)
                            .expect("binary-view range validated before detach"),
                        &arena,
                    )?;
                    shapes.push(parsed.shape);
                    srids.push(parsed.srid);
                },
            }
        }
        Ok(WkbPresentDecode {
            shapes,
            srids,
            missing_local,
            chunk_len: len,
        })
    })
}

/// Row-aligned BinaryView / WKB payload carrier: nulls never parse, inline and
/// referenced spans share one representation with offset-based WKB.
enum WkbRowBytes {
    Missing,
    Inline {
        bytes: [u8; 12],
        len: usize,
    },
    /// Referenced BinaryView data buffer index + admitted local byte range.
    Span {
        index: usize,
        start: usize,
        end: usize,
        prefix: [u8; 4],
    },
}

/// Decode a BinaryView descriptor's control fields. Payload is admitted only
/// after all selected views have projected their referenced byte envelopes.
fn parse_binary_view_descriptor(view: &[u8; 16]) -> PyResult<WkbRowBytes> {
    let length = i32::from_le_bytes([view[0], view[1], view[2], view[3]]);
    let length = usize::try_from(length)
        .map_err(|_| geoarrow_parse_error("Arrow binary-view length is negative"))?;
    if length <= 12 {
        if view[4 + length..16].iter().any(|&b| b != 0) {
            return Err(geoarrow_parse_error(
                "Arrow binary-view inline padding must be zero",
            ));
        }
        let mut bytes = [0_u8; 12];
        bytes[..length].copy_from_slice(&view[4..4 + length]);
        return Ok(WkbRowBytes::Inline { bytes, len: length });
    }
    let buffer_index = i32::from_le_bytes([view[8], view[9], view[10], view[11]]);
    let buffer_index = usize::try_from(buffer_index)
        .map_err(|_| geoarrow_parse_error("Arrow binary-view buffer index is negative"))?;
    let byte_offset = i32::from_le_bytes([view[12], view[13], view[14], view[15]]);
    let byte_offset = usize::try_from(byte_offset)
        .map_err(|_| geoarrow_parse_error("Arrow binary-view byte offset is negative"))?;
    let byte_end = byte_offset
        .checked_add(length)
        .ok_or_else(|| geoarrow_parse_error("Arrow binary-view byte range overflows"))?;
    let prefix = [view[4], view[5], view[6], view[7]];
    Ok(WkbRowBytes::Span {
        index: buffer_index,
        start: byte_offset,
        end: byte_end,
        prefix,
    })
}

/// Snapshot BinaryView's selected descriptor window, then the smallest
/// enclosing payload span for each referenced variadic buffer. The descriptor
/// offsets are rebased to those owned payload windows before the GIL is
/// released, so neither the parent descriptor buffer nor an unrelated payload
/// prefix survives import.
fn snapshot_binary_view_descriptors(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    view_len: usize,
) -> PyResult<AdmittedBuffer> {
    let view_start = arrow_array_offset(storage)?
        .checked_mul(16)
        .ok_or_else(|| geoarrow_parse_error("Arrow binary-view offset overflows"))?;
    let view_end = view_start
        .checked_add(view_len)
        .ok_or_else(|| geoarrow_parse_error("Arrow binary-view length overflows"))?;
    arrow_buffer_span_admitted(py, storage, 1, "binary-view views", view_start, view_end)
}

#[expect(
    clippy::iter_over_hash_type,
    reason = "windows are materialized into a keyed buffer map and all later work follows descriptor row order"
)]
fn admit_binary_view_rows(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
) -> PyResult<(Vec<WkbRowBytes>, HashMap<usize, AdmittedBuffer>)> {
    let len = storage.len()?;
    if len == 0 {
        return Ok((Vec::new(), HashMap::new()));
    }
    let validity = arrow_validity(py, storage)?;
    let view_len = len
        .checked_mul(16)
        .ok_or_else(|| geoarrow_parse_error("Arrow binary-view length overflows"))?;
    let views = snapshot_binary_view_descriptors(py, storage, view_len)?;
    let views_bytes = views.as_slice();
    if views_bytes.len() != view_len {
        return Err(geoarrow_parse_error(
            "Arrow binary-view buffer is shorter than declared array length",
        ));
    }
    let mut rows = crate::try_vec_with_capacity(len)?;
    let mut windows: HashMap<usize, std::ops::Range<usize>> = HashMap::new();
    for idx in 0..len {
        if !validity.is_valid(idx) {
            rows.push(WkbRowBytes::Missing);
            continue;
        }
        let start = idx * 16;
        let descriptor = views_bytes[start..start + 16]
            .first_chunk::<16>()
            .expect("the checked descriptor range is exactly 16 bytes");
        let row = parse_binary_view_descriptor(descriptor)?;
        if let WkbRowBytes::Span {
            index, start, end, ..
        } = &row
        {
            windows
                .entry(*index)
                .and_modify(|window| {
                    window.start = window.start.min(*start);
                    window.end = window.end.max(*end);
                })
                .or_insert(*start..*end);
        }
        rows.push(row);
    }
    drop(views);
    let mut data_buffers = HashMap::new();
    for (&index, window) in &windows {
        data_buffers.insert(
            index,
            arrow_buffer_span_admitted(
                py,
                storage,
                2_usize.checked_add(index).ok_or_else(|| {
                    geoarrow_parse_error("Arrow binary-view buffer index overflows")
                })?,
                "binary-view data",
                window.start,
                window.end,
            )?,
        );
    }
    for row in &mut rows {
        let WkbRowBytes::Span {
            index,
            start,
            end,
            prefix,
        } = row
        else {
            continue;
        };
        let window = windows
            .get(index)
            .expect("every BinaryView span has an admitted window");
        let data = data_buffers
            .get(index)
            .expect("every BinaryView span has an admitted buffer")
            .as_slice();
        let local_start = start
            .checked_sub(window.start)
            .expect("window starts at or before its span");
        let local_end = end
            .checked_sub(window.start)
            .expect("window starts at or before its span");
        let payload = data.get(local_start..local_end).ok_or_else(|| {
            geoarrow_parse_error("Arrow binary-view data buffer is shorter than declared views")
        })?;
        if payload.len() < 4 || *prefix != payload[..4] {
            return Err(geoarrow_parse_error(
                "Arrow binary-view prefix does not match referenced data",
            ));
        }
        *start = local_start;
        *end = local_end;
    }
    Ok((rows, data_buffers))
}

#[expect(
    clippy::iter_over_hash_type,
    reason = "buffer-map iteration only builds keyed borrow lookups; emitted geometry rows retain descriptor order"
)]
fn append_arrow_wkb_binary_view(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    if is_direct_pyarrow_binary_view(storage)? {
        return push_present_wkb_geometries(
            decode_direct_pyarrow_binary_view_wkb_present(py, storage)?,
            crs,
            geometries,
            row,
            missing_rows,
        );
    }
    let (rows, data_buffers) = admit_binary_view_rows(py, storage)?;
    if rows.is_empty() {
        return Ok(());
    }
    let parsed = py.detach(move || {
        // Resolve owned slices once per referenced buffer for the detach scope.
        let mut resolved: HashMap<usize, &[u8]> = HashMap::new();
        for (index, buf) in &data_buffers {
            resolved.insert(*index, buf.as_slice());
        }
        let arena = io::WkbCoordArena::new();
        rows.into_iter()
            .map(|row| {
                let bytes = match &row {
                    WkbRowBytes::Missing => return Ok(None),
                    WkbRowBytes::Inline { bytes, len } => &bytes[..*len],
                    WkbRowBytes::Span {
                        index, start, end, ..
                    } => resolved
                        .get(index)
                        .expect("descriptor references a loaded binary-view buffer")
                        .get(*start..*end)
                        .expect("binary-view range validated before detach"),
                };
                io::parse_wkb_batch(bytes, &arena).map(Some)
            })
            .collect::<Result<Vec<_>, _>>()
    })?;
    push_parsed_wkb_geometries(parsed, crs, geometries, row, missing_rows)
}

/// Real PyArrow BinaryView arrays expose scalar lookup through their C++
/// owner, while their public ``buffers()`` and Arrow-C export both materialize
/// the complete variadic parent table.  Validate the *visible slice* with the
/// C++ owner, then copy each visible scalar WKB value.  This preserves
/// BinaryView's descriptor/prefix/padding validation without turning a
/// one-row import into work proportional to unrelated parent fragments.
fn is_direct_pyarrow_binary_view(storage: &Bound<'_, PyAny>) -> PyResult<bool> {
    if crate::py::arrow_c::is_native_arrow_array(storage) {
        return Ok(false);
    }
    let class = storage.get_type();
    Ok(
        class.getattr("__module__")?.extract::<String>()? == "pyarrow.lib"
            && class.getattr("__name__")?.extract::<String>()? == "BinaryViewArray",
    )
}

fn decode_direct_pyarrow_binary_view_wkb_present(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
) -> PyResult<WkbPresentDecode> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("full", true)?;
    storage
        .call_method("validate", (), Some(&kwargs))
        .map_err(|error| {
            geoarrow_parse_error(format!("Arrow binary-view validation failed: {error}"))
        })?;
    let len = storage.len()?;
    let mut values = crate::try_vec_with_capacity(len)?;
    for index in 0..len {
        let value = storage.get_item(index)?.call_method0("as_py")?;
        if value.is_none() {
            values.push(None);
        } else {
            values.push(Some(value.extract::<Vec<u8>>()?));
        }
    }
    let parsed = py.detach(move || -> crate::error::Result<WkbPresentDecode> {
        let arena = io::WkbCoordArena::new();
        let mut shapes = Vec::with_capacity(values.len());
        let mut srids = Vec::with_capacity(values.len());
        let mut missing_local = Vec::new();
        for (index, value) in values.iter().enumerate() {
            let Some(bytes) = value.as_deref() else {
                missing_local.push(index);
                continue;
            };
            let parsed = io::parse_wkb_batch(bytes, &arena)?;
            shapes.push(parsed.shape);
            srids.push(parsed.srid);
        }
        Ok(WkbPresentDecode {
            shapes,
            srids,
            missing_local,
            chunk_len: values.len(),
        })
    })?;
    Ok(parsed)
}

/// Shared standard/view WKB finalizer: CRS reconciliation + missing-row scatter.
pub(crate) fn push_parsed_wkb_geometries(
    parsed: impl IntoIterator<Item = Option<io::WkbGeometry>>,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    let base_crs = crs.map(crs_arc_str);
    // One canonicalize per distinct embedded SRID for the batch (not per row).
    let mut srid_cache = io::SridCrsCache::default();
    for parsed in parsed {
        if let Some(parsed) = parsed {
            let embedded = srid_cache.resolve(parsed.srid)?;
            guard_embedded_crs_conflict(embedded.as_deref(), crs, "EWKB SRID")?;
            let row_crs = base_crs.clone().or_else(|| embedded.map(crs_arc));
            geometries.push(PyGeometry::from_shape_crs(parsed.shape, row_crs));
        } else {
            missing_rows.push(*row);
            geometries.push(PyGeometry::from_shape_crs(
                crate::PyGeometryArray::missing_placeholder(),
                base_crs.clone(),
            ));
        }
        *row += 1;
    }
    Ok(())
}
