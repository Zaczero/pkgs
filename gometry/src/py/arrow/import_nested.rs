use crate::collections::{HashMap, HashMapExt};
use crate::py::arrow::*;

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
    let line_validity = arrow_validity(py, &lines)?;
    let line_level = ArrowListLevel::read(py, &lines)?;
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
        reject_inner_nulls_in_range(&line_validity, range.start, range.len())?;
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
    let ring_validity = arrow_validity(py, &rings)?;
    let ring_level = ArrowListLevel::read(py, &rings)?;
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
        reject_inner_nulls_in_range(&ring_validity, range.start, range.len())?;
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
    let polygon_validity = arrow_validity(py, &polygons)?;
    let polygon_level = ArrowListLevel::read(py, &polygons)?;
    let rings = polygons.getattr("values")?;
    let ring_validity = arrow_validity(py, &rings)?;
    let ring_level = ArrowListLevel::read(py, &rings)?;
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
        reject_inner_nulls_in_range(&polygon_validity, range.start, range.len())?;
        let mut items = crate::try_vec_with_capacity(range.len())?;
        for polygon_index in range {
            let ring_range = polygon_level.range(polygon_index)?;
            ring_level.ensure(ring_range.start, ring_range.len())?;
            reject_inner_nulls_in_range(&ring_validity, ring_range.start, ring_range.len())?;
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
    let len = storage.len()?;
    // Empty Binary/LargeBinary: no rows to decode; skip offsets/data lookup
    // (zero-sized data buffers may be null pointers).
    if len == 0 {
        return Ok(());
    }
    // Geometry-level nulls become missing rows (their offsets are
    // zero-length): remember them and skip the parse per row.
    let validity = arrow_validity(py, storage)?;
    let valid_rows: Vec<bool> = (0..len).map(|index| validity.is_valid(index)).collect();
    let offset = arrow_array_offset(storage)?;
    // m01: full-window monotonicity including null slots (not just present rows).
    let ranges = if matches!(wkb_offset_width, WkbOffsetWidth::Int64) {
        let offsets = arrow_i64_offsets(py, storage)?;
        // Offsets first; data-buffer length is trusted only after terminals are
        // non-negative (native schema-derived sizes read the terminal).
        ensure_i64_offsets_monotonic(&offsets, offset, len, usize::MAX)?;
        let data_len = arrow_binary_data_buffer_len(storage)?;
        let terminal = i64_offset_to_usize(offsets[offset + len])?;
        ensure_offset_terminal_within_child(terminal, data_len)?;
        (0..len)
            .map(|idx| {
                Ok((
                    i64_offset_to_usize(offsets[offset + idx])?,
                    i64_offset_to_usize(offsets[offset + idx + 1])?,
                ))
            })
            .collect::<PyResult<Vec<_>>>()?
    } else {
        let offsets = arrow_i32_offsets(py, storage)?;
        ensure_i32_offsets_monotonic(&offsets, offset, len, usize::MAX)?;
        let data_len = arrow_binary_data_buffer_len(storage)?;
        let terminal = i32_offset_to_usize(offsets[offset + len])?;
        ensure_offset_terminal_within_child(terminal, data_len)?;
        (0..len)
            .map(|idx| {
                Ok((
                    i32_offset_to_usize(offsets[offset + idx])?,
                    i32_offset_to_usize(offsets[offset + idx + 1])?,
                ))
            })
            .collect::<PyResult<Vec<_>>>()?
    };
    let byte_start = ranges.iter().map(|(start, _)| *start).min().unwrap_or(0);
    let byte_end = ranges.iter().map(|(_, end)| *end).max().unwrap_or(0);
    let data = arrow_binary_data_span(py, storage, byte_start, byte_end)?;
    let parsed = py.detach(move || {
        ranges
            .into_iter()
            .zip(&valid_rows)
            .map(|((start, end), &valid)| {
                if !valid {
                    return Ok(None);
                }
                if start > end || end < byte_start || end > byte_end {
                    return Err(io::IoError::wkb("Arrow WKB offsets are out of bounds"));
                }
                let lo = start - byte_start;
                let hi = end - byte_start;
                io::parse_wkb(&data[lo..hi]).map(Some)
            })
            .collect::<Result<Vec<_>, _>>()
    })?;
    push_parsed_wkb_geometries(parsed, crs, geometries, row, missing_rows)
}

enum OwnedBinaryView {
    Missing,
    Inline {
        bytes: [u8; 12],
        len: usize,
    },
    Buffer {
        index: usize,
        start: usize,
        end: usize,
    },
}

/// Decode one PRESENT BinaryView descriptor (m02: prefix + inline padding).
fn decode_present_binary_view(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    view: &[u8],
    data_buffers: &mut HashMap<usize, Vec<u8>>,
) -> PyResult<OwnedBinaryView> {
    let length = i32::from_le_bytes(view[0..4].try_into().expect("view length"));
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
        return Ok(OwnedBinaryView::Inline { bytes, len: length });
    }
    let buffer_index = i32::from_le_bytes(view[8..12].try_into().expect("view buffer index"));
    let buffer_index = usize::try_from(buffer_index)
        .map_err(|_| geoarrow_parse_error("Arrow binary-view buffer index is negative"))?;
    let byte_offset = i32::from_le_bytes(view[12..16].try_into().expect("view byte offset"));
    let byte_offset = usize::try_from(byte_offset)
        .map_err(|_| geoarrow_parse_error("Arrow binary-view byte offset is negative"))?;
    if let std::collections::hash_map::Entry::Vacant(slot) = data_buffers.entry(buffer_index) {
        slot.insert(required_arrow_buffer(
            py,
            storage,
            2 + buffer_index,
            "binary-view data",
        )?);
    }
    let data = data_buffers
        .get(&buffer_index)
        .expect("binary-view buffer loaded");
    let byte_end = byte_offset
        .checked_add(length)
        .ok_or_else(|| geoarrow_parse_error("Arrow binary-view byte range overflows"))?;
    let payload = data.get(byte_offset..byte_end).ok_or_else(|| {
        geoarrow_parse_error("Arrow binary-view data buffer is shorter than declared views")
    })?;
    if payload.len() < 4 || view[4..8] != payload[..4] {
        return Err(geoarrow_parse_error(
            "Arrow binary-view prefix does not match referenced data",
        ));
    }
    Ok(OwnedBinaryView::Buffer {
        index: buffer_index,
        start: byte_offset,
        end: byte_end,
    })
}

fn append_arrow_wkb_binary_view(
    py: Python<'_>,
    storage: &Bound<'_, PyAny>,
    crs: Option<&str>,
    geometries: &mut Vec<PyGeometry>,
    row: &mut usize,
    missing_rows: &mut Vec<usize>,
) -> PyResult<()> {
    let len = storage.len()?;
    // Empty BinaryView: short-circuit before views-buffer lookup (zero-sized
    // views buffers may carry a null pointer).
    if len == 0 {
        return Ok(());
    }
    // Geometry-level nulls become missing rows; the view slot is skipped.
    let validity = arrow_validity(py, storage)?;
    let valid_rows: Vec<bool> = (0..len).map(|index| validity.is_valid(index)).collect();
    let offset = arrow_array_offset(storage)?;
    let views = required_arrow_buffer(py, storage, 1, "binary-view views")?;
    let view_start = offset
        .checked_mul(16)
        .ok_or_else(|| geoarrow_parse_error("Arrow binary-view offset overflows"))?;
    let view_bytes = len
        .checked_mul(16)
        .and_then(|span| view_start.checked_add(span))
        .ok_or_else(|| geoarrow_parse_error("Arrow binary-view length overflows"))?;
    if view_bytes > views.len() {
        return Err(geoarrow_parse_error(
            "Arrow binary-view buffer is shorter than declared array length",
        ));
    }
    // Sparse materialization only for referenced non-inline buffers — never
    // dense-grow `Vec<Option<Vec<u8>>>` to a forged high buffer_index.
    let mut data_buffers: HashMap<usize, Vec<u8>> = HashMap::new();
    let mut rows = crate::try_vec_with_capacity(len)?;
    for (idx, &valid) in valid_rows.iter().enumerate() {
        if !valid {
            rows.push(OwnedBinaryView::Missing);
            continue;
        }
        let view = &views[view_start + idx * 16..view_start + idx * 16 + 16];
        rows.push(decode_present_binary_view(
            py,
            storage,
            view,
            &mut data_buffers,
        )?);
    }
    let parsed = py.detach(move || {
        rows.into_iter()
            .map(|row| {
                let bytes = match &row {
                    OwnedBinaryView::Missing => return Ok(None),
                    OwnedBinaryView::Inline { bytes, len } => &bytes[..*len],
                    OwnedBinaryView::Buffer { index, start, end } => data_buffers
                        .get(index)
                        .expect("descriptor references a loaded binary-view buffer")
                        .get(*start..*end)
                        .expect("binary-view range validated before detach"),
                };
                io::parse_wkb(bytes).map(Some)
            })
            .collect::<Result<Vec<_>, _>>()
    })?;
    push_parsed_wkb_geometries(parsed, crs, geometries, row, missing_rows)
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
    for parsed in parsed {
        if let Some(parsed) = parsed {
            guard_embedded_crs_conflict(parsed.crs.as_deref(), crs, "EWKB SRID")?;
            let row_crs = base_crs.clone().or_else(|| parsed.crs.map(crs_arc));
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
