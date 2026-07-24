use std::sync::Arc;

use crate::geometry::{HasM, HasZ, column_all_finite};
use crate::py::arrow::*;

pub(crate) fn packed_linestrings_import(
    py: Python<'_>,
    storages: &[ArrowStorage],
    frame: Frame,
) -> PyResult<Option<PyGeometryArray>> {
    let multi_chunk = storages.len() > 1;
    let capacity = if multi_chunk {
        precompute_linestring_coordinate_total(py, storages)?
    } else {
        0
    };
    let mut builder = None;
    let mut single = None;
    let mut offsets: Vec<i32> = vec![0];
    let mut total = 0_usize;
    let mut axes: Option<CoordinateAxes> = None;
    for storage in storages {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        // Dense packed lane: geometry-level nulls fall through to the append
        // path (missing-row scatter). Do not reject — outer nulls are legal.
        let validity = arrow_validity(py, array)?;
        if validity.first_invalid(len).is_some() {
            return Ok(None);
        }
        let level = ArrowListLevel::read(py, array)?;
        level.ensure(0, len)?;
        let values = array.getattr("values")?;
        let (base, span) = coordinate_span(level.endpoint(0)?, level.endpoint(len)?)?;
        let coordinates = arrow_coordinate_values(py, &values, base, span)?;
        if !coordinates.all_valid(base, base + span) {
            return Ok(None);
        }
        let chunk_axes =
            CoordinateAxes::new(HasZ(coordinates.z.is_some()), HasM(coordinates.m.is_some()));
        if *axes.get_or_insert(chunk_axes) != chunk_axes {
            return Ok(None);
        }
        for index in 0..len {
            let range = level.range(index)?;
            if range.len() == 1 {
                return Ok(None);
            }
            total += range.len();
            let Ok(offset) = i32::try_from(total) else {
                return Ok(None);
            };
            offsets.push(offset);
        }
        if multi_chunk {
            if builder.is_none() {
                builder = Some(ArrowCoordinateBuffers::try_with_capacity(
                    chunk_axes, capacity,
                )?);
            }
            builder
                .as_mut()
                .expect("builder inserted")
                .append_arrow_coordinates(&coordinates)?;
        } else {
            single = Some(coordseq_from_arrow_coordinates(&coordinates)?);
        }
    }
    let coords = builder.map_or_else(
        || Ok(single.unwrap_or_else(|| CoordSeq::empty(CoordinateAxes::XY))),
        ArrowCoordinateBuffers::into_coord_seq,
    )?;
    let Ok(offsets) =
        crate::geometry::CsrOffsetColumn::try_from_arc_i32(offsets.into(), coords.len())
    else {
        return Ok(None);
    };
    Ok(Some(PyGeometryArray::packed_lines(coords, offsets, frame)))
}

/// Column-direct point lane: a (chunked) all-point `GeoArrow` column copies
/// its coordinate buffers once into ONE shared `CoordSeq` and lands as packed
/// `Points` storage — no per-row `Shape` synthesis, no `PyGeometry`
/// staging, no re-pack scan. `None` falls back to the boxed path
/// (coordinate nulls, mixed chunk axes).
pub(crate) fn packed_points_import(
    py: Python<'_>,
    storages: &[ArrowStorage],
    frame: Frame,
) -> PyResult<Option<PyGeometryArray>> {
    let multi_chunk = storages.len() > 1;
    let capacity = if multi_chunk {
        storages.iter().try_fold(0_usize, |total, storage| {
            total
                .checked_add(storage.storage.bind(py).len()?)
                .ok_or_else(|| geoarrow_parse_error("Arrow coordinate count overflows"))
        })?
    } else {
        0
    };
    let mut builder = None;
    let mut single = None;
    let mut axes: Option<CoordinateAxes> = None;
    for storage in storages {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        let validity = arrow_validity(py, array)?;
        if validity.first_invalid(len).is_some() {
            return Ok(None);
        }
        let coordinates = arrow_coordinate_values(py, array, 0, len)?;
        if !coordinates.all_valid(0, len) {
            return Ok(None);
        }
        // Non-finite includes the GeoArrow all-NaN POINT EMPTY sentinel — that
        // must land as Shape::Empty via the boxed path, never packed NaN rows
        // (packed NaN is the missing-row placeholder, a different contract).
        if !coordinates_all_finite(&coordinates) {
            return Ok(None);
        }
        let chunk_axes =
            CoordinateAxes::new(HasZ(coordinates.z.is_some()), HasM(coordinates.m.is_some()));
        if *axes.get_or_insert(chunk_axes) != chunk_axes {
            return Ok(None);
        }
        if multi_chunk {
            if builder.is_none() {
                builder = Some(ArrowCoordinateBuffers::try_with_capacity(
                    chunk_axes, capacity,
                )?);
            }
            builder
                .as_mut()
                .expect("builder inserted")
                .append_arrow_coordinates(&coordinates)?;
        } else {
            single = Some(coordseq_from_arrow_coordinates(&coordinates)?);
        }
    }
    let coords = builder.map_or_else(
        || Ok(single.unwrap_or_else(|| CoordSeq::empty(CoordinateAxes::XY))),
        ArrowCoordinateBuffers::into_coord_seq,
    )?;
    Ok(Some(PyGeometryArray::packed_points(coords, frame)))
}

/// Column-direct import of `geoarrow.polygon` chunks into packed `Polygons`
/// storage: each chunk's coordinate span is validated and copied ONCE, the
/// Arrow two-level list offsets rebase directly into the CSR, and the array
/// materializes without any per-row machinery. Returns `Ok(None)` for inputs
/// the lane cannot express with byte-identical semantics — vertex nulls,
/// empty polygon rows, rings shorter than three vertices, rings that need
/// auto-close, cross-chunk axes mismatches, `i32` offset overflow, CSR
/// validation failure.
pub(crate) fn packed_polygons_import(
    py: Python<'_>,
    storages: &[ArrowStorage],
    frame: Frame,
) -> PyResult<Option<PyGeometryArray>> {
    let multi_chunk = storages.len() > 1;
    let capacity = if multi_chunk {
        precompute_polygon_coordinate_total(py, storages)?
    } else {
        0
    };
    let mut builder = None;
    let mut single = None;
    let mut ring_offsets: Vec<i32> = vec![0];
    let mut polygon_offsets: Vec<i32> = vec![0];
    let mut total_vertices = 0_usize;
    let mut total_rings = 0_usize;
    let mut axes: Option<CoordinateAxes> = None;
    for storage in storages {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        let validity = arrow_validity(py, array)?;
        if validity.first_invalid(len).is_some() {
            return Ok(None);
        }
        let levels = ArrowPolygonLevels::read(py, array)?;
        levels.polygons.ensure(0, len)?;
        let (base, span) = levels.visible_coordinate_span(len)?;
        let values = array.getattr("values")?.getattr("values")?;
        let coordinates = arrow_coordinate_values(py, &values, base, span)?;
        if !coordinates.all_valid(base, base + span) {
            return Ok(None);
        }
        let chunk_axes =
            CoordinateAxes::new(HasZ(coordinates.z.is_some()), HasM(coordinates.m.is_some()));
        if *axes.get_or_insert(chunk_axes) != chunk_axes {
            return Ok(None);
        }
        for polygon_index in 0..len {
            let ring_range = levels.polygons.range(polygon_index)?;
            if ring_range.is_empty() {
                return Ok(None);
            }
            levels.rings.ensure(ring_range.start, ring_range.len())?;
            for ring_index in ring_range.clone() {
                let vertex_range = levels.rings.range(ring_index)?;
                // Nested-offset containment before any ordinate indexing:
                // malformed ring windows (e.g. [0,100] against a 4-vertex
                // loaded span) must raise cleanly, never PanicException.
                ensure_vertex_range_in_span(&vertex_range, base, span)?;
                // Shared pack floor: ≥ Ring::MIN_VERTICES_CLOSED (=4), not 3.
                // A closed 3-coordinate ring must not enter packed storage.
                if !crate::array::packable_closed_ring_len(vertex_range.len()) {
                    return Ok(None);
                }
                if arrow_ring_needs_closure(&coordinates, &vertex_range)? {
                    return Ok(None);
                }
                total_vertices += vertex_range.len();
                let Ok(offset) = i32::try_from(total_vertices) else {
                    return Ok(None);
                };
                ring_offsets.push(offset);
            }
            total_rings += ring_range.len();
            let Ok(polygon_offset) = i32::try_from(total_rings) else {
                return Ok(None);
            };
            polygon_offsets.push(polygon_offset);
        }
        if multi_chunk {
            if builder.is_none() {
                builder = Some(ArrowCoordinateBuffers::try_with_capacity(
                    chunk_axes, capacity,
                )?);
            }
            builder
                .as_mut()
                .expect("builder inserted")
                .append_arrow_coordinates(&coordinates)?;
        } else {
            single = Some(coordseq_from_arrow_coordinates(&coordinates)?);
        }
    }
    let coords = builder.map_or_else(
        || Ok(single.unwrap_or_else(|| CoordSeq::empty(CoordinateAxes::XY))),
        ArrowCoordinateBuffers::into_coord_seq,
    )?;
    Ok(finish_packed_polygons(
        coords,
        ring_offsets,
        polygon_offsets,
        frame,
    ))
}

fn finish_packed_polygons(
    coords: CoordSeq,
    ring_offsets: Vec<i32>,
    polygon_offsets: Vec<i32>,
    frame: Frame,
) -> Option<PyGeometryArray> {
    let Ok(ring_offsets) =
        crate::geometry::CsrOffsetColumn::<crate::geometry::RingLevel>::try_from_arc_i32(
            ring_offsets.into(),
            coords.len(),
        )
    else {
        return None;
    };
    let ring_count = ring_offsets.len() - 1;
    let Ok(polygon_offsets) =
        crate::geometry::CsrOffsetColumn::<crate::geometry::PolygonLevel>::try_from_arc_i32(
            polygon_offsets.into(),
            ring_count,
        )
    else {
        return None;
    };
    Some(PyGeometryArray::packed_polygons(
        coords,
        ring_offsets,
        polygon_offsets,
        frame,
    ))
}

fn coordseq_from_arrow_coordinates(coordinates: &ArrowCoordinateValues) -> PyResult<CoordSeq> {
    arrow_coordseq_from_columns(
        Arc::clone(&coordinates.x.values),
        Arc::clone(&coordinates.y.values),
        coordinates
            .z
            .as_ref()
            .map(|ordinate| Arc::clone(&ordinate.values)),
        coordinates
            .m
            .as_ref()
            .map(|ordinate| Arc::clone(&ordinate.values)),
    )
}

fn coordinates_all_finite(coordinates: &ArrowCoordinateValues) -> bool {
    column_all_finite(coordinates.x.values.as_ref())
        && column_all_finite(coordinates.y.values.as_ref())
        && coordinates
            .z
            .as_ref()
            .is_none_or(|ordinate| column_all_finite(ordinate.values.as_ref()))
        && coordinates
            .m
            .as_ref()
            .is_none_or(|ordinate| column_all_finite(ordinate.values.as_ref()))
}

fn precompute_linestring_coordinate_total(
    py: Python<'_>,
    storages: &[ArrowStorage],
) -> PyResult<usize> {
    storages.iter().try_fold(0_usize, |total, storage| {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        let level = ArrowListLevel::read(py, array)?;
        level.ensure(0, len)?;
        let (_, span) = coordinate_span(level.endpoint(0)?, level.endpoint(len)?)?;
        total
            .checked_add(span)
            .ok_or_else(|| geoarrow_parse_error("Arrow coordinate count overflows"))
    })
}

fn precompute_polygon_coordinate_total(
    py: Python<'_>,
    storages: &[ArrowStorage],
) -> PyResult<usize> {
    storages.iter().try_fold(0_usize, |total, storage| {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        let levels = ArrowPolygonLevels::read(py, array)?;
        levels.polygons.ensure(0, len)?;
        let (_, span) = levels.visible_coordinate_span(len)?;
        total
            .checked_add(span)
            .ok_or_else(|| geoarrow_parse_error("Arrow coordinate count overflows"))
    })
}

pub(crate) fn arrow_coordseq_from_columns(
    xs: Arc<[f64]>,
    ys: Arc<[f64]>,
    zs: Option<Arc<[f64]>>,
    ms: Option<Arc<[f64]>>,
) -> PyResult<CoordSeq> {
    debug_assert_eq!(xs.len(), ys.len());
    debug_assert!(zs.as_ref().is_none_or(|column| column.len() == xs.len()));
    debug_assert!(ms.as_ref().is_none_or(|column| column.len() == xs.len()));
    if !column_all_finite(&xs)
        || !column_all_finite(&ys)
        || zs
            .as_deref()
            .is_some_and(|column| !column_all_finite(column))
        || ms
            .as_deref()
            .is_some_and(|column| !column_all_finite(column))
    {
        return Err(arrow_content_error(
            crate::geometry::GeometryErrorKind::NonFiniteCoordinate.into(),
        ));
    }
    Ok(CoordSeq::try_from_columns(xs, ys, zs, ms)?)
}
