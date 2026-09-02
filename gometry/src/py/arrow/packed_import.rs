use std::sync::Arc;

use crate::array::{GeometryArrayStorage, ValidityMask};
use crate::geometry::{HasM, HasZ, column_all_finite};
use crate::py::arrow::{
    ArrowCoordinateValues, ArrowListLevel, ArrowPolygonLevels, ArrowStorage, CoordSeq,
    CoordinateAxes, ExactArrowCoordinateFill, Frame, PyAnyMethods as _, PyGeometryArray, PyResult,
    Python, arrow_content_error, arrow_coordinate_values, arrow_validity, coordinate_span,
    geoarrow_parse_error,
};

pub(crate) fn packed_linestrings_import(
    py: Python<'_>,
    storages: &[ArrowStorage],
    frame: Frame,
) -> PyResult<Option<PyGeometryArray>> {
    let multi_chunk = storages.len() > 1;
    let nullable = storages.iter().try_fold(false, |nullable, storage| {
        let array = storage.storage.bind(py);
        let validity = arrow_validity(py, array)?;
        Ok::<bool, pyo3::PyErr>(nullable || validity.first_invalid(array.len()?).is_some())
    })?;
    let capacity = if multi_chunk || nullable {
        precompute_linestring_coordinate_total(py, storages, nullable)?
    } else {
        0
    };
    let mut builder = None;
    let mut single = None;
    let mut offsets: Vec<i32> = vec![0];
    let mut total = 0_usize;
    let mut row_base = 0_usize;
    let mut missing_rows = Vec::new();
    let mut compact_layout = true;
    let mut axes: Option<CoordinateAxes> = None;
    for storage in storages {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        let validity = arrow_validity(py, array)?;
        let level = ArrowListLevel::read(py, array)?;
        level.ensure(0, len)?;
        let values = array.getattr("values")?;
        let (base, span) = coordinate_span(level.endpoint(0)?, level.endpoint(len)?)?;
        let coordinates = arrow_coordinate_values(py, &values, base, span)?;
        let end = base
            .checked_add(span)
            .ok_or_else(|| geoarrow_parse_error("Arrow coordinate span overflows"))?;
        if !nullable && !coordinates.all_valid(base, end) {
            return Ok(None);
        }
        let chunk_axes =
            CoordinateAxes::new(HasZ(coordinates.z.is_some()), HasM(coordinates.m.is_some()));
        if *axes.get_or_insert(chunk_axes) != chunk_axes {
            return Ok(None);
        }
        for index in 0..len {
            let range = level.range(index)?;
            if !validity.is_valid(index) {
                if !range.is_empty() {
                    compact_layout = false;
                }
                crate::try_push(&mut missing_rows, row_base + index)?;
                push_offset(
                    &mut offsets,
                    i32::try_from(total)
                        .map_err(|_| geoarrow_parse_error("Arrow coordinate count overflows"))?,
                    &mut builder,
                )?;
                continue;
            }
            if range.len() == 1 {
                return Ok(None);
            }
            total = match total.checked_add(range.len()) {
                Some(total) => total,
                None => return Ok(None),
            };
            let Ok(offset) = i32::try_from(total) else {
                return Ok(None);
            };
            push_offset(&mut offsets, offset, &mut builder)?;
        }
        row_base = row_base
            .checked_add(len)
            .ok_or_else(|| geoarrow_parse_error("Arrow row count overflows"))?;
        if nullable {
            for index in 0..len {
                if validity.is_valid(index) {
                    let range = level.range(index)?;
                    if !coordinates.all_valid(range.start, range.end)
                        || coordinates
                            .ensure_range_finite(range.start, range.end)
                            .is_err()
                    {
                        return Ok(None);
                    }
                }
            }
            if builder.is_none() {
                builder = Some(ExactArrowCoordinateFill::with_capacity(
                    chunk_axes, capacity,
                )?);
            }
            if compact_layout {
                builder
                    .as_mut()
                    .expect("builder inserted")
                    .append_arrow_coordinates(&coordinates)?;
            } else {
                for index in 0..len {
                    if !validity.is_valid(index) {
                        continue;
                    }
                    let range = level.range(index)?;
                    builder
                        .as_mut()
                        .expect("builder inserted")
                        .append_arrow_coordinate_range(&coordinates, range.start, range.end)?;
                }
            }
        } else if multi_chunk {
            if builder.is_none() {
                builder = Some(ExactArrowCoordinateFill::with_capacity(
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
        ExactArrowCoordinateFill::into_coord_seq,
    )?;
    let Ok(offsets) =
        crate::geometry::CsrOffsetColumn::try_from_arc_i32(offsets.into(), coords.len())
    else {
        return Ok(None);
    };
    if let Some(mask) = ValidityMask::from_sparse(row_base, &missing_rows) {
        return Ok(Some(PyGeometryArray::compact_from_storage(
            GeometryArrayStorage::Lines {
                coords: Arc::new(coords),
                offsets,
                row_map: crate::array::RowSelection::Identity,
            },
            mask,
            frame,
        )));
    }
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
    let mut total_rows = 0_usize;
    let mut total_present = 0_usize;
    let mut nullable = false;
    for storage in storages {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        let validity = arrow_validity(py, array)?;
        nullable |= validity.first_invalid(len).is_some();
        total_rows = total_rows
            .checked_add(len)
            .ok_or_else(|| geoarrow_parse_error("Arrow coordinate count overflows"))?;
        total_present = total_present
            .checked_add((0..len).filter(|&index| validity.is_valid(index)).count())
            .ok_or_else(|| geoarrow_parse_error("Arrow coordinate count overflows"))?;
    }
    let capacity =
        (multi_chunk || nullable).then_some(if nullable { total_present } else { total_rows });
    let mut builder = None;
    let mut single = None;
    let mut axes: Option<CoordinateAxes> = None;
    let mut row_base = 0_usize;
    let mut missing_rows = Vec::new();
    for storage in storages {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        let validity = arrow_validity(py, array)?;
        let coordinates = arrow_coordinate_values(py, array, 0, len)?;
        let chunk_axes =
            CoordinateAxes::new(HasZ(coordinates.z.is_some()), HasM(coordinates.m.is_some()));
        if *axes.get_or_insert(chunk_axes) != chunk_axes {
            return Ok(None);
        }
        if nullable {
            for index in 0..len {
                if validity.is_valid(index)
                    && (!coordinates.is_valid(index)
                        || coordinates.ensure_range_finite(index, index + 1).is_err())
                {
                    return Ok(None);
                }
            }
            if builder.is_none() {
                builder = Some(ExactArrowCoordinateFill::with_capacity(
                    chunk_axes,
                    capacity.expect("nullable point capacity"),
                )?);
            }
            builder
                .as_mut()
                .expect("builder inserted")
                .append_arrow_present_coordinates(&coordinates, &validity)?;
            for index in 0..len {
                if !validity.is_valid(index) {
                    crate::try_push(&mut missing_rows, row_base + index)?;
                }
            }
        } else if multi_chunk {
            if !coordinates.all_valid(0, len) || !coordinates_all_finite(&coordinates) {
                return Ok(None);
            }
            if builder.is_none() {
                builder = Some(ExactArrowCoordinateFill::with_capacity(
                    chunk_axes,
                    capacity.expect("multi-chunk point capacity"),
                )?);
            }
            builder
                .as_mut()
                .expect("builder inserted")
                .append_arrow_coordinates(&coordinates)?;
        } else {
            if !coordinates.all_valid(0, len) {
                return Ok(None);
            }
            // Non-finite includes the GeoArrow all-NaN POINT EMPTY sentinel —
            // that must use the boxed path, never packed NaN rows.
            if !coordinates_all_finite(&coordinates) {
                return Ok(None);
            }
            single = Some(coordseq_from_arrow_coordinates(&coordinates)?);
        }
        row_base = row_base
            .checked_add(len)
            .ok_or_else(|| geoarrow_parse_error("Arrow row count overflows"))?;
    }
    let coords = builder.map_or_else(
        || Ok(single.unwrap_or_else(|| CoordSeq::empty(CoordinateAxes::XY))),
        ExactArrowCoordinateFill::into_coord_seq,
    )?;
    if let Some(mask) = ValidityMask::from_sparse(total_rows, &missing_rows) {
        return Ok(Some(PyGeometryArray::compact_from_storage(
            GeometryArrayStorage::Points {
                coords: Arc::new(coords),
                row_map: crate::array::RowSelection::Identity,
            },
            mask,
            frame,
        )));
    }
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
    let nullable = storages.iter().try_fold(false, |nullable, storage| {
        let array = storage.storage.bind(py);
        let validity = arrow_validity(py, array)?;
        Ok::<bool, pyo3::PyErr>(nullable || validity.first_invalid(array.len()?).is_some())
    })?;
    let capacity = if multi_chunk || nullable {
        precompute_polygon_coordinate_total(py, storages, nullable)?
    } else {
        0
    };
    let mut builder = None;
    let mut single = None;
    let mut ring_offsets: Vec<i32> = vec![0];
    let mut polygon_offsets: Vec<i32> = vec![0];
    let mut total_vertices = 0_usize;
    let mut total_rings = 0_usize;
    let mut row_base = 0_usize;
    let mut missing_rows = Vec::new();
    let mut compact_layout = true;
    let mut axes: Option<CoordinateAxes> = None;
    for storage in storages {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        let validity = arrow_validity(py, array)?;
        let levels = ArrowPolygonLevels::read(py, array)?;
        levels.polygons.ensure(0, len)?;
        // This full selected child window is the packed path's layout
        // admission.  It must happen before coordinate buffers are read so a
        // successful packed import can be the sole offset snapshot on the
        // direct PyArrow path.
        let ring_start = levels.polygons.endpoint(0)?;
        let ring_end = levels.polygons.endpoint(len)?;
        levels.rings.ensure(
            ring_start,
            ring_end
                .checked_sub(ring_start)
                .ok_or_else(|| geoarrow_parse_error("Arrow offsets must be ordered"))?,
        )?;
        let (base, span) = levels.visible_coordinate_span(len)?;
        let values = array.getattr("values")?.getattr("values")?;
        let coordinates = arrow_coordinate_values(py, &values, base, span)?;
        let end = base
            .checked_add(span)
            .ok_or_else(|| geoarrow_parse_error("Arrow coordinate span overflows"))?;
        if !nullable && !coordinates.all_valid(base, end) {
            return Ok(None);
        }
        let chunk_axes =
            CoordinateAxes::new(HasZ(coordinates.z.is_some()), HasM(coordinates.m.is_some()));
        if *axes.get_or_insert(chunk_axes) != chunk_axes {
            return Ok(None);
        }
        for polygon_index in 0..len {
            let ring_range = levels.polygons.range(polygon_index)?;
            if !validity.is_valid(polygon_index) {
                if !ring_range.is_empty() {
                    compact_layout = false;
                }
                crate::try_push(&mut missing_rows, row_base + polygon_index)?;
                push_offset(
                    &mut polygon_offsets,
                    i32::try_from(total_rings)
                        .map_err(|_| geoarrow_parse_error("Arrow ring count overflows"))?,
                    &mut builder,
                )?;
                continue;
            }
            if ring_range.is_empty() {
                return Ok(None);
            }
            levels.rings.ensure(ring_range.start, ring_range.len())?;
            for ring_index in ring_range.clone() {
                let vertex_range = levels.rings.range(ring_index)?;
                let bounds_end = base
                    .checked_add(span)
                    .ok_or_else(|| geoarrow_parse_error("Arrow coordinate span overflows"))?;
                if nullable
                    && (!coordinates.all_valid(vertex_range.start, vertex_range.end)
                        || coordinates
                            .ensure_range_finite(vertex_range.start, vertex_range.end)
                            .is_err())
                {
                    return Ok(None);
                }
                let checked = match crate::array::CheckedRingWindow::checked_in(
                    &coordinates,
                    vertex_range.clone(),
                    base..bounds_end,
                ) {
                    Ok(checked) => checked,
                    Err(
                        crate::array::RingWindowError::TooShort(_)
                        | crate::array::RingWindowError::Open,
                    ) => {
                        return Ok(None);
                    },
                    Err(crate::array::RingWindowError::Inverted) => {
                        return Err(geoarrow_parse_error("Arrow ring vertex range is inverted"));
                    },
                    Err(crate::array::RingWindowError::OutOfRange) => {
                        return Err(geoarrow_parse_error(
                            "Arrow ring vertex range exceeds the loaded coordinate span",
                        ));
                    },
                };
                total_vertices = match total_vertices.checked_add(checked.range().len()) {
                    Some(total) => total,
                    None => return Ok(None),
                };
                let Ok(offset) = i32::try_from(total_vertices) else {
                    return Ok(None);
                };
                push_offset(&mut ring_offsets, offset, &mut builder)?;
            }
            total_rings = match total_rings.checked_add(ring_range.len()) {
                Some(total) => total,
                None => return Ok(None),
            };
            let Ok(polygon_offset) = i32::try_from(total_rings) else {
                return Ok(None);
            };
            push_offset(&mut polygon_offsets, polygon_offset, &mut builder)?;
        }
        row_base = row_base
            .checked_add(len)
            .ok_or_else(|| geoarrow_parse_error("Arrow row count overflows"))?;
        if nullable {
            if builder.is_none() {
                builder = Some(ExactArrowCoordinateFill::with_capacity(
                    chunk_axes, capacity,
                )?);
            }
            if compact_layout {
                builder
                    .as_mut()
                    .expect("builder inserted")
                    .append_arrow_coordinates(&coordinates)?;
            } else {
                for polygon_index in 0..len {
                    if !validity.is_valid(polygon_index) {
                        continue;
                    }
                    let ring_range = levels.polygons.range(polygon_index)?;
                    for ring_index in ring_range {
                        let vertex_range = levels.rings.range(ring_index)?;
                        builder
                            .as_mut()
                            .expect("builder inserted")
                            .append_arrow_coordinate_range(
                                &coordinates,
                                vertex_range.start,
                                vertex_range.end,
                            )?;
                    }
                }
            }
        } else if multi_chunk {
            if builder.is_none() {
                builder = Some(ExactArrowCoordinateFill::with_capacity(
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
        ExactArrowCoordinateFill::into_coord_seq,
    )?;
    if let Some(mask) = ValidityMask::from_sparse(row_base, &missing_rows) {
        let ring_offsets =
            crate::geometry::CsrOffsetColumn::<crate::geometry::RingLevel>::try_from_arc_i32(
                ring_offsets.into(),
                coords.len(),
            )
            .map_err(|_| geoarrow_parse_error("Arrow ring offsets are not a valid CSR"))?;
        let polygon_offsets =
            crate::geometry::CsrOffsetColumn::<crate::geometry::PolygonLevel>::try_from_arc_i32(
                polygon_offsets.into(),
                ring_offsets.len().saturating_sub(1),
            )
            .map_err(|_| geoarrow_parse_error("Arrow polygon offsets are not a valid CSR"))?;
        return Ok(Some(PyGeometryArray::compact_from_storage(
            GeometryArrayStorage::Polygons {
                coords: Arc::new(coords),
                ring_offsets,
                polygon_offsets,
                row_map: crate::array::RowSelection::Identity,
            },
            mask,
            frame,
        )));
    }
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

fn push_offset(
    offsets: &mut Vec<i32>,
    offset: i32,
    builder: &mut Option<ExactArrowCoordinateFill>,
) -> PyResult<()> {
    if let Err(error) = crate::try_push_vec(offsets, offset) {
        builder.take();
        return Err(pyo3::PyErr::from(error));
    }
    Ok(())
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
    present_only: bool,
) -> PyResult<usize> {
    storages.iter().try_fold(0_usize, |total, storage| {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        let level = ArrowListLevel::read(py, array)?;
        level.ensure(0, len)?;
        if !present_only {
            let (_, span) = coordinate_span(level.endpoint(0)?, level.endpoint(len)?)?;
            return total
                .checked_add(span)
                .ok_or_else(|| geoarrow_parse_error("Arrow coordinate count overflows"));
        }
        let validity = arrow_validity(py, array)?;
        (0..len).try_fold(total, |total, index| {
            if !validity.is_valid(index) {
                return Ok(total);
            }
            let range = level.range(index)?;
            total
                .checked_add(range.len())
                .ok_or_else(|| geoarrow_parse_error("Arrow coordinate count overflows"))
        })
    })
}

fn precompute_polygon_coordinate_total(
    py: Python<'_>,
    storages: &[ArrowStorage],
    present_only: bool,
) -> PyResult<usize> {
    storages.iter().try_fold(0_usize, |total, storage| {
        let array = storage.storage.bind(py);
        let len = array.len()?;
        let levels = ArrowPolygonLevels::read(py, array)?;
        levels.polygons.ensure(0, len)?;
        if !present_only {
            let (_, span) = levels.visible_coordinate_span(len)?;
            return total
                .checked_add(span)
                .ok_or_else(|| geoarrow_parse_error("Arrow coordinate count overflows"));
        }
        let validity = arrow_validity(py, array)?;
        (0..len).try_fold(total, |total, index| {
            if !validity.is_valid(index) {
                return Ok(total);
            }
            let rings = levels.polygons.range(index)?;
            rings.into_iter().try_fold(total, |total, ring| {
                let vertices = levels.rings.range(ring)?;
                total
                    .checked_add(vertices.len())
                    .ok_or_else(|| geoarrow_parse_error("Arrow coordinate count overflows"))
            })
        })
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
