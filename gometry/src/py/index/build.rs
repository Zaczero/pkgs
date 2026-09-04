use crate::array::PackedStorage;
use crate::geometry::TopologyDomain;
use crate::py::index::{
    AABB, Bound, Frame, GeometryArrayStorage, GeometryError, IndexEntry, IndexRows, PackedStore,
    PyAny, PyAnyMethods as _, PyGeometry, PyGeometryArray, PyResult, PySpatialIndex, Python, RTree,
    ShapeData, StaticStrTree, bounds_envelope, exact_geometry, exact_geometry_array,
    geodesic_prunable_point, index_envelopes,
};
pub(crate) fn spatial_index(items: Vec<PyGeometry>) -> PyResult<PySpatialIndex> {
    let metadata = index_metadata(&items)?;
    spatial_index_rows(
        crate::try_collect_results(items.into_iter().map(|item| Ok(Some(item))))?,
        metadata,
    )
}

/// Build over a sparse row table. ``None`` rows retain their input position as
/// a permanently non-live handle, so every reported handle remains an original
/// row id even when missing rows are skipped.
fn spatial_index_rows(
    items: Vec<Option<PyGeometry>>,
    metadata: Option<Frame>,
) -> PyResult<PySpatialIndex> {
    let domain = metadata
        .as_ref()
        .map(crate::geometry::topology_domain)
        .transpose()?
        .unwrap_or(TopologyDomain::Cartesian);
    let mut entries = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        let Some(item) = item else { continue };
        let Some(bounds) = item.shape.bounds() else {
            continue;
        };
        let cover = index_envelopes(item.shape.shape(), bounds, domain)?;
        for &envelope in cover.entries() {
            crate::try_push(&mut entries, IndexEntry { idx, envelope })?;
        }
    }
    let non_prunable_live = items
        .iter()
        .flatten()
        .filter(|item| item.shape.bounds().is_some() && !geodesic_prunable_point(&item.shape))
        .count();
    let live = crate::try_collect_results(items.iter().map(|item| {
        Ok::<_, pyo3::PyErr>(u8::from(
            item.as_ref()
                .is_some_and(|item| item.shape.bounds().is_some()),
        ))
    }))?;
    let live_count = live.iter().map(|&live| usize::from(live != 0)).sum();
    // `total_rows` spans every input row (incl. skipped empties) so the
    // tombstone bitmap and tree/overflow handle boundary stay aligned to the
    // boxed row positions the query results index into.
    let total_rows = items.len();
    let placeholder_frame = metadata.clone().unwrap_or_default();
    let rows = crate::try_collect_results(items.into_iter().map(|item| {
        Ok::<_, pyo3::PyErr>(item.map_or_else(
            || {
                PyGeometry::with_frame(
                    PyGeometryArray::missing_placeholder(),
                    placeholder_frame.clone(),
                )
            },
            |item| item.with_prepared_plan(item.plan_or_transient()),
        ))
    }))?;
    Ok(PySpatialIndex {
        rows: IndexRows {
            packed: None,
            boxed: rows,
            live,
            live_count,
        },
        bulk: StaticStrTree::build(entries, total_rows).map_err(pyo3::PyErr::from)?,
        overflow: RTree::new(),
        geodesic_caps: std::sync::Mutex::default(),
        mutation_gen: 0,
        metadata,
        non_prunable_live,
    })
}

/// The column-direct index over a packed point or line array — entries
/// straight from the coordinate columns and the frame from the array (no
/// per-row boxing, no per-row metadata scan). `None` when the array is not
/// packed, has no rows, or holds an empty line row (the general path owns
/// the "cannot index empty geometry" error).
/// Per-row envelope entries from the array's cached SIMD bounds fold.
/// `None` when any row is empty — the general path owns the canonical
/// empty-geometry error.
pub(crate) fn bounds_index_entries(
    bounds: &crate::array::ElementBounds,
) -> PyResult<Option<Vec<IndexEntry>>> {
    let mut entries = Vec::new();
    for (idx, row_bounds) in bounds.iter().enumerate() {
        let Some(bounds) = row_bounds.as_ref().copied() else {
            return Ok(None);
        };
        crate::try_push(&mut entries, IndexEntry {
            idx,
            envelope: bounds_envelope(bounds),
        })?;
    }
    Ok(Some(entries))
}

pub(crate) fn packed_spatial_index(array: &PyGeometryArray) -> PyResult<Option<PySpatialIndex>> {
    if array.has_missing() {
        return Ok(None);
    }
    let domain = crate::geometry::topology_domain(&array.frame)?;
    if domain.is_geographic() {
        let mut entries = Vec::new();
        let mut non_prunable_live = 0;
        for (idx, shape) in array.storage().iter_shapes().enumerate() {
            let Some(bounds) = shape.bounds() else {
                return Ok(None);
            };
            let cover = index_envelopes(&shape, bounds, domain)?;
            for &envelope in cover.entries() {
                crate::try_push(&mut entries, IndexEntry { idx, envelope })?;
            }
            non_prunable_live += usize::from(!geodesic_prunable_point(&ShapeData::new(
                shape.into_owned(),
            )));
        }
        let total_rows = array.storage().len();
        let mut live = Vec::new();
        crate::try_reserve_vec(&mut live, total_rows).map_err(pyo3::PyErr::from)?;
        live.resize(total_rows, 1);
        return Ok(Some(PySpatialIndex {
            rows: IndexRows {
                packed: Some(PackedStore {
                    source: array.clone(),
                }),
                boxed: Vec::new(),
                live,
                live_count: total_rows,
            },
            bulk: StaticStrTree::build(entries, total_rows).map_err(pyo3::PyErr::from)?,
            overflow: RTree::new(),
            geodesic_caps: std::sync::Mutex::default(),
            mutation_gen: 0,
            metadata: Some(array.frame.clone()),
            non_prunable_live,
        }));
    }
    match array.storage() {
        GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
            if coords.is_empty() {
                return Ok(None);
            }
            let map = row_map.as_deref();
            let len = crate::array::point_logical_len(coords, map);
            let mut entries = Vec::new();
            for logical in 0..len {
                let point = coords.point_at(crate::array::physical_row(map, logical));
                crate::try_push(&mut entries, IndexEntry {
                    idx: logical,
                    envelope: AABB::from_point([point.x, point.y]),
                })?;
            }
            let non_prunable_live = (0..len)
                .filter(|&logical| {
                    let point = coords.point_at(crate::array::physical_row(map, logical));
                    !((-180.0..=180.0).contains(&point.x) && (-90.0..=90.0).contains(&point.y))
                })
                .count();
            let total_rows = entries.len();
            let mut live = Vec::new();
            crate::try_reserve_vec(&mut live, total_rows).map_err(pyo3::PyErr::from)?;
            live.resize(total_rows, 1);
            Ok(Some(PySpatialIndex {
                rows: IndexRows {
                    packed: Some(PackedStore {
                        source: array.clone(),
                    }),
                    boxed: Vec::new(),
                    live,
                    live_count: total_rows,
                },
                bulk: StaticStrTree::build_dense(entries).map_err(pyo3::PyErr::from)?,
                overflow: RTree::new(),
                geodesic_caps: std::sync::Mutex::default(),
                mutation_gen: 0,
                metadata: Some(array.frame.clone()),
                non_prunable_live,
            }))
        },
        GeometryArrayStorage::Packed(PackedStorage::Lines { .. } |
PackedStorage::Polygons { .. }) => {
            let Some(bounds) = array.cached_element_bounds() else {
                return Ok(None);
            };
            if bounds.is_empty() {
                return Ok(None);
            }
            let Some(entries) = bounds_index_entries(&bounds)? else {
                return Ok(None);
            };
            let total_rows = entries.len();
            let mut live = Vec::new();
            crate::try_reserve_vec(&mut live, total_rows).map_err(pyo3::PyErr::from)?;
            live.resize(total_rows, 1);
            Ok(Some(PySpatialIndex {
                rows: IndexRows {
                    packed: Some(PackedStore {
                        source: array.clone(),
                    }),
                    boxed: Vec::new(),
                    live,
                    live_count: total_rows,
                },
                bulk: StaticStrTree::build_dense(entries).map_err(pyo3::PyErr::from)?,
                overflow: RTree::new(),
                geodesic_caps: std::sync::Mutex::default(),
                mutation_gen: 0,
                metadata: Some(array.frame.clone()),
                non_prunable_live: total_rows,
            }))
        },
        GeometryArrayStorage::Mixed(_) => Ok(None),
    }
}

/// Build a spatial index from any geometry source. A packed point array
/// takes the column-direct lane.
pub(crate) fn build_spatial_index(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
) -> PyResult<PySpatialIndex> {
    if let Some(geometry) = exact_geometry(values) {
        return spatial_index(vec![geometry.clone()]);
    }
    if let Some(array) = exact_geometry_array(values) {
        let array = array.clone();
        if !array.has_missing()
            && let Some(index) = py.detach({
                let array = array.clone();
                move || packed_spatial_index(&array)
            })?
        {
            return Ok(index);
        }
        let mut row_operation = array.row_operation();
        let rows = crate::try_collect_results(row_operation.rows_with_missing(|| ()).map(
            |logical_row| match logical_row {
                crate::array::ArrayRow::Missing { .. } => Ok(None),
                crate::array::ArrayRow::Present(row) => {
                    let plan = row_operation.prepared(row).into_owned_plan();
                    Ok(Some(PyGeometry::from_prepared_plan(
                        plan,
                        array.frame.clone(),
                    )))
                },
            },
        ))?;
        // Frame is adopted only when at least one non-missing geometry is
        // present. A zero-row OR all-missing source leaves the index
        // frame-FREE (adopted by the first successful insert) — locking an
        // all-missing array's frame would reject every future differently-
        // framed insert, and binary-op frame rules require a real operand
        // geometry before a frame is established.
        let metadata = rows
            .iter()
            .any(Option::is_some)
            .then(|| array.frame.clone());
        return py.detach(move || spatial_index_rows(rows, metadata));
    }

    let raw_items = crate::collect_py_iter(values, Ok)?;
    if raw_items.iter().all(|item| !item.is_none()) {
        let items = crate::coerce_collected_geometry_items(
            &raw_items,
            true,
            crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
        )?
        .into_items();
        return py.detach(move || spatial_index(items));
    }
    let present = crate::try_collect_results(
        raw_items
            .iter()
            .filter(|item| !item.is_none())
            .map(|item| Ok::<_, pyo3::PyErr>(item.clone())),
    )?;
    let present = crate::coerce_collected_geometry_items(
        &present,
        true,
        crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
    )?
    .into_items();
    let metadata = index_metadata(&present)?;
    let mut present = present.into_iter();
    let rows = crate::try_collect_results(raw_items.into_iter().map(|item| {
        let row = if item.is_none() {
            None
        } else {
            Some(
                present
                    .next()
                    .expect("present rows were coerced from the same input mask"),
            )
        };
        Ok::<_, pyo3::PyErr>(row)
    }))?;
    py.detach(move || spatial_index_rows(rows, metadata))
}

pub(crate) fn restore_spatial_index(
    values: &PyGeometryArray,
    live_handles: &[usize],
) -> PyResult<PySpatialIndex> {
    if values.has_missing() {
        return Err(GeometryError::new_err(
            "spatial index pickle payload cannot contain missing rows",
        ));
    }
    let mut rows = crate::try_vec_with_capacity_hint(values.len())?;
    for row in values.storage().iter_rows() {
        let plan = std::sync::Arc::new(crate::PreparedPlan::new(ShapeData::new(
            row.with_shape(std::clone::Clone::clone),
        )));
        crate::try_push(
            &mut rows,
            PyGeometry::from_prepared_plan(plan, values.frame.clone()),
        )?;
    }
    let total_rows = rows.len();
    let mut seen = crate::try_vec_with_capacity(total_rows)?;
    seen.resize(total_rows, 0_u8);
    let mut entries = crate::try_vec_with_capacity_hint(live_handles.len())?;
    let mut non_prunable_live = 0;
    for &handle in live_handles {
        let Some(item) = rows.get(handle) else {
            return Err(GeometryError::new_err(format!(
                "spatial index pickle payload live handle {handle} is out of range"
            )));
        };
        if std::mem::replace(&mut seen[handle], 1) != 0 {
            return Err(GeometryError::new_err(format!(
                "spatial index pickle payload repeats live handle {handle}"
            )));
        }
        let bounds = item.shape.bounds().ok_or_else(|| {
            GeometryError::new_err(format!(
                "spatial index pickle payload marks empty handle {handle} as live"
            ))
        })?;
        let domain = crate::geometry::topology_domain(&values.frame)?;
        let cover = index_envelopes(item.shape.shape(), bounds, domain)?;
        for &envelope in cover.entries() {
            crate::try_push(&mut entries, IndexEntry {
                idx: handle,
                envelope,
            })?;
        }
        non_prunable_live += usize::from(!geodesic_prunable_point(&item.shape));
    }
    // Re-derive metadata from the row storage — never trust a serialized flag.
    let metadata = index_metadata(&rows)?;
    Ok(PySpatialIndex {
        rows: IndexRows {
            packed: None,
            boxed: rows,
            live: seen,
            live_count: live_handles.len(),
        },
        bulk: StaticStrTree::build(entries, total_rows).map_err(pyo3::PyErr::from)?,
        overflow: RTree::new(),
        geodesic_caps: std::sync::Mutex::default(),
        mutation_gen: 0,
        metadata,
        non_prunable_live,
    })
}

/// The CRS + coordinate epoch shared by every item, or `None` for an empty
/// index. Errors if items disagree — an index over mixed frames is meaningless.
pub(crate) fn index_metadata(items: &[PyGeometry]) -> PyResult<Option<Frame>> {
    if items.is_empty() {
        return Ok(None);
    }
    // The canonical shared-frame gate: per-axis CRSMismatchError messages.
    Ok(Some(crate::Frame::common(items, "spatial index")?))
}

#[cfg(test)]
mod ownership_tests {
    use std::sync::Arc;

    use super::*;
    use crate::HeapSize;
    use crate::boundary::Frame;
    use crate::geometry::{CoordSeq, CsrOffsetColumn, PolygonLevel, RingLevel};

    fn points() -> PyGeometryArray {
        PyGeometryArray::packed_points(
            CoordSeq::from_vecs(vec![0.0, 10.0], vec![0.0, 10.0], None, None),
            Frame::None,
        )
    }

    fn long_lines() -> PyGeometryArray {
        PyGeometryArray::packed_lines(
            CoordSeq::from_vecs((0..34).map(f64::from).collect(), vec![0.0; 34], None, None),
            CsrOffsetColumn::try_new(vec![0, 34], 34).unwrap(),
            Frame::None,
        )
    }

    fn polygons() -> PyGeometryArray {
        PyGeometryArray::packed_polygons(
            CoordSeq::from_vecs(
                vec![0.0, 1.0, 1.0, 0.0, 0.0],
                vec![0.0, 0.0, 1.0, 1.0, 0.0],
                None,
                None,
            ),
            CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5], 5).unwrap(),
            CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1], 1).unwrap(),
            Frame::None,
        )
    }

    #[test]
    fn packed_sources_are_indexed_without_build_time_boxes() {
        let point_index = packed_spatial_index(&points()).unwrap().unwrap();
        assert!(point_index.rows.packed.is_some());
        assert!(point_index.rows.boxed.is_empty());

        let line_index = packed_spatial_index(&long_lines()).unwrap().unwrap();
        assert!(line_index.rows.packed.is_some());
        assert!(line_index.rows.boxed.is_empty());

        let polygon_index = packed_spatial_index(&polygons()).unwrap().unwrap();
        assert!(polygon_index.rows.packed.is_some());
        assert!(polygon_index.rows.boxed.is_empty());

        let source = points();
        let mut index = packed_spatial_index(&source).unwrap().unwrap();
        let inserted = index.rows.packed.as_ref().unwrap().source.clone();
        let handles = index.insert_array(&inserted).unwrap();
        assert_eq!(handles, [2, 3]);
        assert_eq!(index.rows.boxed.len(), 2);
    }

    #[test]
    fn packed_index_and_source_share_the_persistent_row_arc() {
        let source = long_lines();
        let index = packed_spatial_index(&source).unwrap().unwrap();
        let source_arc = match source.prepared_row(0, source.storage().row(0)) {
            crate::array::PreparedRow::Shared(data) => Arc::clone(data),
            crate::array::PreparedRow::Owned(data) => data,
            crate::array::PreparedRow::Transient(_) => panic!("long packed row must persist"),
        };
        let indexed_arc = index.rows.prepared_row(0).into_owned_plan();
        assert!(Arc::ptr_eq(&source_arc, &indexed_arc));

        let inverse = packed_spatial_index(&source).unwrap().unwrap();
        let indexed_first = inverse.rows.prepared_row(0).into_owned_plan();
        let source_second = source
            .prepared_row(0, source.storage().row(0))
            .into_owned_plan();
        assert!(Arc::ptr_eq(&indexed_first, &source_second));
        assert!(crate::HeapSize::heap_bytes(&source) > source.nbytes());
    }

    #[test]
    fn pickle_restore_keeps_sparse_handles_and_boxes_rows() {
        let source = points();
        let restored = restore_spatial_index(&source, &[1]).unwrap();
        assert!(restored.rows.packed.is_none());
        assert_eq!(restored.rows.boxed.len(), 2);
        assert!(!restored.rows.is_live(0));
        assert!(restored.rows.is_live(1));
        assert_eq!(restored.rows.live, [0, 1]);
        assert_eq!(
            restored.rows.heap_bytes(),
            HeapSize::heap_bytes(&restored.rows)
        );
    }
}
