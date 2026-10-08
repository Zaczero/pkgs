use crate::boundary::metadata;
use crate::geometry::{TopologyDomain, storage};
use crate::py::index::{
    Bound, Frame, GeometryArrayStorage, GeometryError, IndexEntry, IndexRows, PackedStore, PyAny,
    PyAnyMethods as _, PyGeometry, PyGeometryArray, PyResult, PySpatialIndex, Python, ShapeData,
    StaticStrForest, StaticStrTree, bounds_envelope, exact_geometry, exact_geometry_array,
    geodesic_domain_invalid, index_envelopes,
};
pub(crate) fn spatial_index(mut items: Vec<PyGeometry>) -> PyResult<PySpatialIndex> {
    let metadata = index_metadata(&items)?;
    let scan = scan_spatial_index_rows(items.iter().map(Some), metadata.as_ref())?;
    for item in &mut items {
        *item = item.with_prepared_plan(item.prepared_plan());
    }
    finish_spatial_index(items, scan, metadata)
}

struct IndexScan {
    entries: Vec<IndexEntry>,
    live: Vec<u8>,
    non_prunable_live: usize,
}

/// Build over a sparse row table. ``None`` rows retain their input position as
/// a permanently non-live handle, so every reported handle remains an original
/// row id even when missing rows are skipped.
fn spatial_index_rows(
    items: Vec<Option<PyGeometry>>,
    metadata: Option<Frame>,
) -> PyResult<PySpatialIndex> {
    let scan = scan_spatial_index_rows(items.iter().map(Option::as_ref), metadata.as_ref())?;
    let placeholder_frame = metadata.clone().unwrap_or_default();
    let rows = items
        .into_iter()
        .map(|item| {
            item.map_or_else(
                || {
                    PyGeometry::with_frame(
                        PyGeometryArray::missing_placeholder(),
                        placeholder_frame.clone(),
                    )
                },
                |item| item.with_prepared_plan(item.prepared_plan()),
            )
        })
        .collect();
    finish_spatial_index(rows, scan, metadata)
}

fn scan_spatial_index_rows<'a>(
    items: impl ExactSizeIterator<Item = Option<&'a PyGeometry>>,
    metadata: Option<&Frame>,
) -> PyResult<IndexScan> {
    let domain = metadata
        .map(crate::geometry::topology_domain)
        .transpose()?
        .unwrap_or(TopologyDomain::Cartesian);
    let mut entries = Vec::new();
    let mut live = Vec::with_capacity(items.len());
    let mut non_prunable_live = 0_usize;
    for (idx, item) in items.enumerate() {
        let Some(item) = item else {
            live.push(0);
            continue;
        };
        let Some(bounds) = item.shape.bounds() else {
            live.push(0);
            continue;
        };
        let cover = index_envelopes(item.shape.shape(), bounds, domain)?;
        for &envelope in cover.entries() {
            entries.push(IndexEntry { idx, envelope });
        }
        live.push(1);
        if matches!(domain, TopologyDomain::Geographic(_)) {
            non_prunable_live = non_prunable_live.wrapping_add(usize::from(
                geodesic_domain_invalid(item.shape.shape(), domain),
            ));
        }
    }
    Ok(IndexScan {
        entries,
        live,
        non_prunable_live,
    })
}

fn finish_spatial_index(
    rows: Vec<PyGeometry>,
    scan: IndexScan,
    metadata: Option<Frame>,
) -> PyResult<PySpatialIndex> {
    // `total_rows` spans every input row (incl. skipped empties) so the
    // tombstone bitmap and tree/overflow handle boundary stay aligned to the
    // boxed row positions the query results index into.
    let total_rows = rows.len();
    let live_count = scan.live.iter().filter(|&&live| live != 0).count();
    let bulk_entry_counts = bulk_entry_counts(&scan.entries, live_count, total_rows);
    let live_handles = super::live_handles::LiveHandles::from_live(&scan.live, live_count);
    Ok(PySpatialIndex {
        rows: IndexRows {
            packed: None,
            boxed: rows,
            live: scan.live,
            live_count,
            live_handles,
        },
        bulk: StaticStrTree::build(scan.entries, total_rows).map_err(pyo3::PyErr::from)?,
        bulk_dead_entries: 0,
        bulk_entry_counts,
        overflow: StaticStrForest::new(total_rows),
        mutation_gen: 0,
        metadata,
        non_prunable_live: scan.non_prunable_live,
    })
}

/// Build packed entries directly from the retained array owner. Empty and
/// missing rows stay in the liveness bitmap without a tree entry.
fn packed_index_payload(
    array: &PyGeometryArray,
    domain: TopologyDomain,
) -> PyResult<(Vec<IndexEntry>, Vec<u8>, usize)> {
    let mut entries = Vec::with_capacity(array.len());
    let mut live = Vec::with_capacity(array.len());
    let mut non_prunable_live = 0_usize;
    let element_bounds = (!matches!(
        array.storage(),
        GeometryArrayStorage::Packed(storage::PackedStorage::Points { .. })
    ))
    .then(|| array.element_bounds())
    .flatten();
    let rows = array.row_operation();
    for logical_row in rows.rows_with_missing(|| ()) {
        let crate::array::ArrayRow::Present(row) = logical_row else {
            live.push(0);
            continue;
        };
        let logical = row.logical();
        let shape_row = row.row();
        let bounds = element_bounds
            .as_ref()
            .and_then(|bounds| bounds.get(logical).copied().flatten())
            .or_else(|| shape_row.quick_bounds());
        let Some(bounds) = bounds else {
            live.push(0);
            continue;
        };
        let mut row_non_prunable = false;
        match domain {
            TopologyDomain::Cartesian => {
                entries.push(IndexEntry {
                    idx: logical,
                    envelope: bounds_envelope(bounds),
                });
            },
            TopologyDomain::Geographic(_) => shape_row.try_with_shape(|shape| {
                let cover = index_envelopes(shape, bounds, domain)?;
                entries.extend(cover.entries().iter().map(|&envelope| IndexEntry {
                    idx: logical,
                    envelope,
                }));
                row_non_prunable = geodesic_domain_invalid(shape, domain);
                Ok::<_, pyo3::PyErr>(())
            })?,
        }
        live.push(1);
        if matches!(domain, TopologyDomain::Geographic(_)) {
            non_prunable_live = non_prunable_live.wrapping_add(usize::from(row_non_prunable));
        }
    }
    Ok((entries, live, non_prunable_live))
}

pub(crate) fn packed_spatial_index(array: &PyGeometryArray) -> PyResult<Option<PySpatialIndex>> {
    if array.len() == 0 || !matches!(array.storage(), GeometryArrayStorage::Packed(_)) {
        return Ok(None);
    }
    let metadata = array
        .missing()
        .is_none_or(|missing| missing.present_count() != 0)
        .then(|| array.frame.clone());
    let domain = metadata
        .as_ref()
        .map(crate::geometry::topology_domain)
        .transpose()?
        .unwrap_or(TopologyDomain::Cartesian);
    let (entries, live, non_prunable_live) = packed_index_payload(array, domain)?;
    let live_count = match domain {
        TopologyDomain::Cartesian => entries.len(),
        TopologyDomain::Geographic(_) => live.iter().map(|&live| usize::from(live != 0)).sum(),
    };
    let total_rows = array.len();
    let bulk_entry_counts = bulk_entry_counts(&entries, live_count, total_rows);
    let live_handles = super::live_handles::LiveHandles::from_live(&live, live_count);
    Ok(Some(PySpatialIndex {
        rows: IndexRows {
            packed: Some(PackedStore {
                source: array.clone(),
            }),
            boxed: Vec::new(),
            live,
            live_count,
            live_handles,
        },
        bulk: StaticStrTree::build(entries, total_rows).map_err(pyo3::PyErr::from)?,
        bulk_dead_entries: 0,
        bulk_entry_counts,
        overflow: StaticStrForest::new(total_rows),
        mutation_gen: 0,
        metadata,
        non_prunable_live,
    }))
}

/// Build a spatial index from any geometry source. Packed arrays retain their
/// immutable columns and use logical row handles for the bulk tree.
pub(crate) fn build_spatial_index(
    py: Python<'_>,
    values: &Bound<'_, PyAny>,
) -> PyResult<PySpatialIndex> {
    if let Some(geometry) = exact_geometry(values) {
        return spatial_index(vec![geometry.clone()]);
    }
    if let Some(array) = exact_geometry_array(values) {
        let array = array.clone();
        if let Some(index) = py.detach({
            let array = array.clone();
            move || packed_spatial_index(&array)
        })? {
            return Ok(index);
        }
        let mut row_operation = array.row_operation();
        let rows = row_operation
            .rows_with_missing(|| ())
            .map(|logical_row| match logical_row {
                crate::array::ArrayRow::Missing { .. } => Ok(None),
                crate::array::ArrayRow::Present(row) => {
                    let plan = row_operation.prepared(row)?.into_owned_plan();
                    Ok(Some(PyGeometry::from_prepared_plan(
                        plan,
                        array.frame.clone(),
                    )))
                },
            })
            .collect::<pyo3::PyResult<Vec<_>>>()?;
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
        let items = crate::coerce_present_geometry_items(
            raw_items.iter(),
            true,
            crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
        )?;
        return py.detach(move || spatial_index(items));
    }
    let present = crate::coerce_present_geometry_items(
        raw_items.iter().filter(|item| !item.is_none()),
        true,
        crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
    )?;
    let metadata = index_metadata(&present)?;
    let mut present = present.into_iter();
    let rows = raw_items
        .into_iter()
        .map(|item| {
            if item.is_none() {
                None
            } else {
                Some(
                    present
                        .next()
                        .expect("present rows were coerced from the same input mask"),
                )
            }
        })
        .collect();
    py.detach(move || spatial_index_rows(rows, metadata))
}

pub(crate) fn restore_spatial_index(
    values: &PyGeometryArray,
    has_metadata: bool,
) -> PyResult<PySpatialIndex> {
    if let Some(mut index) = packed_spatial_index(values)? {
        let has_present_rows = values
            .missing()
            .map_or(values.len() != 0, |missing| missing.present_count() != 0);
        index.metadata = (has_metadata || has_present_rows).then(|| values.frame.clone());
        return Ok(index);
    }
    let mut rows = Vec::with_capacity(values.len());
    let mut live = Vec::with_capacity(values.len());
    let mut present = false;
    let mut row_operation = values.row_operation();
    let domain = crate::geometry::topology_domain(&values.frame)?;
    let mut entries = Vec::with_capacity(values.len());
    let mut non_prunable_live = 0_usize;
    for logical in 0..values.len() {
        let geometry = if let Some(row) = row_operation.present_row_at(logical) {
            present = true;
            let plan = std::sync::Arc::new(crate::PreparedPlan::new(ShapeData::new(
                row.row().with_shape(std::clone::Clone::clone)?,
            )));
            let geometry = PyGeometry::from_prepared_plan(plan, values.frame.clone());
            let bounds = geometry.shape.bounds().ok_or_else(|| {
                GeometryError::new_err(format!(
                    "spatial index pickle payload marks empty handle {logical} as live"
                ))
            })?;
            let cover = index_envelopes(geometry.shape.shape(), bounds, domain)?;
            for &envelope in cover.entries() {
                entries.push(IndexEntry {
                    idx: logical,
                    envelope,
                });
            }
            if matches!(domain, TopologyDomain::Geographic(_)) {
                non_prunable_live = non_prunable_live.wrapping_add(usize::from(
                    geodesic_domain_invalid(geometry.shape.shape(), domain),
                ));
            }
            live.push(1);
            geometry
        } else {
            live.push(0);
            PyGeometry::with_frame(PyGeometryArray::missing_placeholder(), values.frame.clone())
        };
        rows.push(geometry);
    }
    let total_rows = rows.len();
    let live_count = live.iter().filter(|&&live| live != 0).count();
    let bulk_entry_counts = bulk_entry_counts(&entries, live_count, total_rows);
    let live_handles = super::live_handles::LiveHandles::from_live(&live, live_count);
    // Re-derive metadata from the row storage, while retaining an explicitly
    // framed index after its last live row is removed.
    let metadata = (has_metadata || present).then(|| values.frame.clone());
    Ok(PySpatialIndex {
        rows: IndexRows {
            packed: None,
            boxed: rows,
            live,
            live_count,
            live_handles,
        },
        bulk: StaticStrTree::build(entries, total_rows).map_err(pyo3::PyErr::from)?,
        bulk_dead_entries: 0,
        bulk_entry_counts,
        overflow: StaticStrForest::new(total_rows),
        mutation_gen: 0,
        metadata,
        non_prunable_live,
    })
}

fn bulk_entry_counts(
    entries: &[IndexEntry],
    live_count: usize,
    total_rows: usize,
) -> Option<crate::geometry::SharedSlice<usize>> {
    if entries.len() == live_count {
        return None;
    }
    let mut counts = vec![0_usize; total_rows];
    for entry in entries {
        let count = counts
            .get_mut(entry.idx)
            .expect("bulk entry belongs to the admitted row table");
        *count = count.wrapping_add(1);
    }
    Some(crate::geometry::SharedSlice::from_vec(counts))
}

/// The CRS + coordinate epoch shared by every item, or `None` for an empty
/// index. Errors if items disagree — an index over mixed frames is meaningless.
pub(crate) fn index_metadata(items: &[PyGeometry]) -> PyResult<Option<Frame>> {
    if items.is_empty() {
        return Ok(None);
    }
    // The canonical shared-frame gate: per-axis CRSMismatchError messages.
    Ok(Some(metadata::common_frame(items, "spatial index")?))
}

#[cfg(test)]
mod ownership_tests {
    use std::sync::Arc;

    use rstar::AABB;

    use super::*;
    use crate::HeapSize;
    use crate::crs::frame;
    use crate::geometry::{
        Bounds, CoordSeq, CsrOffsetColumn, LineSeq, Point, PolygonLevel, RingLevel, Shape,
        SharedSlice,
    };

    fn points() -> PyGeometryArray {
        PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![0.0, 10.0], vec![0.0, 10.0], None, None).unwrap(),
            Frame::None,
        )
        .unwrap()
    }

    fn long_lines() -> PyGeometryArray {
        PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs((0..34).map(f64::from).collect(), vec![0.0; 34], None, None)
                .unwrap(),
            CsrOffsetColumn::try_new(vec![0, 34], 34).unwrap(),
            Frame::None,
        )
        .unwrap()
    }

    fn polygons() -> PyGeometryArray {
        PyGeometryArray::packed_polygons(
            CoordSeq::try_from_vecs(
                vec![0.0, 1.0, 1.0, 0.0, 0.0],
                vec![0.0, 0.0, 1.0, 1.0, 0.0],
                None,
                None,
            )
            .unwrap(),
            CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5], 5).unwrap(),
            CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1], 1).unwrap(),
            Frame::None,
        )
        .unwrap()
    }

    fn compact_points(missing: Vec<bool>) -> PyGeometryArray {
        let present = missing.iter().filter(|&&missing| !missing).count();
        let source = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(
                (0..present).map(|row| row as f64 * 10.0).collect(),
                vec![0.0; present],
                None,
                None,
            )
            .unwrap(),
            Frame::None,
        )
        .unwrap();
        let missing = storage::ValidityMask::from_vec(missing.len(), missing)
            .expect("fixture contains a missing row");
        PyGeometryArray::scatter_present_rows(&source, missing).unwrap()
    }

    fn compact_lines() -> PyGeometryArray {
        let source = PyGeometryArray::packed_lines(
            CoordSeq::try_from_vecs(vec![0.0, 1.0, 10.0, 11.0], vec![0.0; 4], None, None).unwrap(),
            CsrOffsetColumn::try_new(vec![0, 2, 4], 4).unwrap(),
            Frame::None,
        )
        .unwrap();
        let missing = storage::ValidityMask::from_vec(3, vec![false, true, false])
            .expect("fixture contains a missing row");
        PyGeometryArray::scatter_present_rows(&source, missing).unwrap()
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
    fn nullable_packed_points_keep_logical_handles_without_row_publication() {
        let source = compact_points(vec![true, false, true, false]);
        let before = source.row_products.initialized_counts();
        storage::reset_shape_iter_materializations();
        let index = packed_spatial_index(&source).unwrap().unwrap();

        assert_eq!(index.rows.live, [0, 1, 0, 1]);
        assert_eq!(index.live_handles_sorted(), [1, 3]);
        assert!(index.rows.packed.is_some());
        assert!(index.rows.boxed.is_empty());
        assert_eq!(source.row_products.initialized_counts(), before);
        assert_eq!(storage::shape_iter_materializations(), 0);

        let mut rows = index.rows.operation();
        let mut point = |handle| match rows.row(handle).unwrap() {
            storage::ShapeRow::Point(point) => point,
            storage::ShapeRow::Handle(_)
            | storage::ShapeRow::Shape(_)
            | storage::ShapeRow::Packed(_) => {
                panic!("compact point index resolved a non-point row")
            },
        };
        assert_eq!(point(1), Point::new_raw_xy(0.0, 0.0));
        assert_eq!(point(3), Point::new_raw_xy(10.0, 0.0));

        let mut hits = Vec::new();
        index.collect_intersecting(&AABB::from_point([0.0, 0.0]), &mut hits);
        assert_eq!(hits, [1]);
    }

    #[test]
    fn nullable_packed_lines_keep_logical_handles_without_row_publication() {
        let source = compact_lines();
        let index = packed_spatial_index(&source).unwrap().unwrap();

        assert_eq!(index.rows.live, [1, 0, 1]);
        assert_eq!(index.live_handles_sorted(), [0, 2]);
        assert!(index.rows.packed.is_some());
        assert!(index.rows.boxed.is_empty());
        let mut rows = index.rows.operation();
        assert_eq!(
            rows.row(0).unwrap().quick_bounds(),
            Some(Bounds::new_raw(0.0, 0.0, 1.0, 0.0))
        );
        assert_eq!(
            rows.row(2).unwrap().quick_bounds(),
            Some(Bounds::new_raw(10.0, 0.0, 11.0, 0.0))
        );
    }

    #[test]
    fn nullable_packed_windows_and_duplicate_gathers_resolve_rows_locally() {
        let source = compact_points(vec![true, false, false, true, false, true]);
        let window = source.gather_logical_row_range(1..5).unwrap();
        let index = packed_spatial_index(&window).unwrap().unwrap();
        assert!(window.is_compact_nullable());
        assert_eq!(index.live_handles_sorted(), [0, 1, 3]);

        let mut rows = index.rows.operation();
        let mut point_x = |handle| match rows.row(handle).unwrap() {
            storage::ShapeRow::Point(point) => point.x,
            storage::ShapeRow::Handle(_)
            | storage::ShapeRow::Shape(_)
            | storage::ShapeRow::Packed(_) => {
                panic!("compact point window resolved a non-point row")
            },
        };
        assert_eq!(point_x(0), 0.0);
        assert_eq!(point_x(1), 10.0);
        assert_eq!(point_x(3), 20.0);

        let gathered = source.gather_logical_rows(&[4, 2, 4, 1]).unwrap();
        let gathered_index = packed_spatial_index(&gathered).unwrap().unwrap();
        assert_eq!(gathered_index.live_handles_sorted(), [0, 1, 2, 3]);
        let mut gathered_rows = gathered_index.rows.operation();
        let mut point_x = |handle| match gathered_rows.row(handle).unwrap() {
            storage::ShapeRow::Point(point) => point.x,
            storage::ShapeRow::Handle(_)
            | storage::ShapeRow::Shape(_)
            | storage::ShapeRow::Packed(_) => {
                panic!("duplicate point gather resolved a non-point row")
            },
        };
        assert_eq!(
            (point_x(0), point_x(1), point_x(2), point_x(3)),
            (20.0, 10.0, 20.0, 0.0)
        );
    }

    #[test]
    fn nullable_packed_tiny_selection_does_not_warm_huge_parent_products() {
        let parent_len = 1_000_000;
        let mut missing = vec![true; parent_len];
        missing[parent_len - 2] = false;
        missing[parent_len - 1] = false;
        let source = compact_points(missing);
        let selected = source
            .gather_logical_row_range(parent_len - 3..parent_len)
            .unwrap();
        assert_eq!(selected.len(), 3);
        assert_eq!(selected.row_products.initialized_counts(), (0, 0, 0));

        let index = packed_spatial_index(&selected).unwrap().unwrap();
        assert_eq!(index.live_handles_sorted(), [1, 2]);
        assert_eq!(selected.row_products.initialized_counts(), (0, 0, 0));
        assert!(index.rows.boxed.is_empty());
    }

    /// A ring whose traversals cancel has no support; the index still holds
    /// its envelope, and exact refinement answers queries against it.
    #[test]
    fn cartesian_index_keeps_rows_with_cancelled_support() {
        pyo3::Python::initialize();
        let shape = crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0,0 2,2 2,2 0,0 0))")
            .expect("test WKT admits");
        let item = PyGeometry::with_frame(ShapeData::new(shape), Frame::None);
        let index = spatial_index(vec![item]).unwrap();
        assert_eq!(index.live_handles_sorted(), [0]);

        let array = PyGeometryArray::packed_polygons(
            CoordSeq::try_from_vecs(
                vec![0.0, 2.0, 2.0, 0.0, 0.0, 0.0, 2.0, 2.0, 0.0],
                vec![0.0, 0.0, 2.0, 2.0, 0.0, 2.0, 2.0, 0.0, 0.0],
                None,
                None,
            )
            .unwrap(),
            CsrOffsetColumn::<RingLevel>::try_new(vec![0, 9], 9).unwrap(),
            CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1], 1).unwrap(),
            Frame::None,
        )
        .unwrap();
        let index = packed_spatial_index(&array).unwrap().unwrap();
        assert_eq!(index.live_handles_sorted(), [0]);
    }

    #[test]
    fn packed_index_and_source_share_the_persistent_row_arc() {
        pyo3::Python::initialize();
        let source = long_lines();
        let index = packed_spatial_index(&source).unwrap().unwrap();
        let source_arc = match source.prepared_row(0, source.storage().row(0)).unwrap() {
            crate::array::PreparedRow::Shared(data) => Arc::clone(data),
            crate::array::PreparedRow::Owned(data) => data,
            crate::array::PreparedRow::Transient(_) => panic!("long packed row must persist"),
        };
        let indexed_arc = index
            .rows
            .operation()
            .prepared(0)
            .unwrap()
            .into_owned_plan();
        assert!(Arc::ptr_eq(&source_arc, &indexed_arc));

        let inverse = packed_spatial_index(&source).unwrap().unwrap();
        let indexed_first = inverse
            .rows
            .operation()
            .prepared(0)
            .unwrap()
            .into_owned_plan();
        let source_second = source
            .prepared_row(0, source.storage().row(0))
            .unwrap()
            .into_owned_plan();
        assert!(Arc::ptr_eq(&indexed_first, &source_second));
        let nbytes = Python::attach(|py| source.nbytes(py).unwrap().extract::<usize>().unwrap());
        assert!(crate::HeapSize::heap_bytes(&source) > nbytes);
    }

    #[test]
    fn pickle_restore_keeps_sparse_handles_on_the_packed_owner() {
        let source = PyGeometryArray::scatter_present_rows(
            &PyGeometryArray::packed_points(
                CoordSeq::try_from_vecs(vec![10.0], vec![10.0], None, None).unwrap(),
                Frame::None,
            )
            .unwrap(),
            storage::ValidityMask::from_vec(2, vec![true, false]).unwrap(),
        )
        .unwrap();
        let restored = restore_spatial_index(&source, false).unwrap();
        assert!(restored.rows.packed.is_some());
        assert!(restored.rows.boxed.is_empty());
        assert!(!restored.rows.is_live(0));
        assert!(restored.rows.is_live(1));
        assert_eq!(restored.rows.live, [0, 1]);
        assert_eq!(
            restored.rows.heap_bytes(),
            HeapSize::heap_bytes(&restored.rows)
        );
    }

    #[test]
    fn geographic_bulk_counts_keep_empty_and_multi_entry_handle_positions() {
        let frame =
            Frame::new(Some(frame::crs_arc_static("EPSG:4326")), None).expect("geographic frame");
        let point = PyGeometry::with_frame(
            ShapeData::new(Shape::Point(Point::new_raw_xy(10.0, 0.0))),
            frame.clone(),
        );
        let empty = PyGeometry::with_frame(ShapeData::new(Shape::empty_point()), frame.clone());
        let coordinates = CoordSeq::try_from_vecs(vec![170.0, -170.0], vec![0.0, 0.0], None, None)
            .expect("crossing coordinates");
        let crossing = PyGeometry::with_frame(
            ShapeData::new(Shape::LineString(
                LineSeq::try_new(coordinates).expect("crossing line"),
            )),
            frame.clone(),
        );
        let mut index = spatial_index(vec![point, empty, crossing]).expect("spatial index");
        assert_eq!(
            index
                .bulk_entry_counts
                .as_ref()
                .expect("periodic bulk counts")
                .as_slice(),
            [1, 0, 2]
        );
        assert_eq!(index.live_handles_sorted(), [0, 2]);

        let mut copied = index.clone();
        assert!(index.remove_handle(2).expect("remove periodic row"));
        assert_eq!(index.live_handles_sorted(), [0]);
        assert_eq!(index.bulk.entry_count(), 1);
        assert_eq!(index.bulk_dead_entries, 0);
        assert_eq!(
            index
                .bulk_entry_counts
                .as_ref()
                .expect("retained original counts")
                .as_slice(),
            [1, 0, 2]
        );
        assert_eq!(copied.live_handles_sorted(), [0, 2]);
        assert!(copied.remove_handle(2).expect("remove copied periodic row"));
        assert_eq!(copied.live_handles_sorted(), [0]);
        assert_eq!(
            copied
                .bulk_entry_counts
                .as_ref()
                .expect("copied original counts")
                .as_slice(),
            [1, 0, 2]
        );

        assert!(index.remove_handle(0).expect("remove remaining row"));
        assert_eq!(index.bulk.entry_count(), 0);
        let appended = PyGeometry::with_frame(
            ShapeData::new(Shape::Point(Point::new_raw_xy(30.0, 0.0))),
            frame,
        );
        assert_eq!(
            index
                .insert_one(&appended)
                .expect("append after compaction"),
            3
        );
        assert_eq!(index.live_handles_sorted(), [3]);
    }

    #[test]
    fn shared_bulk_counts_account_for_an_over_reserved_vector() {
        let mut values = Vec::with_capacity(17);
        values.extend([1_usize, 0, 2]);
        let capacity = values.capacity();
        let counts = SharedSlice::from_vec(values);
        assert!(capacity > counts.len());

        let expected_bytes = std::mem::size_of::<[usize; 2]>()
            .saturating_add(std::mem::size_of::<Vec<usize>>())
            .saturating_add(capacity.saturating_mul(std::mem::size_of::<usize>()));
        assert_eq!(counts.heap_bytes(), expected_bytes);
    }
}
