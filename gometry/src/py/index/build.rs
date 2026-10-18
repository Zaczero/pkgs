use super::live_handles::LiveHandles;
use crate::boundary::metadata;
use crate::error::Result;
use crate::geometry::{GeometryErrorKind, TopologyDomain, storage};
use crate::py::index::{
    Bound, BoxedRows, Frame, GeometryArrayStorage, IndexEntry, IndexRows, PackedRowAddressing,
    PackedStore, PyAny, PyAnyMethods as _, PyGeometry, PyGeometryArray, PyResult, PySpatialIndex,
    Python, ShapeData, StaticStrForest, StaticStrTree, bounds_envelope, checked_index_handle_end,
    cover_count_exceptions, exact_geometry, exact_geometry_array, geodesic_domain_invalid,
    index_envelopes,
};

pub(crate) fn spatial_index(mut items: Vec<PyGeometry>) -> Result<PySpatialIndex> {
    let metadata = index_metadata(&items)?;
    let scan = scan_spatial_index_rows(items.iter().map(Some), metadata.as_ref())?;
    for item in &mut items {
        *item = item.with_prepared_plan(item.prepared_plan());
    }
    let boxed = BoxedRows::from_dense(items, &scan.live_handles);
    finish_spatial_index(boxed, scan, metadata)
}

struct IndexScan {
    total_rows: usize,
    entries: Vec<IndexEntry>,
    live_handles: LiveHandles,
    non_prunable_live: usize,
}

/// Build over sparse input rows. ``None`` rows consume an input handle without
/// entering the live-handle directory, so later rows retain their original ids.
fn spatial_index_rows(
    items: Vec<Option<PyGeometry>>,
    metadata: Option<Frame>,
) -> Result<PySpatialIndex> {
    let scan = scan_spatial_index_rows(items.iter().map(Option::as_ref), metadata.as_ref())?;
    let rows = items
        .into_iter()
        .map(|item| item.map(|item| item.with_prepared_plan(item.prepared_plan())))
        .collect();
    let boxed = BoxedRows::from_optional(rows, &scan.live_handles, scan.total_rows);
    finish_spatial_index(boxed, scan, metadata)
}

fn scan_spatial_index_rows<'a>(
    items: impl ExactSizeIterator<Item = Option<&'a PyGeometry>>,
    metadata: Option<&Frame>,
) -> Result<IndexScan> {
    let total_rows = checked_index_handle_end(0, items.len())?;
    let domain = metadata
        .map(crate::geometry::topology_domain)
        .transpose()?
        .unwrap_or(TopologyDomain::Cartesian);
    let mut entries = Vec::new();
    let mut live_handles = LiveHandles::default();
    let mut non_prunable_live = 0_usize;
    for (idx, item) in items.enumerate() {
        let Some(item) = item else {
            continue;
        };
        let Some(bounds) = item.shape.bounds() else {
            continue;
        };
        let cover = index_envelopes(item.shape.shape(), bounds, domain)?;
        for &envelope in cover.entries() {
            entries.push(IndexEntry { idx, envelope });
        }
        live_handles.insert(idx);
        if matches!(domain, TopologyDomain::Geographic(_)) {
            non_prunable_live = non_prunable_live.wrapping_add(usize::from(
                geodesic_domain_invalid(item.shape.shape(), domain),
            ));
        }
    }
    Ok(IndexScan {
        total_rows,
        entries,
        live_handles,
        non_prunable_live,
    })
}

fn finish_spatial_index(
    boxed: BoxedRows,
    scan: IndexScan,
    metadata: Option<Frame>,
) -> Result<PySpatialIndex> {
    // Skipped empty rows consume handles, so the initial tree boundary remains
    // the complete input length even though the directory contains only live rows.
    let total_rows = scan.total_rows;
    let cover_counts = cover_count_exceptions(&scan.entries);
    Ok(PySpatialIndex {
        rows: IndexRows {
            packed: None,
            boxed,
            next_handle: total_rows,
            live_handles: scan.live_handles,
        },
        bulk: StaticStrTree::build(scan.entries, total_rows)?,
        bulk_dead_entries: 0,
        cover_counts,
        overflow: StaticStrForest::new(),
        mutation_gen: 0,
        metadata,
        non_prunable_live: scan.non_prunable_live,
    })
}

/// Build packed entries directly from the retained array owner. Empty and
/// missing rows consume their logical handles without becoming live.
fn packed_index_payload(
    array: &PyGeometryArray,
    domain: TopologyDomain,
) -> Result<(Vec<IndexEntry>, LiveHandles, usize, PackedRowAddressing)> {
    let present_count = array
        .missing()
        .map_or(array.len(), storage::ValidityMask::present_count);
    let mut entries = Vec::with_capacity(present_count);
    let mut live_handles = LiveHandles::default();
    let mut non_prunable_live = 0_usize;
    let mut present_ordinals = (array.missing().is_some()
        && matches!(
            array.storage(),
            GeometryArrayStorage::Packed(storage::PackedStorage::Points { .. })
        ))
    .then(|| Vec::with_capacity(present_count));
    let element_bounds = (!matches!(
        array.storage(),
        GeometryArrayStorage::Packed(storage::PackedStorage::Points { .. })
    ))
    .then(|| array.element_bounds())
    .flatten();
    let rows = array.row_operation();
    for logical_row in rows.rows_with_missing(|| ()) {
        let crate::array::ArrayRow::Present(row) = logical_row else {
            continue;
        };
        let logical = row.logical();
        if let Some(handles) = &mut present_ordinals {
            // A present empty point occupies a physical ordinal but no live index row.
            handles.push(logical);
        }
        let shape_row = row.row();
        let bounds = element_bounds
            .as_ref()
            .and_then(|bounds| bounds.get(logical).copied().flatten())
            .or_else(|| shape_row.quick_bounds());
        let Some(bounds) = bounds else {
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
                Ok::<_, crate::error::Error>(())
            })?,
        }
        live_handles.insert(logical);
        if matches!(domain, TopologyDomain::Geographic(_)) {
            non_prunable_live = non_prunable_live.wrapping_add(usize::from(row_non_prunable));
        }
    }
    let addressing = present_ordinals.map_or(PackedRowAddressing::Logical, |handles| {
        debug_assert_eq!(handles.len(), present_count);
        debug_assert_eq!(handles.len(), array.storage().len());
        PackedRowAddressing::PresentOrdinals(std::sync::Arc::from(handles))
    });
    Ok((entries, live_handles, non_prunable_live, addressing))
}

pub(crate) fn packed_spatial_index(array: &PyGeometryArray) -> Result<Option<PySpatialIndex>> {
    if array.len() == 0 || !matches!(array.storage(), GeometryArrayStorage::Packed(_)) {
        return Ok(None);
    }
    let total_rows = checked_index_handle_end(0, array.len())?;
    let metadata = array
        .missing()
        .is_none_or(|missing| missing.present_count() != 0)
        .then(|| array.frame.clone());
    let domain = metadata
        .as_ref()
        .map(crate::geometry::topology_domain)
        .transpose()?
        .unwrap_or(TopologyDomain::Cartesian);
    let (entries, live_handles, non_prunable_live, addressing) =
        packed_index_payload(array, domain)?;
    let cover_counts = cover_count_exceptions(&entries);
    Ok(Some(PySpatialIndex {
        rows: IndexRows {
            packed: Some(PackedStore {
                source: array.clone(),
                addressing,
            }),
            boxed: BoxedRows::default(),
            next_handle: total_rows,
            live_handles,
        },
        bulk: StaticStrTree::build(entries, total_rows)?,
        bulk_dead_entries: 0,
        cover_counts,
        overflow: StaticStrForest::new(),
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
        return Ok(py.detach(|| spatial_index(vec![geometry.clone()]))?);
    }
    if let Some(array) = exact_geometry_array(values) {
        return Ok(py.detach(|| spatial_index_array(array))?);
    }

    let raw_items = crate::collect_py_iter(values, Ok)?;
    if raw_items.iter().all(|item| !item.is_none()) {
        let items = crate::coerce_present_geometry_items(
            raw_items.iter(),
            true,
            crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
        )?;
        return Ok(py.detach(move || spatial_index(items))?);
    }
    let present = crate::coerce_present_geometry_items(
        raw_items.iter().filter(|item| !item.is_none()),
        true,
        crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
    )?;
    let missing: Vec<_> = raw_items
        .iter()
        .map(pyo3::types::PyAnyMethods::is_none)
        .collect();
    Ok(py.detach(move || {
        let metadata = index_metadata(&present)?;
        let mut present = present.into_iter();
        let rows = missing
            .into_iter()
            .map(|missing| {
                (!missing).then(|| {
                    present
                        .next()
                        .expect("present rows match the admitted input mask")
                })
            })
            .collect();
        spatial_index_rows(rows, metadata)
    })?)
}

fn spatial_index_array(array: &PyGeometryArray) -> Result<PySpatialIndex> {
    if let Some(index) = packed_spatial_index(array)? {
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
                    &plan,
                    array.frame.clone(),
                )))
            },
        })
        .collect::<Result<Vec<_>>>()?;
    // Missing rows do not establish a frame for later insertions.
    let metadata = rows
        .iter()
        .any(Option::is_some)
        .then(|| array.frame.clone());
    spatial_index_rows(rows, metadata)
}

pub(crate) fn restore_spatial_index(
    values: &PyGeometryArray,
    handles: &[usize],
    next_handle: usize,
    has_metadata: bool,
) -> Result<PySpatialIndex> {
    let total_rows = checked_index_handle_end(0, next_handle)?;
    if handles.len() != values.len() {
        return Err(GeometryErrorKind::message(
            "spatial index pickle handle count does not match value count",
        ));
    }
    if values.has_missing() {
        return Err(GeometryErrorKind::message(
            "spatial index pickle values must not contain missing geometries",
        ));
    }
    if handles.iter().any(|&handle| handle >= total_rows) {
        return Err(GeometryErrorKind::message(
            "spatial index pickle handle must be less than next_handle",
        ));
    }
    if handles
        .array_windows::<2>()
        .any(|[left, right]| left >= right)
    {
        return Err(GeometryErrorKind::message(
            "spatial index pickle handles must be strictly increasing",
        ));
    }

    let metadata = (has_metadata || !handles.is_empty()).then(|| values.frame.clone());
    let domain = metadata
        .as_ref()
        .map(crate::geometry::topology_domain)
        .transpose()?
        .unwrap_or(TopologyDomain::Cartesian);
    let mut entries = Vec::with_capacity(values.len());
    let mut rows = Vec::with_capacity(values.len());
    let mut non_prunable_live = 0_usize;
    let mut row_operation = values.row_operation();
    for (logical, &handle) in handles.iter().enumerate() {
        let row = row_operation.present_row_at(logical).ok_or_else(|| {
            GeometryErrorKind::message("spatial index pickle values must be present")
        })?;
        let plan = std::sync::Arc::new(crate::PreparedPlan::new(ShapeData::new(
            row.row().with_shape(std::clone::Clone::clone)?,
        )));
        let geometry = PyGeometry::from_prepared_plan(&plan, values.frame.clone());
        let Some(bounds) = geometry.shape.bounds() else {
            return Err(GeometryErrorKind::message(format!(
                "spatial index pickle payload marks empty handle {handle} as live"
            )));
        };
        let cover = index_envelopes(geometry.shape.shape(), bounds, domain)?;
        for &envelope in cover.entries() {
            entries.push(IndexEntry {
                idx: handle,
                envelope,
            });
        }
        if matches!(domain, TopologyDomain::Geographic(_)) {
            non_prunable_live = non_prunable_live.wrapping_add(usize::from(
                geodesic_domain_invalid(geometry.shape.shape(), domain),
            ));
        }
        rows.push(geometry);
    }

    let live_handles = LiveHandles::from_sorted(handles.iter().copied());
    let boxed = BoxedRows::from_pairs(handles, rows, total_rows);
    let cover_counts = cover_count_exceptions(&entries);
    Ok(PySpatialIndex {
        rows: IndexRows {
            packed: None,
            boxed,
            next_handle: total_rows,
            live_handles,
        },
        bulk: StaticStrTree::build(entries, total_rows)?,
        bulk_dead_entries: 0,
        cover_counts,
        overflow: StaticStrForest::new(),
        mutation_gen: 0,
        metadata,
        non_prunable_live,
    })
}

/// The CRS + coordinate epoch shared by every item, or `None` for an empty
/// index. Errors if items disagree — an index over mixed frames is meaningless.
pub(crate) fn index_metadata(items: &[PyGeometry]) -> Result<Option<Frame>> {
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

    fn sparse_nullable_points(logical_len: usize) -> PyGeometryArray {
        let present_handles = [
            7_usize,
            63,
            logical_len
                .checked_sub(1)
                .expect("sparse test points require nonempty logical rows"),
        ];
        let mut bitmap = vec![0_u8; logical_len.div_ceil(8)];
        for handle in present_handles {
            *bitmap
                .get_mut(handle / 8)
                .expect("present handle belongs to the sparse point mask") |= 1 << (handle % 8);
        }
        let mask =
            storage::ValidityMask::from_bitmap(SharedSlice::from_vec(bitmap), 0, logical_len)
                .expect("sparse point fixture includes missing rows");
        let source = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![10.0, 20.0, 30.0], vec![1.0, 2.0, 3.0], None, None)
                .unwrap(),
            Frame::None,
        )
        .unwrap();
        PyGeometryArray::scatter_present_rows(&source, mask).unwrap()
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
        let (point_bounds, point_allocations) = crate::test_support::allocation_count(|| {
            point_index
                .rows
                .operation()
                .row(0)
                .expect("packed point")
                .quick_bounds()
        });
        assert_eq!(point_allocations, 0);
        assert!(point_bounds.is_some());

        let line_index = packed_spatial_index(&long_lines()).unwrap().unwrap();
        assert!(line_index.rows.packed.is_some());
        assert!(line_index.rows.boxed.is_empty());
        let (_, line_allocations) = crate::test_support::allocation_count(|| {
            line_index
                .rows
                .operation()
                .row(0)
                .expect("packed line")
                .quick_bounds()
        });
        assert_eq!(line_allocations, 0);

        let polygon_index = packed_spatial_index(&polygons()).unwrap().unwrap();
        assert!(polygon_index.rows.packed.is_some());
        assert!(polygon_index.rows.boxed.is_empty());
        let (_, polygon_allocations) = crate::test_support::allocation_count(|| {
            polygon_index
                .rows
                .operation()
                .row(0)
                .expect("packed polygon")
                .quick_bounds()
        });
        assert_eq!(polygon_allocations, 0);

        let source = points();
        let mut index = packed_spatial_index(&source).unwrap().unwrap();
        let inserted = index.rows.packed.as_ref().unwrap().source.clone();
        let handles = index.insert_array(&inserted).unwrap();
        assert_eq!(handles, [2, 3]);
        assert_eq!(index.rows.boxed.len(), 2);
    }

    #[test]
    fn compact_point_index_heap_excludes_missing_handle_span() {
        let small_source = sparse_nullable_points(1_000);
        let large_source = sparse_nullable_points(0x0002_0000);
        let small = packed_spatial_index(&small_source).unwrap().unwrap();
        let large = packed_spatial_index(&large_source).unwrap().unwrap();
        let small_delta = HeapSize::heap_bytes(&small)
            .checked_sub(HeapSize::heap_bytes(&small_source))
            .expect("index includes its retained source");
        let large_delta = HeapSize::heap_bytes(&large)
            .checked_sub(HeapSize::heap_bytes(&large_source))
            .expect("index includes its retained source");
        assert_eq!(small_delta, large_delta);
    }

    #[test]
    fn dense_boxed_rows_keep_vector_lookup_and_release_removed_copies() {
        let first = PyGeometry::with_frame(
            ShapeData::new(Shape::Point(Point::new_raw_xy(-3.0, 2.0))),
            Frame::None,
        );
        let first_shape = Arc::downgrade(&first.shape);
        let second = PyGeometry::with_frame(
            ShapeData::new(Shape::Point(Point::new_raw_xy(1.0, 2.0))),
            Frame::None,
        );
        let second_shape = Arc::downgrade(&second.shape);
        let rows = [first, second]
            .into_iter()
            .chain((2..8).map(|row| {
                PyGeometry::with_frame(
                    ShapeData::new(Shape::Point(Point::new_raw_xy(f64::from(row), 2.0))),
                    Frame::None,
                )
            }))
            .collect();
        let mut index = spatial_index(rows).expect("dense boxed index");
        assert!(matches!(&index.rows.boxed, BoxedRows::Dense(_)));

        let (bounds, allocations) = crate::test_support::allocation_count(|| {
            index
                .rows
                .operation()
                .row(7)
                .expect("dense boxed row")
                .quick_bounds()
        });
        assert_eq!(allocations, 0);
        assert!(bounds.is_some());

        let mut copy = index.clone();
        let (_, first_remove_allocations) =
            crate::test_support::allocation_count(|| index.remove_handle(0).unwrap());
        assert!(first_remove_allocations > 0);
        assert!(matches!(&index.rows.boxed, BoxedRows::Sparse(_)));
        assert!(first_shape.upgrade().is_some());
        assert_eq!(index.rows.boxed.len(), 7);
        assert_eq!(copy.rows.boxed.len(), 8);
        assert!(copy.remove_handle(0).unwrap());
        assert!(first_shape.upgrade().is_none());
        drop(copy);
        assert!(index.remove_handle(1).unwrap());
        assert!(second_shape.upgrade().is_none());
    }

    #[test]
    fn nullable_packed_points_keep_logical_handles_without_row_publication() {
        let source = compact_points(vec![true, false, true, false]);
        let before = source.row_products.initialized_counts();
        storage::reset_shape_iter_materializations();
        let index = packed_spatial_index(&source).unwrap().unwrap();

        assert_eq!(index.rows.live_handles.iter().collect::<Vec<_>>(), [1, 3]);
        assert_eq!(index.live_handles_sorted(), [1, 3]);
        assert!(index.rows.packed.is_some());
        assert!(index.rows.boxed.is_empty());
        let directory = match &index.rows.packed.as_ref().unwrap().addressing {
            PackedRowAddressing::PresentOrdinals(handles) => Arc::clone(handles),
            PackedRowAddressing::Logical => panic!("compact nullable points need ordinals"),
        };
        let copy = index.clone();
        let copy_directory = match &copy.rows.packed.as_ref().unwrap().addressing {
            PackedRowAddressing::PresentOrdinals(handles) => Arc::clone(handles),
            PackedRowAddressing::Logical => panic!("compact nullable point copy needs ordinals"),
        };
        assert!(Arc::ptr_eq(&directory, &copy_directory));
        assert_eq!(&*directory, [1, 3]);
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
    fn compact_point_addressing_keeps_present_empty_ordinals() {
        let present = PyGeometryArray::packed_points(
            CoordSeq::try_from_vecs(vec![f64::NAN, 30.0], vec![f64::NAN, 40.0], None, None)
                .unwrap(),
            Frame::None,
        )
        .unwrap();
        let source = PyGeometryArray::scatter_present_rows(
            &present,
            storage::ValidityMask::from_vec(3, vec![false, true, false]).unwrap(),
        )
        .unwrap();
        assert_eq!(source.storage().len(), 2);
        assert_eq!(source.missing().unwrap().present_count(), 2);

        let index = packed_spatial_index(&source).unwrap().unwrap();
        let handles = match &index.rows.packed.as_ref().unwrap().addressing {
            PackedRowAddressing::PresentOrdinals(handles) => handles,
            PackedRowAddressing::Logical => panic!("compact nullable points need ordinals"),
        };
        assert_eq!(&**handles, [0, 2]);
        assert_eq!(index.live_handles_sorted(), [2]);
        assert!(!index.rows.is_live(0));
        assert_eq!(
            index
                .rows
                .operation()
                .row(2)
                .unwrap()
                .quick_bounds()
                .unwrap()
                .minx(),
            30.0
        );
    }

    #[test]
    fn nullable_packed_lines_keep_logical_handles_without_row_publication() {
        let source = compact_lines();
        let index = packed_spatial_index(&source).unwrap().unwrap();

        assert_eq!(index.rows.live_handles.iter().collect::<Vec<_>>(), [0, 2]);
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
    fn nullable_packed_tail_selection_does_not_warm_parent_products() {
        let parent_len = 129;
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
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
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
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
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
    fn restore_rebuilds_sparse_payloads_under_original_handles() {
        let values = PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(7.0, 1.0)),
                Shape::Point(Point::new_raw_xy(63.0, 2.0)),
                Shape::Point(Point::new_raw_xy(f64::from(0x0001_0001_u32), 3.0)),
            ],
            Frame::None,
        )
        .unwrap();
        let mut index =
            restore_spatial_index(&values, &[7, 63, 0x0001_0001], 70_000, false).unwrap();

        assert!(matches!(&index.rows.boxed, BoxedRows::Sparse(_)));
        assert_eq!(index.rows.next_handle, 70_000);
        assert_eq!(index.live_handles_sorted(), [7, 63, 0x0001_0001]);
        let mut rows = index.rows.operation();
        assert_eq!(rows.row(7).unwrap().quick_bounds().unwrap().minx(), 7.0);
        assert_eq!(rows.row(63).unwrap().quick_bounds().unwrap().minx(), 63.0);
        assert_eq!(
            rows.row(0x0001_0001)
                .unwrap()
                .quick_bounds()
                .unwrap()
                .minx(),
            f64::from(0x0001_0001_u32)
        );

        assert_eq!(
            index
                .insert_one(&PyGeometry::with_frame(
                    ShapeData::new(Shape::Point(Point::new_raw_xy(70_000.0, 4.0))),
                    Frame::None,
                ))
                .unwrap(),
            70_000
        );
        let mut copy = index.clone();
        assert!(copy.remove_handle(63).unwrap());
        assert!(index.rows.is_live(63));
        assert!(!copy.rows.is_live(63));
    }

    #[test]
    fn exhausted_handle_domain_rejects_insert_without_state_change() {
        let maximum = isize::MAX as usize;
        let values = PyGeometryArray::from_shapes(Vec::new(), Frame::None).unwrap();
        let mut index = restore_spatial_index(&values, &[], maximum, false).unwrap();
        let geometry = PyGeometry::with_frame(
            ShapeData::new(Shape::Point(Point::new_raw_xy(4.0, 5.0))),
            Frame::None,
        );

        let error = index
            .insert_one(&geometry)
            .expect_err("identity domain exhausted");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
        assert_eq!(index.rows.next_handle, maximum);
        assert!(index.rows.live_handles.is_empty());
        assert!(index.metadata.is_none());
        assert_eq!(index.mutation_gen, 0);
    }

    #[test]
    fn sparse_owner_costs_follow_live_rows_not_next_handle() {
        let values = PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(7.0, 1.0)),
                Shape::Point(Point::new_raw_xy(63.0, 2.0)),
                Shape::Point(Point::new_raw_xy(f64::from(0x0001_0001_u32), 3.0)),
            ],
            Frame::None,
        )
        .unwrap();
        let handles = [7, 63, 0x0001_0001];
        let small = restore_spatial_index(&values, &handles, 70_000, false).unwrap();
        let large = restore_spatial_index(&values, &handles, 1 << 40, false).unwrap();

        assert_eq!(HeapSize::heap_bytes(&small), HeapSize::heap_bytes(&large));
        let (small_size, small_size_allocations) =
            crate::test_support::allocation_count(|| HeapSize::heap_bytes(&small));
        let (large_size, large_size_allocations) =
            crate::test_support::allocation_count(|| HeapSize::heap_bytes(&large));
        assert_eq!(small_size, large_size);
        assert_eq!(small_size_allocations, 0);
        assert_eq!(large_size_allocations, 0);

        let (small_copy, small_copy_allocations) =
            crate::test_support::allocation_count(|| small.clone());
        let (large_copy, large_copy_allocations) =
            crate::test_support::allocation_count(|| large.clone());
        assert_eq!(small_copy_allocations, large_copy_allocations);
        assert_eq!(
            small_copy.live_handles_sorted(),
            large_copy.live_handles_sorted()
        );

        let mut small_rows = small.rows.operation();
        let (small_bounds, small_resolve_allocations) =
            crate::test_support::allocation_count(|| {
                small_rows.row(0x0001_0001).unwrap().quick_bounds()
            });
        let mut large_rows = large.rows.operation();
        let (large_bounds, large_resolve_allocations) =
            crate::test_support::allocation_count(|| {
                large_rows.row(0x0001_0001).unwrap().quick_bounds()
            });
        assert_eq!(small_bounds, large_bounds);
        assert!(small_bounds.is_some());
        assert_eq!(small_resolve_allocations, 0);
        assert_eq!(large_resolve_allocations, 0);

        LiveHandles::reset_visit_counts();
        let small_ids: Vec<_> = small.rows.live_handles.iter().collect();
        let small_iteration_visits = LiveHandles::iterated_handle_count();
        assert_eq!(small_iteration_visits, handles.len());
        let (small_pickle, small_pickle_allocations) =
            crate::test_support::allocation_count(|| {
                small
                    .rows
                    .pickle_live_rows(Frame::None)
                    .expect("compact small pickle rows")
            });
        assert_eq!(
            LiveHandles::membership_check_count(),
            handles.len(),
            "pickle resolves each live row without probing the historical handle span"
        );
        LiveHandles::reset_visit_counts();
        let large_ids: Vec<_> = large.rows.live_handles.iter().collect();
        let large_iteration_visits = LiveHandles::iterated_handle_count();
        assert_eq!(large_iteration_visits, handles.len());
        let (large_pickle, large_pickle_allocations) =
            crate::test_support::allocation_count(|| {
                large
                    .rows
                    .pickle_live_rows(Frame::None)
                    .expect("compact large pickle rows")
            });
        assert_eq!(LiveHandles::membership_check_count(), handles.len());
        assert_eq!(small_ids, handles);
        assert_eq!(large_ids, handles);
        assert_eq!(small_pickle.1, large_pickle.1);
        assert_eq!(small_pickle.0.len(), large_pickle.0.len());
        assert_eq!(small_pickle_allocations, large_pickle_allocations);
    }

    #[test]
    fn cover_count_exceptions_follow_live_periodic_handles() {
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
        assert_eq!(index.cover_counts, [(2, 2)].into());
        assert_eq!(index.live_handles_sorted(), [0, 2]);

        let mut copied = index.clone();
        assert!(index.remove_handle(2).expect("remove periodic row"));
        assert_eq!(index.live_handles_sorted(), [0]);
        assert!(index.cover_counts.is_empty());
        assert_eq!(index.bulk.entry_count(), 1);
        assert_eq!(index.bulk_dead_entries, 0);
        assert_eq!(copied.live_handles_sorted(), [0, 2]);
        assert_eq!(copied.cover_counts, [(2, 2)].into());
        assert!(copied.remove_handle(2).expect("remove copied periodic row"));
        assert_eq!(copied.live_handles_sorted(), [0]);
        assert!(copied.cover_counts.is_empty());

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
}
