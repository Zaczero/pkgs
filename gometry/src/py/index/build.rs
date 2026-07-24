use crate::py::index::*;
pub(crate) fn spatial_index(items: Vec<PyGeometry>) -> PyResult<PySpatialIndex> {
    let metadata = index_metadata(&items)?;
    Ok(spatial_index_rows(
        items.into_iter().map(Some).collect(),
        metadata,
    ))
}

/// Build over a sparse row table. ``None`` rows retain their input position as
/// a permanently non-live handle, so every reported handle remains an original
/// row id even when missing rows are skipped.
fn spatial_index_rows(items: Vec<Option<PyGeometry>>, metadata: Option<Frame>) -> PySpatialIndex {
    let geographic = metadata
        .as_ref()
        .is_some_and(crate::geometry::is_geographic_frame);
    let entries = items
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| {
            item.as_ref().and_then(|item| {
                item.shape.bounds().map(|bounds| {
                    let envelope = if geographic && item.shape.shape().crosses_antimeridian() {
                        crossing_index_envelope(item.shape.shape(), bounds)
                    } else {
                        bounds_envelope(bounds)
                    };
                    IndexEntry { idx, envelope }
                })
            })
        })
        .collect();
    let non_prunable_live = items
        .iter()
        .flatten()
        .filter(|item| item.shape.bounds().is_some() && !geodesic_prunable_point(&item.shape))
        .count();
    let live = items
        .iter()
        .map(|item| {
            item.as_ref()
                .is_some_and(|item| item.shape.bounds().is_some())
        })
        .collect();
    // `total_rows` spans every input row (incl. skipped empties) so the
    // tombstone bitmap and tree/overflow handle boundary stay aligned to the
    // boxed row positions the query results index into.
    let total_rows = items.len();
    let placeholder_frame = metadata.clone().unwrap_or_default();
    let rows = items
        .into_iter()
        .map(|item| {
            item.unwrap_or_else(|| {
                PyGeometry::with_frame(
                    PyGeometryArray::missing_placeholder(),
                    placeholder_frame.clone(),
                )
            })
        })
        .collect();
    PySpatialIndex {
        rows: IndexRows {
            packed: None,
            packed_frame_caches: None,
            boxed: rows,
            live,
        },
        bulk: StaticStrTree::build(entries, total_rows),
        overflow: RTree::new(),
        geodesic_caps: std::sync::Mutex::default(),
        mutation_gen: 0,
        metadata,
        non_prunable_live,
    }
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
) -> Option<Vec<IndexEntry>> {
    bounds
        .iter()
        .enumerate()
        .map(|(idx, row_bounds)| {
            row_bounds.as_ref().copied().map(|bounds| IndexEntry {
                idx,
                envelope: bounds_envelope(bounds),
            })
        })
        .collect()
}

/// Widen antimeridian-crossing rows of a geographic packed array to the
/// full-longitude band — the planar SIMD bounds fold gives the spurious
/// false-middle box, which would exclude the row's true wrapped extent from
/// envelope narrowing. No-op for projected/CRS-free arrays and non-crossing
/// rows. Points never cross, so only the Lines/Polygons builders call it.
fn widen_crossing_entries(array: &PyGeometryArray, entries: &mut [IndexEntry]) {
    if !crate::geometry::is_geographic_frame(&array.frame) {
        return;
    }
    for (entry, shape) in entries.iter_mut().zip(array.storage().iter_shapes()) {
        if shape.crosses_antimeridian() {
            let (lower, upper) = (entry.envelope.lower(), entry.envelope.upper());
            let planar = Bounds::new_unchecked(lower[0], lower[1], upper[0], upper[1]);
            entry.envelope = crossing_index_envelope(&shape, planar);
        }
    }
}

pub(crate) fn packed_spatial_index(array: &PyGeometryArray) -> Option<PySpatialIndex> {
    match array.storage() {
        GeometryArrayStorage::Points { coords, row_map } => {
            if coords.is_empty() {
                return None;
            }
            let map = row_map.as_deref();
            let len = crate::array::point_logical_len(coords, map);
            let entries: Vec<IndexEntry> = (0..len)
                .map(|logical| {
                    let point = coords.point_at(crate::array::physical_row(map, logical));
                    IndexEntry {
                        idx: logical,
                        envelope: AABB::from_point([point.x, point.y]),
                    }
                })
                .collect();
            let non_prunable_live = (0..len)
                .filter(|&logical| {
                    let point = coords.point_at(crate::array::physical_row(map, logical));
                    !((-180.0..=180.0).contains(&point.x) && (-90.0..=90.0).contains(&point.y))
                })
                .count();
            let total_rows = entries.len();
            Some(PySpatialIndex {
                rows: IndexRows {
                    packed: Some(Arc::clone(array.storage_arc())),
                    packed_frame_caches: Some(array.frame_caches.clone()),
                    boxed: Vec::new(),
                    live: vec![true; total_rows],
                },
                bulk: StaticStrTree::build(entries, total_rows),
                overflow: RTree::new(),
                geodesic_caps: std::sync::Mutex::default(),
                mutation_gen: 0,
                metadata: Some(array.frame.clone()),
                non_prunable_live,
            })
        },
        GeometryArrayStorage::Lines { .. } | GeometryArrayStorage::Polygons { .. } => {
            let bounds = array.cached_element_bounds()?;
            if bounds.is_empty() {
                return None;
            }
            let mut entries = bounds_index_entries(&bounds)?;
            widen_crossing_entries(array, &mut entries);
            let total_rows = entries.len();
            Some(PySpatialIndex {
                rows: IndexRows {
                    packed: Some(Arc::clone(array.storage_arc())),
                    packed_frame_caches: Some(array.frame_caches.clone()),
                    boxed: Vec::new(),
                    live: vec![true; total_rows],
                },
                bulk: StaticStrTree::build(entries, total_rows),
                overflow: RTree::new(),
                geodesic_caps: std::sync::Mutex::default(),
                mutation_gen: 0,
                metadata: Some(array.frame.clone()),
                non_prunable_live: total_rows,
            })
        },
        GeometryArrayStorage::Mixed(_) => None,
    }
}

/// Build a spatial index from any geometry source. A packed point array
/// takes the column-direct lane.
pub(crate) fn build_spatial_index(values: &Bound<'_, PyAny>) -> PyResult<PySpatialIndex> {
    if let Some(geometry) = exact_geometry(values) {
        return spatial_index(vec![geometry.clone()]);
    }
    if let Some(array) = exact_geometry_array(values) {
        if !array.has_missing()
            && let Some(index) = packed_spatial_index(array)
        {
            return Ok(index);
        }
        let rows: Vec<Option<PyGeometry>> = array
            .masked_storage_rows()
            .map(|(missing, row)| {
                (!missing).then(|| {
                    PyGeometry::with_frame(
                        row.into_shape_data(row.quick_bounds()),
                        array.frame.clone(),
                    )
                })
            })
            .collect();
        // A zero-row source leaves the index frame-FREE (adopted by the first
        // insert), exactly like building from an empty iterable — locking an
        // empty array's frame would reject every future differently-framed
        // insert for no reason.
        let metadata = (!rows.is_empty()).then(|| array.frame.clone());
        return Ok(spatial_index_rows(rows, metadata));
    }

    let raw_items = crate::collect_py_iter(values, Ok)?;
    if raw_items.iter().all(|item| !item.is_none()) {
        return spatial_index(
            crate::coerce_collected_geometry_items(
                &raw_items,
                true,
                crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
            )?
            .into_items(),
        );
    }
    let present: Vec<_> = raw_items
        .iter()
        .filter(|item| !item.is_none())
        .cloned()
        .collect();
    let present = crate::coerce_collected_geometry_items(
        &present,
        true,
        crate::io::LegacyGeoJsonCrsPolicy::Adopt(None),
    )?
    .into_items();
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
    Ok(spatial_index_rows(rows, metadata))
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
    let mut rows = crate::try_vec_with_capacity_hint(values.storage().len())?;
    for row in values.storage().iter_rows() {
        crate::try_push(
            &mut rows,
            PyGeometry::with_frame(
                ShapeData::new(row.with_shape(std::clone::Clone::clone)),
                values.frame.clone(),
            ),
        )?;
    }
    let total_rows = rows.len();
    let geographic = crate::geometry::is_geographic_frame(&values.frame);
    let mut seen = vec![false; total_rows];
    let mut entries = crate::try_vec_with_capacity_hint(live_handles.len())?;
    let mut non_prunable_live = 0;
    for &handle in live_handles {
        let Some(item) = rows.get(handle) else {
            return Err(GeometryError::new_err(format!(
                "spatial index pickle payload live handle {handle} is out of range"
            )));
        };
        if std::mem::replace(&mut seen[handle], true) {
            return Err(GeometryError::new_err(format!(
                "spatial index pickle payload repeats live handle {handle}"
            )));
        }
        let bounds = item.shape.bounds().ok_or_else(|| {
            GeometryError::new_err(format!(
                "spatial index pickle payload marks empty handle {handle} as live"
            ))
        })?;
        let envelope = if geographic && item.shape.shape().crosses_antimeridian() {
            crossing_index_envelope(item.shape.shape(), bounds)
        } else {
            bounds_envelope(bounds)
        };
        crate::try_push(&mut entries, IndexEntry {
            idx: handle,
            envelope,
        })?;
        non_prunable_live += usize::from(!geodesic_prunable_point(&item.shape));
    }
    // Re-derive metadata from the row storage — never trust a serialized flag.
    let metadata = index_metadata(&rows)?;
    Ok(PySpatialIndex {
        rows: IndexRows {
            packed: None,
            packed_frame_caches: None,
            boxed: rows,
            live: seen,
        },
        bulk: StaticStrTree::build(entries, total_rows),
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
