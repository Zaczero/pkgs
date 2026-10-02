//! Internal (non-pymethods) mutation and handle helpers for
//! [`PySpatialIndex`], plus the envelope builder shared by insert lanes.

use crate::py::index::{
    Frame, GeometryError, IndexEntry, PyGeometry, PyGeometryArray, PyResult, PySpatialIndex,
    TopologyDomain, geodesic_prunable_point, index_envelopes, index_metadata,
};

fn checked_total_rows(start: usize, count: usize) -> PyResult<usize> {
    start
        .checked_add(count)
        .ok_or(crate::error::ResourceError::SizeOverflow)
        .map_err(crate::error::Error::from)
        .map_err(pyo3::PyErr::from)
}

impl PySpatialIndex {
    pub(crate) fn live_handles_sorted(&self) -> Vec<usize> {
        let mut handles = self
            .live_entries()
            .map(|entry| entry.idx)
            .collect::<Vec<_>>();
        handles.sort_unstable();
        handles.dedup();
        handles
    }

    pub(crate) fn is_live_handle(&self, handle: usize) -> bool {
        self.rows.is_live(handle)
    }

    /// Geometry at a live handle, ready for the Python leaf-typed boundary.
    ///
    /// Public pymethods must return [`crate::Typed`] so `isinstance(..., Point)`
    /// (and the other leaves) holds — a bare [`crate::PyGeometry`] silently
    /// yields the base `Geometry` class.
    pub(crate) fn geometry_at_handle(&self, handle: usize) -> PyResult<crate::Typed> {
        let frame = self.metadata.clone().unwrap_or_default();
        let mut rows = self.rows.operation();
        match rows.resolve(handle).map_err(crate::PyErr::from)? {
            super::IndexRowSource::Packed {
                operation,
                logical,
                row,
            } => {
                let geometry = operation.geometry_for_resolved_row(logical, row)?;
                let plan = operation.prepared_at(logical, row)?.into_owned_plan();
                Ok(crate::Typed(geometry.with_prepared_plan(plan)))
            },
            super::IndexRowSource::Boxed(geometry) => {
                if let Some(plan) = geometry.cached_prepared_plan() {
                    return Ok(crate::Typed(geometry.with_prepared_plan(plan)));
                }
                Ok(crate::Typed(crate::PyGeometry::with_frame(
                    crate::ShapeData::new(geometry.shape.shape().clone()),
                    frame,
                )))
            },
        }
    }

    pub(crate) fn insert_one(&mut self, geometry: &PyGeometry) -> PyResult<usize> {
        let geometry = geometry.with_prepared_plan(geometry.prepared_plan());
        // A non-empty index validates the frame WITHOUT mutating; an empty index
        // will adopt this geometry's frame, but only AFTER the geometry is proven
        // indexable below — so a failed empty insert never frame-locks a fresh
        // index (which must stay free to accept any frame).
        if self.metadata.is_some() {
            // Exact identity: indexed geometries are later returned with the
            // index's single CRS label — do not silently retag.
            self.ensure_frame_compatible(
                geometry.crs_ref(),
                geometry.epoch(),
                "spatial index insert",
            )?;
        }
        // Geographic-ness of the EFFECTIVE frame: the index frame when set, else
        // this geometry's own frame (which becomes the index frame on the first
        // insert) — so the first geographic antimeridian-crossing row still gets
        // the wrapped-band envelope.
        let bounds = geometry
            .shape
            .bounds()
            .ok_or_else(|| GeometryError::new_err("cannot index empty geometry"))?;
        let effective_frame = self.metadata.as_ref().unwrap_or(&geometry.frame);
        let domain = crate::geometry::topology_domain(effective_frame)?;
        let cover = index_envelopes(geometry.shape.shape(), bounds, domain)?;
        let non_prunable_live = usize::from(
            matches!(domain, TopologyDomain::Geographic(_))
                && !geodesic_prunable_point(&geometry.shape),
        );
        let adopt_frame = self.metadata.is_none().then(|| geometry.frame.clone());
        let mut entry_counts = [0_usize; 1];
        let mut entries = Vec::with_capacity(cover.entries().len());
        let idx = self.rows.len();
        entries.extend(
            cover
                .entries()
                .iter()
                .map(|&envelope| IndexEntry { idx, envelope }),
        );
        entry_counts[0] = entries.len();
        let total_rows = checked_total_rows(idx, 1)?;
        self.insert_prepared_rows(
            std::iter::once(geometry),
            entries,
            &entry_counts,
            non_prunable_live,
            adopt_frame,
            total_rows,
        )
    }

    pub(crate) fn insert_items(&mut self, items: Vec<PyGeometry>) -> PyResult<Vec<usize>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let items = items
            .into_iter()
            .map(|item| item.with_prepared_plan(item.prepared_plan()))
            .collect::<Vec<_>>();
        let frame = index_metadata(&items)?;
        if let Some(frame) = &frame {
            self.check_insert_frame(frame)?;
        }
        let adopt_frame = self.metadata.is_none().then(|| frame.clone()).flatten();
        let start = self.rows.len();
        let mut entries = Vec::with_capacity(items.len());
        let mut entry_counts = Vec::with_capacity(items.len());
        let domain = crate::geometry::topology_domain(
            frame
                .as_ref()
                .or(self.metadata.as_ref())
                .expect("non-empty batch has a frame"),
        )?;
        let mut non_prunable_live = 0_usize;
        for (offset, geometry) in items.iter().enumerate() {
            let bounds = geometry
                .shape
                .bounds()
                .ok_or_else(|| GeometryError::new_err("cannot index empty geometry"))?;
            let cover = index_envelopes(geometry.shape.shape(), bounds, domain)?;
            for &envelope in cover.entries() {
                entries.push(IndexEntry {
                    idx: start.wrapping_add(offset),
                    envelope,
                });
            }
            entry_counts.push(cover.entries().len());
            if matches!(domain, TopologyDomain::Geographic(_)) {
                non_prunable_live = non_prunable_live
                    .wrapping_add(usize::from(!geodesic_prunable_point(&geometry.shape)));
            }
        }
        self.insert_prepared_batch(
            items,
            entries,
            &entry_counts,
            non_prunable_live,
            adopt_frame,
        )
    }

    pub(crate) fn insert_array(&mut self, array: &PyGeometryArray) -> PyResult<Vec<usize>> {
        let len = array.len();
        if len == 0 {
            return Ok(Vec::new());
        }
        // Packed storage retains placeholder coordinates for missing rows.
        // Reject the entire logical batch before frame adoption or any tree/
        // row mutation; indexing a placeholder would silently create a live
        // handle for a value the user did not provide.
        if array.has_missing() {
            return Err(GeometryError::new_err(
                "cannot insert a GeometryArray containing missing geometries into a spatial index",
            ));
        }
        self.check_insert_frame(&array.frame)?;
        let adopt_frame = self.metadata.is_none().then(|| array.frame.clone());
        let start = self.rows.len();
        let mut entries = Vec::with_capacity(len);
        let mut items = Vec::with_capacity(len);
        let mut entry_counts = Vec::with_capacity(len);
        let domain = crate::geometry::topology_domain(&array.frame)?;
        let mut non_prunable_live = 0_usize;
        for (offset, row) in array.storage().iter_rows().enumerate() {
            let bounds = row
                .quick_bounds()
                .ok_or_else(|| GeometryError::new_err("cannot index empty geometry"))?;
            let shape = row.with_shape(std::clone::Clone::clone)?;
            let cover = index_envelopes(&shape, bounds, domain)?;
            for &envelope in cover.entries() {
                entries.push(IndexEntry {
                    idx: start.wrapping_add(offset),
                    envelope,
                });
            }
            entry_counts.push(cover.entries().len());
            let plan = array.prepared_row(offset, row)?.into_owned_plan();
            if matches!(domain, TopologyDomain::Geographic(_)) {
                non_prunable_live = non_prunable_live
                    .wrapping_add(usize::from(!geodesic_prunable_point(plan.shape())));
            }
            items.push(PyGeometry::from_prepared_plan(plan, array.frame.clone()));
        }
        self.insert_prepared_batch(
            items,
            entries,
            &entry_counts,
            non_prunable_live,
            adopt_frame,
        )
    }

    pub(crate) fn check_insert_frame(&self, frame: &Frame) -> PyResult<()> {
        match &self.metadata {
            Some(_) => {
                // Exact identity — see insert_one.
                self.ensure_frame_compatible(frame.crs_ref(), frame.epoch(), "spatial index insert")
            },
            None => Ok(()),
        }
    }

    pub(crate) fn insert_prepared_batch(
        &mut self,
        items: Vec<PyGeometry>,
        entries: Vec<IndexEntry>,
        entry_counts: &[usize],
        non_prunable_live: usize,
        adopt_frame: Option<Frame>,
    ) -> PyResult<Vec<usize>> {
        let start = self.rows.len();
        let count = items.len();
        debug_assert_eq!(entry_counts.len(), count);
        debug_assert_eq!(entry_counts.iter().copied().sum::<usize>(), entries.len());
        let total_rows = checked_total_rows(start, count)?;
        let mut handles = Vec::with_capacity(count);
        handles.extend((0..count).map(|offset| start.wrapping_add(offset)));
        self.insert_prepared_rows(
            items.into_iter(),
            entries,
            entry_counts,
            non_prunable_live,
            adopt_frame,
            total_rows,
        )?;
        Ok(handles)
    }

    fn insert_prepared_rows(
        &mut self,
        items: impl ExactSizeIterator<Item = PyGeometry>,
        entries: Vec<IndexEntry>,
        entry_counts: &[usize],
        non_prunable_live: usize,
        adopt_frame: Option<Frame>,
        total_rows: usize,
    ) -> PyResult<usize> {
        let start = self.rows.len();
        let count = items.len();
        debug_assert_eq!(entry_counts.len(), count);
        debug_assert_eq!(entry_counts.iter().copied().sum::<usize>(), entries.len());
        debug_assert_eq!(total_rows.checked_sub(start), Some(count));
        debug_assert!(
            entries
                .iter()
                .all(|entry| entry.idx >= start && entry.idx < total_rows)
        );
        let prepared = self.overflow.prepare_insert(&entries, total_rows)?;
        self.rows.reserve_insert(count);
        self.overflow.reserve_entry_counts(count);
        if let Some(frame) = adopt_frame {
            self.metadata = Some(frame);
        }
        for (offset, geometry) in items.into_iter().enumerate() {
            let handle = self.rows.push(geometry);
            debug_assert_eq!(handle, start.wrapping_add(offset));
        }
        self.overflow.commit_insert(prepared, entry_counts);
        self.non_prunable_live = self.non_prunable_live.wrapping_add(non_prunable_live);
        self.mutation_gen = self.mutation_gen.wrapping_add(1);
        Ok(start)
    }

    pub(crate) fn remove_handle(&mut self, handle: usize) -> PyResult<bool> {
        if !self.rows.is_live(handle) {
            return Ok(false);
        }
        let non_prunable = if self.non_prunable_live == 0 {
            false
        } else {
            let mut rows = self.rows.operation();
            rows.row(handle)?
                .with_shape(|shape| !geodesic_prunable_point(shape))?
        };
        let removed = if handle < self.bulk.initial_len() {
            let removed_entries = self.bulk_entry_counts.as_ref().map_or(1, |counts| {
                *counts
                    .get(handle)
                    .expect("bulk handle belongs to the original cover count column")
            });
            let dead_after = self.bulk_dead_entries.wrapping_add(removed_entries);
            let prepared = if super::str_tree::compaction_due(self.bulk.entry_count(), dead_after) {
                Some(self.bulk.prepare_compaction(
                    &self.rows.live,
                    handle,
                    self.bulk.entry_count().wrapping_sub(dead_after),
                )?)
            } else {
                None
            };
            let removed = self.rows.mark_removed(handle);
            debug_assert!(removed);
            if let Some(prepared) = prepared {
                self.bulk = prepared;
                self.bulk_dead_entries = 0;
            } else {
                self.bulk_dead_entries = dead_after;
            }
            removed
        } else {
            // The forest records the physical cover size when the row is
            // inserted, so removal only consults that count. It never
            // re-materializes the geometry or recomputes its cover.
            let removed_entries = self.overflow.entry_count_for_handle(handle);
            let prepared = if self.overflow.should_compact(removed_entries) {
                Some(self.overflow.prepare_compaction(
                    &self.rows.live,
                    handle,
                    self.rows.len(),
                    removed_entries,
                )?)
            } else {
                None
            };
            let removed = self.rows.mark_removed(handle);
            debug_assert!(removed);
            if let Some(prepared) = prepared {
                self.overflow.commit_compaction(prepared);
            } else {
                self.overflow.mark_removed(removed_entries);
            }
            removed
        };
        if removed {
            if non_prunable {
                debug_assert!(self.non_prunable_live > 0);
                self.non_prunable_live = self.non_prunable_live.wrapping_sub(1);
            }
            self.mutation_gen = self.mutation_gen.wrapping_add(1);
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use rstar::AABB;

    use super::*;
    use crate::crs::frame;
    use crate::geometry::{CoordSeq, LineSeq, Point, Shape, ShapeData};
    use crate::py::index::{DistanceUnit, NearestOptions, spatial_index};

    fn point(x: f64, y: f64) -> PyGeometry {
        PyGeometry::with_frame(
            ShapeData::new(Shape::Point(Point::new_raw_xy(x, y))),
            Frame::None,
        )
    }

    fn empty_index() -> PySpatialIndex {
        spatial_index(Vec::new()).expect("empty index")
    }

    fn crossing_line() -> PyGeometry {
        let coordinates = CoordSeq::try_from_vecs(vec![170.0, -170.0], vec![0.0, 0.0], None, None)
            .expect("crossing coordinates");
        let shape = Shape::LineString(LineSeq::try_new(coordinates).expect("crossing line"));
        let frame =
            Frame::new(Some(frame::crs_arc_static("EPSG:4326")), None).expect("geographic frame");
        PyGeometry::with_frame(ShapeData::new(shape), frame)
    }

    fn framed_point(x: f64, y: f64, frame: &Frame) -> PyGeometry {
        PyGeometry::with_frame(
            ShapeData::new(Shape::Point(Point::new_raw_xy(x, y))),
            frame.clone(),
        )
    }

    fn clone_fixture() -> PySpatialIndex {
        let frame =
            Frame::new(Some(frame::crs_arc_static("EPSG:4326")), None).expect("geographic frame");
        let bulk = (0..4)
            .map(|handle| framed_point(f64::from(handle), 0.0, &frame))
            .collect();
        let mut index = spatial_index(bulk).expect("bulk index");
        let overflow = (4..17)
            .map(|handle| framed_point(f64::from(handle), 0.0, &frame))
            .collect();
        assert_eq!(
            index.insert_items(overflow).expect("overflow batch"),
            (4..17).collect::<Vec<_>>()
        );
        for handle in 4..10 {
            assert!(index.remove_handle(handle).expect("tombstone"));
        }
        assert!(index.remove_handle(10).expect("compacting tombstone"));
        assert_eq!(index.overflow.dead_entry_count(), 0);
        assert!(index.remove_handle(11).expect("retained tombstone"));
        assert_eq!(index.overflow.dead_entry_count(), 1);
        assert_eq!(
            index.insert_one(&crossing_line()).expect("crossing insert"),
            17
        );
        let additions = (18..21)
            .map(|handle| framed_point(f64::from(handle), 0.0, &frame))
            .collect();
        assert_eq!(
            index
                .insert_items(additions)
                .expect("post-compaction batch"),
            (18..21).collect::<Vec<_>>()
        );
        assert_eq!(index.overflow.total_entries(), 11);
        assert_eq!(index.overflow.live_entry_count(), 10);
        assert_eq!(index.overflow.dead_entry_count(), 1);
        index
    }

    fn run_weights(index: &PySpatialIndex) -> Vec<(usize, usize)> {
        index
            .overflow
            .occupied_runs()
            .map(|(rank, tree)| (rank, tree.entry_count()))
            .collect()
    }

    fn assert_copy_state(index: &PySpatialIndex, handles: &[usize], weights: &[(usize, usize)]) {
        assert_eq!(index.live_handles_sorted(), handles);
        assert_eq!(run_weights(index), weights);
        assert_eq!(index.overflow.total_entries(), 11);
        assert_eq!(index.overflow.live_entry_count(), 10);
        assert_eq!(index.overflow.dead_entry_count(), 1);
        assert_eq!(index.rows.live_count(), 13);
        assert_eq!(index.mutation_gen, 11);
        assert!(
            index
                .geodesic_caps
                .lock()
                .expect("caps lock")
                .caps
                .is_none()
        );
    }

    #[test]
    fn bulk_compaction_bounds_physical_entries_and_preserves_shared_copies() {
        let mut index = spatial_index(
            (0..256)
                .map(|handle| point(f64::from(handle), 0.0))
                .collect(),
        )
        .expect("bulk index");
        let copy = index.clone();
        for handle in 0..240 {
            assert!(index.remove_handle(handle).expect("bulk remove"));
        }
        assert!(index.bulk.entry_count() <= 32);
        assert_eq!(index.live_handles_sorted(), (240..256).collect::<Vec<_>>());
        assert_eq!(copy.bulk.entry_count(), 256);
        assert_eq!(copy.rows.live_count(), 256);
        assert_eq!(index.bulk.initial_len(), 256);
        assert_eq!(index.insert_one(&point(256.0, 0.0)).unwrap(), 256);
    }

    #[test]
    fn bulk_compaction_accounts_for_periodic_cover_aliases() {
        let mut index = spatial_index(vec![crossing_line(); 65]).expect("geographic index");
        let copy = index.clone();
        for handle in 0..61 {
            assert!(index.remove_handle(handle).expect("remove periodic cover"));
        }
        assert_eq!(index.live_handles_sorted(), [61, 62, 63, 64]);
        assert!(index.bulk.entry_count() <= 2 * index.live_entries().count());
        assert_eq!(copy.rows.live_count(), 65);
        assert_eq!(copy.bulk.entry_count(), copy.live_entries().count());
    }

    #[test]
    fn removal_without_compaction_only_tombstones_the_row() {
        let mut index = empty_index();
        for handle in 0..3 {
            assert_eq!(
                index
                    .insert_one(&point(handle as f64, 0.0))
                    .expect("insert"),
                handle
            );
        }
        let generation = index.mutation_gen;
        let removed =
            crate::test_support::assert_no_allocations(|| index.remove_handle(0).expect("remove"));
        assert!(removed);
        assert!(!index.rows.is_live(0));
        assert_eq!(index.overflow.total_entries(), 3);
        assert_eq!(index.overflow.live_entry_count(), 2);
        assert_eq!(index.overflow.dead_entry_count(), 1);
        assert_eq!(index.mutation_gen, generation + 1);
    }

    #[test]
    fn cloned_forests_mutate_independently() {
        let mut left = empty_index();
        assert_eq!(left.insert_one(&point(0.0, 0.0)).unwrap(), 0);
        let mut right = left.clone();
        assert_eq!(left.insert_one(&point(10.0, 0.0)).unwrap(), 1);
        assert_eq!(right.overflow.total_entries(), 1);
        assert_eq!(right.rows.live_count(), 1);
        assert_eq!(right.insert_one(&point(20.0, 0.0)).unwrap(), 1);
        assert_eq!(left.overflow.total_entries(), 2);
        assert_eq!(right.overflow.total_entries(), 2);
    }

    #[test]
    fn populated_index_clone_preserves_state_and_mutates_independently() {
        let source = clone_fixture();
        let handles = source.live_handles_sorted();
        let weights = run_weights(&source);
        assert_eq!(weights, [(0, 1), (1, 2), (3, 8)]);
        let counted = source.clone();
        assert_copy_state(&counted, &handles, &weights);
        let spatial_query = AABB::from_corners([11.5, -1.0], [19.5, 1.0]);
        let mut source_hits = Vec::new();
        source.collect_intersecting(&spatial_query, &mut source_hits);
        let mut clone_hits = Vec::new();
        counted.collect_intersecting(&spatial_query, &mut clone_hits);
        assert_eq!(source_hits, clone_hits);

        let query_frame =
            Frame::new(Some(frame::crs_arc_static("EPSG:4326")), None).expect("query frame");
        let query = framed_point(14.25, 0.0, &query_frame);
        let source_nearest = source
            .nearest_one(
                &query,
                4,
                Some(DistanceUnit::Planar),
                None,
                NearestOptions {
                    exclude_equal: false,
                    include_ties: false,
                },
            )
            .expect("source nearest");
        let clone_nearest = counted
            .nearest_one(
                &query,
                4,
                Some(DistanceUnit::Planar),
                None,
                NearestOptions {
                    exclude_equal: false,
                    include_ties: false,
                },
            )
            .expect("clone nearest");
        assert_eq!(source_nearest, clone_nearest);

        let mut cloned = source.clone();
        assert_copy_state(&cloned, &handles, &weights);
        let new_handle = cloned
            .insert_one(&framed_point(100.0, 0.0, &query_frame))
            .expect("clone insert");
        assert_eq!(new_handle, 21);
        assert!(source.rows.is_live(12));
        assert!(cloned.remove_handle(12).expect("clone remove"));
        assert!(source.rows.is_live(12));
        assert!(!cloned.rows.is_live(12));
    }

    #[test]
    fn public_copy_protocols_preserve_live_handles() {
        Python::initialize();
        Python::attach(|py| {
            let source = clone_fixture();
            let copy = source.__copy__(py).expect("copy");
            let memo = py.None().bind(py).clone();
            let deep = source.__deepcopy__(py, &memo).expect("deepcopy");
            assert_eq!(
                copy.bind(py).borrow().live_handles_sorted(),
                source.live_handles_sorted()
            );
            assert_eq!(
                deep.bind(py).borrow().live_handles_sorted(),
                source.live_handles_sorted()
            );
            assert_eq!(source.overflow.total_entries(), 11);
            assert_eq!(source.overflow.dead_entry_count(), 1);
        });
    }
}
