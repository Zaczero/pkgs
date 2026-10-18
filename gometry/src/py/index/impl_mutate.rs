//! Internal (non-pymethods) mutation and handle helpers for
//! [`PySpatialIndex`], plus the envelope builder shared by insert lanes.

use crate::error::Result;
use crate::geometry::GeometryErrorKind;
use crate::py::index::{
    Frame, IndexEntry, PyGeometry, PyGeometryArray, PySpatialIndex, geodesic_domain_invalid,
    index_envelopes, index_metadata,
};

impl PySpatialIndex {
    pub(crate) fn scalar_insert_is_local(&self, geometry: &PyGeometry) -> bool {
        matches!(geometry.shape.shape(), crate::Shape::Point(_))
            && geometry.frame == Frame::None
            && self
                .metadata
                .as_ref()
                .is_none_or(|frame| *frame == Frame::None)
            && !self.overflow.single_insert_carries()
            && match &self.rows.boxed {
                super::BoxedRows::Dense(rows) => {
                    self.rows.next_handle == self.rows.packed_len().wrapping_add(rows.len())
                        && rows.len() < rows.capacity()
                },
                super::BoxedRows::Sparse(_) => true,
            }
    }

    pub(crate) fn removal_is_local(&self, handle: usize) -> bool {
        if self.non_prunable_live != 0 {
            return false;
        }
        let removed_entries = self.cover_counts.get(&handle).copied().unwrap_or(1);
        let rebuilds = if handle < self.bulk.initial_len() {
            super::str_tree::compaction_due(
                self.bulk.entry_count(),
                self.bulk_dead_entries.wrapping_add(removed_entries),
            )
        } else {
            self.overflow.should_compact(removed_entries)
        };
        if rebuilds {
            return false;
        }
        if handle < self.rows.packed_len() {
            return true;
        }
        match &self.rows.boxed {
            super::BoxedRows::Dense(_) => false,
            super::BoxedRows::Sparse(rows) => rows.get(&handle).is_some_and(|geometry| {
                matches!(geometry.shape.shape(), crate::Shape::Point(_))
                    && geometry.row_products.is_scalar_owner()
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn live_handles_sorted(&self) -> Vec<usize> {
        let mut handles = Vec::with_capacity(self.rows.live_handles.len());
        self.rows.live_handles.append_ordered(&mut handles);
        handles
    }

    pub(crate) fn is_live_handle(&self, handle: usize) -> bool {
        self.rows.is_live(handle)
    }

    pub(crate) fn geometry_lookup_is_local(&self, handle: usize) -> Result<bool> {
        if handle >= self.rows.packed_len() {
            return Ok(true);
        }
        let source = &self
            .rows
            .packed
            .as_ref()
            .expect("packed handle has a source")
            .source;
        if source.row_products.cached_prepared_plan(handle).is_some() {
            return Ok(true);
        }
        Ok(self.rows.operation().row(handle)?.point().is_some())
    }

    pub(crate) fn geometry_at_handle(&self, handle: usize) -> Result<PyGeometry> {
        let frame = self.metadata.clone().unwrap_or_default();
        self.rows.operation().geometry(handle, frame)
    }

    pub(crate) fn live_geometries(&self) -> Result<Vec<(usize, PyGeometry)>> {
        let mut geometries = Vec::with_capacity(self.rows.live_handles.len());
        for handle in self.rows.live_handles.iter() {
            geometries.push((handle, self.geometry_at_handle(handle)?));
        }
        Ok(geometries)
    }

    pub(crate) fn insert_one(&mut self, geometry: &PyGeometry) -> Result<usize> {
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
            .ok_or_else(|| GeometryErrorKind::message("cannot index empty geometry"))?;
        let effective_frame = self.metadata.as_ref().unwrap_or(&geometry.frame);
        let domain = crate::geometry::topology_domain(effective_frame)?;
        let cover = index_envelopes(geometry.shape.shape(), bounds, domain)?;
        let non_prunable_live =
            usize::from(geodesic_domain_invalid(geometry.shape.shape(), domain));
        let adopt_frame = self.metadata.is_none().then(|| geometry.frame.clone());
        let mut entry_counts = [0_usize; 1];
        let mut entries = Vec::with_capacity(cover.entries().len());
        let idx = self.rows.next_handle;
        entries.extend(
            cover
                .entries()
                .iter()
                .map(|&envelope| IndexEntry { idx, envelope }),
        );
        entry_counts[0] = entries.len();
        let total_rows = super::checked_index_handle_end(idx, 1)?;
        self.insert_prepared_rows(
            std::iter::once(geometry),
            &entries,
            &entry_counts,
            non_prunable_live,
            adopt_frame,
            total_rows,
        )
    }

    pub(crate) fn insert_items(&mut self, items: Vec<PyGeometry>) -> Result<Vec<usize>> {
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
        let start = self.rows.next_handle;
        super::checked_index_handle_end(start, items.len())?;
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
                .ok_or_else(|| GeometryErrorKind::message("cannot index empty geometry"))?;
            let cover = index_envelopes(geometry.shape.shape(), bounds, domain)?;
            for &envelope in cover.entries() {
                entries.push(IndexEntry {
                    idx: start.wrapping_add(offset),
                    envelope,
                });
            }
            entry_counts.push(cover.entries().len());
            non_prunable_live = non_prunable_live.wrapping_add(usize::from(
                geodesic_domain_invalid(geometry.shape.shape(), domain),
            ));
        }
        self.insert_prepared_batch(
            items,
            &entries,
            &entry_counts,
            non_prunable_live,
            adopt_frame,
        )
    }

    pub(crate) fn insert_array(&mut self, array: &PyGeometryArray) -> Result<Vec<usize>> {
        let len = array.len();
        if len == 0 {
            return Ok(Vec::new());
        }
        // Packed storage retains placeholder coordinates for missing rows.
        // Reject the entire logical batch before frame adoption or any tree/
        // row mutation; indexing a placeholder would silently create a live
        // handle for a value the user did not provide.
        if array.has_missing() {
            return Err(GeometryErrorKind::message(
                "cannot insert a GeometryArray containing missing geometries into a spatial index",
            ));
        }
        self.check_insert_frame(&array.frame)?;
        let adopt_frame = self.metadata.is_none().then(|| array.frame.clone());
        let start = self.rows.next_handle;
        super::checked_index_handle_end(start, len)?;
        let mut entries = Vec::with_capacity(len);
        let mut items = Vec::with_capacity(len);
        let mut entry_counts = Vec::with_capacity(len);
        let domain = crate::geometry::topology_domain(&array.frame)?;
        let mut non_prunable_live = 0_usize;
        for (offset, row) in array.storage().iter_rows().enumerate() {
            let bounds = row
                .quick_bounds()
                .ok_or_else(|| GeometryErrorKind::message("cannot index empty geometry"))?;
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
            non_prunable_live = non_prunable_live
                .wrapping_add(usize::from(geodesic_domain_invalid(plan.shape(), domain)));
            items.push(PyGeometry::from_prepared_plan(&plan, array.frame.clone()));
        }
        self.insert_prepared_batch(
            items,
            &entries,
            &entry_counts,
            non_prunable_live,
            adopt_frame,
        )
    }

    pub(crate) fn check_insert_frame(&self, frame: &Frame) -> Result<()> {
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
        entries: &[IndexEntry],
        entry_counts: &[usize],
        non_prunable_live: usize,
        adopt_frame: Option<Frame>,
    ) -> Result<Vec<usize>> {
        let start = self.rows.next_handle;
        let count = items.len();
        debug_assert_eq!(entry_counts.len(), count);
        debug_assert_eq!(entry_counts.iter().copied().sum::<usize>(), entries.len());
        let total_rows = super::checked_index_handle_end(start, count)?;
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
        entries: &[IndexEntry],
        entry_counts: &[usize],
        non_prunable_live: usize,
        adopt_frame: Option<Frame>,
        total_rows: usize,
    ) -> Result<usize> {
        let start = self.rows.next_handle;
        let count = items.len();
        debug_assert_eq!(entry_counts.len(), count);
        debug_assert_eq!(entry_counts.iter().copied().sum::<usize>(), entries.len());
        debug_assert_eq!(total_rows.checked_sub(start), Some(count));
        debug_assert!(
            entries
                .iter()
                .all(|entry| entry.idx >= start && entry.idx < total_rows)
        );
        let prepared = self.overflow.prepare_insert(entries, total_rows)?;
        self.rows.reserve_insert(count);
        if let Some(frame) = adopt_frame {
            self.metadata = Some(frame);
        }
        for (offset, (geometry, entry_count)) in items
            .into_iter()
            .zip(entry_counts.iter().copied())
            .enumerate()
        {
            let handle = self.rows.push(geometry);
            debug_assert_eq!(handle, start.wrapping_add(offset));
            if entry_count > 1 {
                self.cover_counts.insert(handle, entry_count);
            }
        }
        self.overflow.commit_insert(prepared);
        self.non_prunable_live = self.non_prunable_live.wrapping_add(non_prunable_live);
        self.mutation_gen = self.mutation_gen.wrapping_add(1);
        Ok(start)
    }

    pub(crate) fn remove_handle(&mut self, handle: usize) -> Result<bool> {
        if !self.rows.is_live(handle) {
            return Ok(false);
        }
        let non_prunable = if self.non_prunable_live == 0 {
            false
        } else {
            let domain = self.domain()?;
            let mut rows = self.rows.operation();
            rows.row(handle)?
                .with_shape(|shape| geodesic_domain_invalid(shape, domain))?
        };
        let removed = if handle < self.bulk.initial_len() {
            let removed_entries = self.cover_counts.get(&handle).copied().unwrap_or(1);
            let dead_after = self.bulk_dead_entries.wrapping_add(removed_entries);
            let prepared = if super::str_tree::compaction_due(self.bulk.entry_count(), dead_after) {
                Some(self.bulk.prepare_compaction(
                    &self.rows.live_handles,
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
            let removed_entries = self.cover_counts.get(&handle).copied().unwrap_or(1);
            let prepared = if self.overflow.should_compact(removed_entries) {
                Some(self.overflow.prepare_compaction(
                    &self.rows.live_handles,
                    handle,
                    self.rows.next_handle,
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
            self.cover_counts.remove(&handle);
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
        assert_eq!(index.rows.live_handles.len(), 13);
        assert_eq!(index.mutation_gen, 11);
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
        assert_eq!(copy.rows.live_handles.len(), 256);
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
        assert_eq!(copy.rows.live_handles.len(), 65);
        assert_eq!(copy.bulk.entry_count(), copy.live_entries().count());
    }

    #[test]
    fn overflow_cover_exceptions_are_removed_with_periodic_rows() {
        let mut index = empty_index();
        assert_eq!(index.insert_one(&crossing_line()).unwrap(), 0);
        assert_eq!(index.cover_counts, [(0, 2)].into());

        assert!(index.remove_handle(0).unwrap());
        assert!(index.cover_counts.is_empty());
        assert_eq!(index.overflow.total_entries(), 0);
        assert_eq!(index.overflow.live_entry_count(), 0);
        assert_eq!(index.overflow.dead_entry_count(), 0);

        let frame = index.metadata.clone().unwrap();
        assert_eq!(
            index.insert_one(&framed_point(12.0, 0.0, &frame)).unwrap(),
            1
        );
        assert!(index.cover_counts.is_empty());
    }

    #[test]
    fn sparse_boxed_removal_reuses_payload_owner_without_compaction() {
        let mut index = empty_index();
        for handle in 0..6 {
            assert_eq!(
                index
                    .insert_one(&point(handle as f64, 0.0))
                    .expect("insert"),
                handle
            );
        }
        assert!(index.remove_handle(0).expect("first boxed remove"));
        let generation = index.mutation_gen;
        let removed =
            crate::test_support::assert_no_allocations(|| index.remove_handle(1).expect("remove"));
        assert!(removed);
        assert!(!index.rows.is_live(1));
        assert_eq!(index.overflow.total_entries(), 6);
        assert_eq!(index.overflow.live_entry_count(), 4);
        assert_eq!(index.overflow.dead_entry_count(), 2);
        assert_eq!(index.mutation_gen, generation + 1);
    }

    #[test]
    fn cloned_forests_mutate_independently() {
        let mut left = empty_index();
        assert_eq!(left.insert_one(&point(0.0, 0.0)).unwrap(), 0);
        let mut right = left.clone();
        assert_eq!(left.insert_one(&point(10.0, 0.0)).unwrap(), 1);
        assert_eq!(right.overflow.total_entries(), 1);
        assert_eq!(right.rows.live_handles.len(), 1);
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
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
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
