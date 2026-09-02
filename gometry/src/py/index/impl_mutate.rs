//! Internal (non-pymethods) mutation and handle helpers for
//! [`PySpatialIndex`], plus the envelope builder shared by insert lanes.

use crate::py::index::{
    Frame, GeometryError, IndexEntry, PyGeometry, PyGeometryArray, PyResult, PySpatialIndex,
    geodesic_prunable_point, index_envelopes, index_metadata,
};

impl PySpatialIndex {
    pub(crate) fn live_handles_sorted(&self) -> PyResult<Vec<usize>> {
        let mut handles = crate::try_collect_results(
            self.live_entries()
                .map(|entry| Ok::<_, pyo3::PyErr>(entry.idx)),
        )?;
        handles.sort_unstable();
        handles.dedup();
        Ok(handles)
    }

    pub(crate) fn is_live_handle(&self, handle: usize) -> bool {
        self.rows.is_live(handle)
    }

    /// Geometry at a live handle, ready for the Python leaf-typed boundary.
    ///
    /// Public pymethods must return [`crate::Typed`] so `isinstance(..., Point)`
    /// (and the other leaves) holds — a bare [`crate::PyGeometry`] silently
    /// yields the base `Geometry` class.
    pub(crate) fn geometry_at_handle(&self, handle: usize) -> crate::Typed {
        let frame = self.metadata.clone().unwrap_or_default();
        let packed_len = self
            .rows
            .packed
            .as_ref()
            .map_or(0, |packed| packed.source.len());
        if handle < packed_len {
            let geometry = self
                .rows
                .packed
                .as_ref()
                .expect("packed length implies packed storage")
                .source
                .geometry_at(handle);
            let plan = self.rows.prepared_row(handle).into_owned_plan();
            return crate::Typed(geometry.with_prepared_plan(plan));
        }
        let geometry = &self.rows.boxed[handle - packed_len];
        if let Some(plan) = geometry.cached_prepared_plan() {
            return crate::Typed(geometry.with_prepared_plan(plan));
        }
        crate::Typed(crate::PyGeometry::with_frame(
            crate::ShapeData::new(self.rows.row(handle).with_shape(std::clone::Clone::clone)),
            frame,
        ))
    }

    pub(crate) fn insert_one(&mut self, geometry: &PyGeometry) -> PyResult<usize> {
        let geometry = geometry.with_prepared_plan(geometry.plan_or_transient());
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
        let idx = self.rows.len();
        self.rows.try_reserve(1)?;
        // The geometry is indexable — adopt the frame only after all fallible
        // preparation for this mutation has succeeded.
        if self.metadata.is_none() {
            self.metadata = Some(geometry.frame.clone());
        }
        for &envelope in cover.entries() {
            self.overflow.insert(IndexEntry { idx, envelope });
        }
        self.non_prunable_live += usize::from(!geodesic_prunable_point(&geometry.shape));
        self.rows.push(geometry.clone());
        self.mutation_gen = self.mutation_gen.wrapping_add(1);
        Ok(idx)
    }

    pub(crate) fn insert_items(&mut self, items: Vec<PyGeometry>) -> PyResult<Vec<usize>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let items: Vec<PyGeometry> = items
            .into_iter()
            .map(|item| item.with_prepared_plan(item.plan_or_transient()))
            .collect();
        let frame = index_metadata(&items)?;
        if let Some(frame) = &frame {
            self.check_insert_frame(frame)?;
        }
        let adopt_frame = self.metadata.is_none().then(|| frame.clone()).flatten();
        let start = self.rows.len();
        let mut entries = Vec::new();
        if let Err(error) = crate::try_reserve_vec(&mut entries, items.len()) {
            drop(items);
            return Err(pyo3::PyErr::from(error));
        }
        let domain = crate::geometry::topology_domain(
            frame
                .as_ref()
                .or(self.metadata.as_ref())
                .expect("non-empty batch has a frame"),
        )?;
        let mut non_prunable_live = 0;
        for (offset, geometry) in items.iter().enumerate() {
            let bounds = geometry
                .shape
                .bounds()
                .ok_or_else(|| GeometryError::new_err("cannot index empty geometry"))?;
            let cover = index_envelopes(geometry.shape.shape(), bounds, domain)?;
            for &envelope in cover.entries() {
                if let Err(error) = crate::try_push_vec(&mut entries, IndexEntry {
                    idx: start + offset,
                    envelope,
                }) {
                    drop(items);
                    return Err(pyo3::PyErr::from(error));
                }
            }
            non_prunable_live += usize::from(!geodesic_prunable_point(&geometry.shape));
        }
        self.insert_prepared_batch(items, entries, non_prunable_live, adopt_frame)
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
        let mut entries = Vec::new();
        let mut items = Vec::new();
        if let Err(error) = crate::try_reserve_pair(&mut entries, &mut items, len) {
            return Err(pyo3::PyErr::from(error));
        }
        let domain = crate::geometry::topology_domain(&array.frame)?;
        let mut non_prunable_live = 0;
        for (offset, row) in array.storage().iter_rows().enumerate() {
            let bounds = row
                .quick_bounds()
                .ok_or_else(|| GeometryError::new_err("cannot index empty geometry"))?;
            let shape = row.with_shape(std::clone::Clone::clone);
            let cover = index_envelopes(&shape, bounds, domain)?;
            for &envelope in cover.entries() {
                if let Err(error) = crate::try_push_vec(&mut entries, IndexEntry {
                    idx: start + offset,
                    envelope,
                }) {
                    drop(items);
                    return Err(pyo3::PyErr::from(error));
                }
            }
            let plan = array.prepared_row(offset, row).into_owned_plan();
            non_prunable_live += usize::from(!geodesic_prunable_point(plan.shape()));
            if let Err(error) = crate::try_push_vec(
                &mut items,
                PyGeometry::from_prepared_plan(plan, array.frame.clone()),
            ) {
                drop(entries);
                return Err(pyo3::PyErr::from(error));
            }
        }
        self.insert_prepared_batch(items, entries, non_prunable_live, adopt_frame)
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
        non_prunable_live: usize,
        adopt_frame: Option<Frame>,
    ) -> PyResult<Vec<usize>> {
        debug_assert!(
            entries
                .iter()
                .all(|entry| entry.idx >= self.rows.len()
                    && entry.idx < self.rows.len() + items.len())
        );
        let start = self.rows.len();
        let count = items.len();
        let mut handles = crate::try_vec_with_capacity(count)?;
        for offset in 0..count {
            crate::try_push(&mut handles, start + offset)?;
        }
        self.rows.try_reserve(count)?;
        if let Some(frame) = adopt_frame {
            self.metadata = Some(frame);
        }
        for (offset, geometry) in items.into_iter().enumerate() {
            let handle = self.rows.push(geometry);
            debug_assert_eq!(handle, start + offset);
        }
        if self.overflow.size() == 0 {
            self.overflow = rstar::RTree::bulk_load(entries);
        } else {
            for entry in entries {
                self.overflow.insert(entry);
            }
        }
        self.non_prunable_live += non_prunable_live;
        self.mutation_gen = self.mutation_gen.wrapping_add(1);
        Ok(handles)
    }
}
