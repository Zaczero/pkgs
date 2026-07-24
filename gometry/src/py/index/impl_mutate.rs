//! Internal (non-pymethods) mutation and handle helpers for
//! [`PySpatialIndex`], plus the envelope builder shared by insert lanes.

use crate::collections::sort_row_ids;
use crate::py::index::*;

impl PySpatialIndex {
    pub(crate) fn live_handles_sorted(&self) -> Vec<usize> {
        let mut handles: Vec<usize> = self.live_entries().map(|entry| entry.idx).collect();
        sort_row_ids(&mut handles, self.rows.len());
        handles
    }

    pub(crate) fn is_live_handle(&self, handle: usize) -> bool {
        self.rows.is_live(handle)
    }

    pub(crate) fn geometry_at_handle(&self, handle: usize) -> crate::PyGeometry {
        crate::PyGeometry::with_frame(
            crate::ShapeData::new(self.rows.row(handle).with_shape(std::clone::Clone::clone)),
            self.metadata.clone().unwrap_or_default(),
        )
    }

    pub(crate) fn insert_one(&mut self, geometry: &PyGeometry) -> PyResult<usize> {
        // A non-empty index validates the frame WITHOUT mutating; an empty index
        // will adopt this geometry's frame, but only AFTER the geometry is proven
        // indexable below — so a failed empty insert never frame-locks a fresh
        // index (which must stay free to accept any frame).
        if self.metadata.is_some() {
            self.ensure_query_compatible(geometry, "spatial index insert")?;
        }
        // Geographic-ness of the EFFECTIVE frame: the index frame when set, else
        // this geometry's own frame (which becomes the index frame on the first
        // insert) — so the first geographic antimeridian-crossing row still gets
        // the wrapped-band envelope.
        let geographic = match &self.metadata {
            Some(_) => self.geographic(),
            None => crate::geometry::is_geographic_frame(&geometry.frame),
        };
        let envelope = if let Shape::Point(point) = geometry.shape.shape() {
            AABB::from_point([point.x, point.y])
        } else {
            let bounds = geometry
                .shape
                .bounds()
                .ok_or_else(|| GeometryError::new_err("cannot index empty geometry"))?;
            // Geographic antimeridian-crossing rows need the wrapped-band
            // envelope, exactly like build-time rows (build.rs) — the planar
            // bounds is the spurious false-middle box that would exclude the
            // row's true extent from envelope narrowing (e.g. a later
            // `query_pairs` missing the pair against a lower-id row).
            if geographic && geometry.shape.shape().crosses_antimeridian() {
                crossing_index_envelope(geometry.shape.shape(), bounds)
            } else {
                bounds_envelope(bounds)
            }
        };
        // The geometry is indexable — adopt the frame on the first insert, commit.
        if self.metadata.is_none() {
            self.metadata = Some(geometry.frame.clone());
        }
        let idx = self.rows.len();
        self.overflow.insert(IndexEntry { idx, envelope });
        self.non_prunable_live += usize::from(!geodesic_prunable_point(&geometry.shape));
        self.rows.push(geometry.clone());
        self.mutation_gen = self.mutation_gen.wrapping_add(1);
        Ok(idx)
    }

    pub(crate) fn insert_items(&mut self, items: Vec<PyGeometry>) -> PyResult<Vec<usize>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let frame = index_metadata(&items)?;
        if let Some(frame) = &frame {
            self.check_insert_frame(frame)?;
        }
        let adopt_frame = self.metadata.is_none().then(|| frame.clone()).flatten();
        let geographic = frame
            .as_ref()
            .or(self.metadata.as_ref())
            .is_some_and(crate::geometry::is_geographic_frame);
        let start = self.rows.len();
        let mut entries = Vec::with_capacity(items.len());
        let mut non_prunable_live = 0;
        for (offset, geometry) in items.iter().enumerate() {
            let bounds = geometry
                .shape
                .bounds()
                .ok_or_else(|| GeometryError::new_err("cannot index empty geometry"))?;
            entries.push(IndexEntry {
                idx: start + offset,
                envelope: insert_envelope(ShapeRow::Handle(&geometry.shape), bounds, geographic),
            });
            non_prunable_live += usize::from(!geodesic_prunable_point(&geometry.shape));
        }
        Ok(self.insert_prepared_batch(items, entries, non_prunable_live, adopt_frame))
    }

    pub(crate) fn insert_array(&mut self, array: &PyGeometryArray) -> PyResult<Vec<usize>> {
        let len = array.storage().len();
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
        let geographic = crate::geometry::is_geographic_frame(&array.frame);
        let start = self.rows.len();
        let mut entries = Vec::with_capacity(len);
        let mut items = Vec::with_capacity(len);
        let mut non_prunable_live = 0;
        for (offset, row) in array.storage().iter_rows().enumerate() {
            let bounds = row
                .quick_bounds()
                .ok_or_else(|| GeometryError::new_err("cannot index empty geometry"))?;
            entries.push(IndexEntry {
                idx: start + offset,
                envelope: insert_envelope(row, bounds, geographic),
            });
            let shape = row.into_shape_data(Some(bounds));
            non_prunable_live += usize::from(!geodesic_prunable_point(shape.shape()));
            items.push(PyGeometry::with_frame(shape, array.frame.clone()));
        }
        Ok(self.insert_prepared_batch(items, entries, non_prunable_live, adopt_frame))
    }

    pub(crate) fn check_insert_frame(&self, frame: &Frame) -> PyResult<()> {
        match &self.metadata {
            Some(_) => {
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
    ) -> Vec<usize> {
        debug_assert_eq!(items.len(), entries.len());
        let start = self.rows.len();
        let count = items.len();
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
        (start..start + count).collect()
    }
}

fn insert_envelope(row: ShapeRow<'_>, bounds: Bounds, geographic: bool) -> AABB<[f64; 2]> {
    if let ShapeRow::Point(point) = row {
        return AABB::from_point([point.x, point.y]);
    }
    if let Some(point) = row.with_shape(|shape| match shape {
        Shape::Point(point) => Some(*point),
        _ => None,
    }) {
        return AABB::from_point([point.x, point.y]);
    }
    if geographic && row.with_shape(Shape::crosses_antimeridian) {
        row.with_shape(|shape| crossing_index_envelope(shape, bounds))
    } else {
        bounds_envelope(bounds)
    }
}
