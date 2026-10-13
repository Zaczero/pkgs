use super::*;

#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q) struct FaceArc {
    pub(in crate::geometry::topology::q) from: GlobalFaceId,
    pub(in crate::geometry::topology::q) to: GlobalFaceId,
    pub(in crate::geometry::topology::q) span: usize,
    pub(in crate::geometry::topology::q) forward: bool,
}

pub(in crate::geometry::topology::q) struct FaceWalkFrame {
    pub(in crate::geometry::topology::q) next_arc: usize,
    pub(in crate::geometry::topology::q) end_arc: usize,
    pub(in crate::geometry::topology::q) undo: Option<(usize, i64)>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::geometry::topology) struct QFaceValidity {
    pub outside_hole: Option<(SourcePolygonId, SourceRingId)>,
    pub hole_overlap: Option<(SourcePolygonId, SourceRingId, SourceRingId)>,
    pub polygon_overlap: Option<(SourcePolygonId, SourcePolygonId)>,
}

pub(in crate::geometry::topology::q) struct FaceOccupancyState {
    pub(in crate::geometry::topology::q) winding: Vec<i64>,
    pub(in crate::geometry::topology::q) polygon_shells: Vec<usize>,
    pub(in crate::geometry::topology::q) polygon_holes: Vec<usize>,
    pub(in crate::geometry::topology::q) row_polygon_counts: Vec<usize>,
    pub(in crate::geometry::topology::q) active_holes: BTreeSet<(SourcePolygonId, SourceRingId)>,
    pub(in crate::geometry::topology::q) outside_hole_polygons: BTreeSet<SourcePolygonId>,
    pub(in crate::geometry::topology::q) overlapping_hole_polygons: BTreeSet<SourcePolygonId>,
    pub(in crate::geometry::topology::q) active_polygons: BTreeSet<(SourceRowId, SourcePolygonId)>,
    pub(in crate::geometry::topology::q) overlapping_rows: BTreeSet<SourceRowId>,
    pub(in crate::geometry::topology::q) active_count: usize,
    pub(in crate::geometry::topology::q) interval_starts: Vec<Option<usize>>,
    pub(in crate::geometry::topology::q) intervals: Vec<(SourceRowId, [usize; 2])>,
    pub(in crate::geometry::topology::q) cursor: usize,
}

impl FaceOccupancyState {
    pub(in crate::geometry::topology::q) fn new(
        sources: &ExactGeographicChartInventory,
        winding: &[i64],
    ) -> Result<Self> {
        if winding.len() != sources.rings.len() {
            return Err(GeometryErrorKind::Invalid(
                "Q face seed has the wrong source-ring winding count".into(),
            )
            .into());
        }
        let mut polygon_shells = vec![0_usize; sources.polygons.len()];
        let mut polygon_holes = vec![0_usize; sources.polygons.len()];
        for (ring, &ring_winding) in sources.rings.iter().zip(winding) {
            if ring_winding == 0 {
                continue;
            }
            let polygon = if ring.hole {
                polygon_holes.get_mut(ring.polygon.0 as usize)
            } else {
                polygon_shells.get_mut(ring.polygon.0 as usize)
            }
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring has an unknown polygon".into())
            })?;
            *polygon = polygon.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        let mut row_polygon_counts = vec![0_usize; sources.row_count];
        for ((polygon, &shells), &holes) in sources
            .polygons
            .iter()
            .zip(&polygon_shells)
            .zip(&polygon_holes)
        {
            if shells != 0 && holes == 0 {
                let count = row_polygon_counts
                    .get_mut(polygon.row.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q polygon has an unknown row".into())
                    })?;
                *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
        }
        let mut active_holes = BTreeSet::new();
        for (ring, &ring_winding) in sources.rings.iter().zip(winding) {
            if ring.hole && ring_winding != 0 {
                active_holes.insert((ring.polygon, ring.id));
            }
        }
        let mut outside_hole_polygons = BTreeSet::new();
        let mut overlapping_hole_polygons = BTreeSet::new();
        let mut active_polygons = BTreeSet::new();
        for (index, ((polygon, &shells), &holes)) in sources
            .polygons
            .iter()
            .zip(&polygon_shells)
            .zip(&polygon_holes)
            .enumerate()
        {
            let polygon_id =
                SourcePolygonId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?);
            if holes != 0 && shells == 0 {
                outside_hole_polygons.insert(polygon_id);
            }
            if holes >= 2 {
                overlapping_hole_polygons.insert(polygon_id);
            }
            if shells != 0 && holes == 0 {
                active_polygons.insert((polygon.row, polygon_id));
            }
        }
        let mut overlapping_rows = BTreeSet::new();
        for (index, &count) in row_polygon_counts.iter().enumerate() {
            if count >= 2 {
                overlapping_rows.insert(SourceRowId(
                    u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
                ));
            }
        }
        let interval_starts = row_polygon_counts
            .iter()
            .map(|&count| (count != 0).then_some(0))
            .collect::<Vec<_>>();
        let active_count = interval_starts
            .iter()
            .filter(|start| start.is_some())
            .count();
        Ok(Self {
            winding: winding.to_vec(),
            polygon_shells,
            polygon_holes,
            row_polygon_counts,
            active_holes,
            outside_hole_polygons,
            overlapping_hole_polygons,
            active_polygons,
            overlapping_rows,
            active_count,
            interval_starts,
            intervals: Vec::new(),
            cursor: 0,
        })
    }

    pub(in crate::geometry::topology::q) fn face_validity(
        &self,
        sources: &ExactGeographicChartInventory,
    ) -> Result<QFaceValidity> {
        let outside_hole = self.outside_hole_polygons.first().and_then(|&polygon| {
            self.active_holes
                .range((polygon, SourceRingId(0))..=(polygon, SourceRingId(u32::MAX)))
                .next()
                .map(|&(_, ring)| (polygon, ring))
        });
        let hole_overlap = self.overlapping_hole_polygons.first().and_then(|&polygon| {
            let mut rings = self
                .active_holes
                .range((polygon, SourceRingId(0))..=(polygon, SourceRingId(u32::MAX)))
                .map(|&(_, ring)| ring);
            rings
                .next()
                .zip(rings.next())
                .map(|(first, second)| (polygon, first, second))
        });
        let polygon_overlap = self.overlapping_rows.first().and_then(|&row| {
            let mut polygons = self
                .active_polygons
                .range((row, SourcePolygonId(0))..=(row, SourcePolygonId(u32::MAX)))
                .map(|&(_, polygon)| polygon);
            polygons.next().zip(polygons.next())
        });
        if outside_hole.is_some_and(|(polygon, ring)| {
            sources
                .polygons
                .get(polygon.0 as usize)
                .is_none_or(|record| !record.rings.contains(&(ring.0 as usize)))
        }) {
            return Err(GeometryErrorKind::Invalid(
                "Q outside-hole occupancy references an unknown source ring".into(),
            )
            .into());
        }
        Ok(QFaceValidity {
            outside_hole,
            hole_overlap,
            polygon_overlap,
        })
    }

    pub(in crate::geometry::topology::q) fn apply(
        &mut self,
        owners: &[QSpanOwner],
        multiplier: i64,
        sources: &ExactGeographicChartInventory,
    ) -> Result<()> {
        for_each_span_ring_transition(owners, &mut |transition| {
            self.apply_transition(transition, multiplier, sources)
        })
    }

    pub(in crate::geometry::topology::q) fn seam_owner_interior(
        &self,
        owner: SeamBoundaryOwner,
    ) -> Result<bool> {
        let ring_inside = self
            .winding
            .get(owner.ring.0 as usize)
            .map(|&winding| winding != 0)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q seam owner has an unknown ring".into()))?;
        let polygon = owner.polygon.0 as usize;
        let (Some(&shells), Some(&holes)) = (
            self.polygon_shells.get(polygon),
            self.polygon_holes.get(polygon),
        ) else {
            return Err(
                GeometryErrorKind::Invalid("Q seam owner has an unknown polygon".into()).into(),
            );
        };
        let polygon_inside = shells != 0 && holes == 0;
        Ok((if owner.hole {
            !ring_inside
        } else {
            ring_inside
        }) && polygon_inside)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "source ring and polygon IDs index state vectors sized from the same inventory"
    )]
    pub(in crate::geometry::topology::q) fn apply_transition(
        &mut self,
        transition: RingTransition,
        multiplier: i64,
        sources: &ExactGeographicChartInventory,
    ) -> Result<()> {
        let index = transition.ring.0 as usize;
        let winding = self
            .winding
            .get_mut(index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q transition has an unknown ring".into()))?;
        let delta = transition
            .delta
            .checked_mul(multiplier)
            .ok_or(ResourceError::SizeOverflow)?;
        let next = winding
            .checked_add(delta)
            .ok_or(ResourceError::SizeOverflow)?;
        let was_inside = *winding != 0;
        let is_inside = next != 0;
        *winding = next;
        if was_inside == is_inside {
            return Ok(());
        }
        let ring = sources.rings.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q transition has an unknown source ring".into())
        })?;
        if ring.hole {
            let active = (ring.polygon, ring.id);
            if is_inside {
                self.active_holes.insert(active);
            } else {
                self.active_holes.remove(&active);
            }
        }
        let polygon_index = ring.polygon.0 as usize;
        let (Some(&shells), Some(&holes)) = (
            self.polygon_shells.get(polygon_index),
            self.polygon_holes.get(polygon_index),
        ) else {
            return Err(
                GeometryErrorKind::Invalid("Q transition has an unknown polygon".into()).into(),
            );
        };
        let before_polygon = shells != 0 && holes == 0;
        let before_outside_hole = holes != 0 && shells == 0;
        let before_hole_overlap = holes >= 2;
        let count = if ring.hole {
            self.polygon_holes.get_mut(polygon_index)
        } else {
            self.polygon_shells.get_mut(polygon_index)
        }
        .ok_or_else(|| GeometryErrorKind::Invalid("Q transition has an unknown polygon".into()))?;
        *count = if is_inside {
            count.checked_add(1).ok_or(ResourceError::SizeOverflow)?
        } else {
            count.checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q polygon ring occupancy underflowed".into())
            })?
        };
        let after_polygon =
            self.polygon_shells[polygon_index] != 0 && self.polygon_holes[polygon_index] == 0;
        let after_shells = self.polygon_shells[polygon_index];
        let after_holes = self.polygon_holes[polygon_index];
        let after_outside_hole = after_holes != 0 && after_shells == 0;
        let after_hole_overlap = after_holes >= 2;
        if before_outside_hole != after_outside_hole {
            if after_outside_hole {
                self.outside_hole_polygons.insert(ring.polygon);
            } else {
                self.outside_hole_polygons.remove(&ring.polygon);
            }
        }
        if before_hole_overlap != after_hole_overlap {
            if after_hole_overlap {
                self.overlapping_hole_polygons.insert(ring.polygon);
            } else {
                self.overlapping_hole_polygons.remove(&ring.polygon);
            }
        }
        if before_polygon == after_polygon {
            return Ok(());
        }
        let row = sources
            .polygons
            .get(polygon_index)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q polygon state has no source polygon".into())
            })?
            .row;
        let row_count = self
            .row_polygon_counts
            .get_mut(row.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q polygon has an unknown row".into()))?;
        let before_row_count = *row_count;
        *row_count = if after_polygon {
            row_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
        } else {
            row_count.checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q row polygon occupancy underflowed".into())
            })?
        };
        if before_polygon {
            self.active_polygons.remove(&(row, ring.polygon));
        } else {
            self.active_polygons.insert((row, ring.polygon));
        }
        if before_row_count < 2 && *row_count >= 2 {
            self.overlapping_rows.insert(row);
        } else if before_row_count >= 2 && *row_count < 2 {
            self.overlapping_rows.remove(&row);
        }
        if *row_count == 0 {
            self.deactivate_row(row)?;
        } else if !before_polygon {
            self.activate_row(row)?;
        }
        Ok(())
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "active row count is bounded by the admitted row inventory"
    )]
    pub(in crate::geometry::topology::q) fn activate_row(
        &mut self,
        row: SourceRowId,
    ) -> Result<()> {
        let start = self
            .interval_starts
            .get_mut(row.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q active row is unknown".into()))?;
        if start.is_none() {
            *start = Some(self.cursor);
            self.active_count += 1;
        }
        Ok(())
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "taking an active interval removes exactly one counted row"
    )]
    pub(in crate::geometry::topology::q) fn deactivate_row(
        &mut self,
        row: SourceRowId,
    ) -> Result<()> {
        let start = self
            .interval_starts
            .get_mut(row.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q inactive row is unknown".into()))?;
        let start = start.take().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q row occupancy disappeared before its last polygon".into())
        })?;
        if start < self.cursor {
            self.intervals.push((row, [start, self.cursor]));
        }
        self.active_count -= 1;
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "each admitted global face is visited once"
    )]
    pub(in crate::geometry::topology::q) fn append_face(
        &mut self,
        face: GlobalFaceId,
        sources: &ExactGeographicChartInventory,
        order: &mut [usize],
        counts: &mut [usize],
        validity: &mut [QFaceValidity],
    ) -> Result<()> {
        order[face.0 as usize] = self.cursor;
        counts[face.0 as usize] = self.active_count;
        validity[face.0 as usize] = self.face_validity(sources)?;
        self.cursor += 1;
        Ok(())
    }
}
