use super::*;

impl ExactSourceInventory {
    /// Stage each operand as a set, visiting repeated shared collection owners once.
    pub(in crate::geometry) fn from_shapes<S: Borrow<Shape>>(shapes: &[S]) -> Result<Self> {
        Self::from_rows(
            shapes
                .iter()
                .map(|shape| storage::ShapeRow::Shape(shape.borrow())),
        )
    }

    pub(in crate::geometry) fn from_rows<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
    ) -> Result<Self> {
        Self::from_staged(StagedExactSources::from_rows(rows)?)
    }

    /// Preserve every source occurrence for consumers whose result observes
    /// multiplicity, such as DE-9IM line-boundary facts and polygonize diagnostics.
    pub(in crate::geometry) fn from_shapes_with_occurrences<S: Borrow<Shape>>(
        shapes: &[S],
    ) -> Result<Self> {
        Self::from_rows_with_occurrences(
            shapes
                .iter()
                .map(|shape| storage::ShapeRow::Shape(shape.borrow())),
        )
    }

    pub(in crate::geometry) fn from_rows_with_occurrences<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
    ) -> Result<Self> {
        Self::stage_rows(rows)
    }

    pub(super) fn stage_rows<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
    ) -> Result<Self> {
        Self::from_staged(StagedExactSources::from_rows_with_occurrences(rows, false)?)
    }

    /// Build only the sources that can select occupied two-dimensional faces.
    ///
    /// Point and line sources can subdivide an areal boundary, but cannot alter
    /// its selected faces. Their extra boundary crosses telescope exactly
    /// across the homogeneous subdivisions, so they do not belong in this
    /// area-only partition.
    pub(super) fn from_occupied_area_shape(shape: &Shape) -> Result<Option<Self>> {
        let mut staged = StagedExactSources::new(1);
        let mut collection_owners = SharedShapeOwners::for_collections();
        staged.append_occupied_area_shape(shape, 0, &mut collection_owners)?;
        if staged.polygon_count == 0 {
            return Ok(None);
        }
        Self::from_staged(staged).map(Some)
    }

    /// Stage polygonal inputs as one operand for their union, or `None` when
    /// an input is not polygonal. Union selects a cell any operand occupies,
    /// and a polygonal operand is the union of its polygons, so the selection
    /// is unchanged while each per-operand cell fact keeps one entry instead
    /// of one per input. Line inputs keep their own operands: line boundary
    /// counts are per operand.
    pub(in crate::geometry) fn from_polygonal_union(shapes: &[&Shape]) -> Result<Option<Self>> {
        if !shapes
            .iter()
            .all(|shape| matches!(shape, Shape::Polygon(_) | Shape::MultiPolygon(_)))
        {
            return Ok(None);
        }
        let mut staged = StagedExactSources::new(1);
        let mut owners = SharedShapeOwners::for_collections();
        for shape in shapes {
            staged.append_shape(shape, 0, &mut owners)?;
        }
        Self::from_staged(staged).map(Some)
    }

    pub(super) fn from_staged(staged: StagedExactSources) -> Result<Self> {
        let (unique, provenance) =
            dedup_undirected_segments_sourced(staged.segments.iter().copied())?;
        Ok(Self {
            source_segments: staged.segments,
            unique,
            provenance,
            source_operands: staged.source_operands,
            source_rings: staged.source_rings,
            ring_kinds: staged.ring_kinds,
            ring_polygons: staged.ring_polygons,
            ring_holes: staged.ring_holes,
            polygon_count: staged.polygon_count,
            polygon_operands: staged.polygon_operands,
            polygon_ring_ranges: staged.polygon_ring_ranges,
            source_points: staged.source_points,
            polygon_rings: staged.polygon_rings,
            ring_count: staged.ring_count,
            isolated: staged.isolated,
            isolated_points: staged.isolated_points,
            isolated_operands: staged.isolated_operands,
            line_boundaries: staged.line_boundaries,
        })
    }

    pub(super) fn isolated_point_occurrences<'a>(
        &'a self,
        partition: &'a ExactPartition<'_>,
    ) -> impl Iterator<Item = (NodeId, Point, u32)> + 'a {
        debug_assert_eq!(self.isolated.len(), self.isolated_points.len());
        debug_assert_eq!(self.isolated.len(), self.isolated_operands.len());
        debug_assert_eq!(self.isolated.len(), partition.point_nodes.len());
        partition
            .point_nodes
            .iter()
            .copied()
            .zip(self.isolated_points.iter().copied())
            .zip(self.isolated_operands.iter().copied())
            .map(|((node, point), operand)| (node, point, operand))
    }

    pub(in crate::geometry) fn from_polygon_operands<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
        left: &[P],
        right: &[Q],
    ) -> Result<Self> {
        let mut staged = StagedExactSources::new(2);
        for polygon in left {
            staged.append_polygon(polygon.as_ref(), 0)?;
        }
        for polygon in right {
            staged.append_polygon(polygon.as_ref(), 1)?;
        }
        Self::from_staged(staged)
    }

    pub(super) fn from_polygon_rect_clip<P: AsRef<Polygon>>(
        polygons: &[P],
        rectangle: &Polygon,
        bounds: Bounds,
        rectangle_first: bool,
    ) -> Result<Option<(Self, Vec<SegmentClipInterval>)>> {
        let mut staged = StagedExactSources::new(2);
        if rectangle_first {
            staged.append_polygon(rectangle, 1)?;
        }
        for polygon in polygons {
            staged.append_polygon(polygon.as_ref(), 0)?;
        }
        if !rectangle_first {
            staged.append_polygon(rectangle, 1)?;
        }

        let mut active_segments = Vec::new();
        let mut active_sources = Vec::new();
        for (source, (&segment, &operand)) in staged
            .segments
            .iter()
            .zip(&staged.source_operands)
            .enumerate()
        {
            if operand == 0 {
                let interior = |point: XY| {
                    point.x > bounds.minx()
                        && point.x < bounds.maxx()
                        && point.y > bounds.miny()
                        && point.y < bounds.maxy()
                };
                let contact = if interior(segment.start) || interior(segment.end) {
                    RectContact::Open
                } else {
                    segment_vs_rect(segment, bounds)
                };
                if contact == RectContact::Closed {
                    // Contact-only cells use the full partition's point/edge owner.
                    return Ok(None);
                }
                if contact == RectContact::None {
                    continue;
                }
            }
            let source = u32::try_from(source).map_err(|_| ResourceError::SizeOverflow)?;
            active_segments.push(segment);
            active_sources.push(source);
        }
        let (unique, local_provenance) =
            dedup_undirected_segments_sourced(active_segments.iter().copied())?;
        let mut offsets = Vec::with_capacity(
            unique
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
        let mut entries = Vec::with_capacity(active_sources.len());
        offsets.push(0);
        for owners in local_provenance.iter() {
            for &(active, reversed) in owners {
                let source = active_sources
                    .get(active as usize)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "rectangle-local provenance lost its full source ordinal".into(),
                        )
                    })?;
                entries.push((source, reversed));
            }
            offsets.push(u32::try_from(entries.len()).map_err(|_| ResourceError::SizeOverflow)?);
        }
        let provenance = SegmentProvenance::from_csr(offsets, entries);

        let mut intervals = Vec::with_capacity(unique.len());
        for &segment in &unique {
            intervals.push(clip_segment_rect_interval(segment, bounds)?.ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "a rectangle-contact segment has no closed clip interval".into(),
                )
            })?);
        }

        Ok(Some((
            Self {
                source_segments: staged.segments,
                unique,
                provenance,
                source_operands: staged.source_operands,
                source_rings: staged.source_rings,
                ring_kinds: staged.ring_kinds,
                ring_polygons: staged.ring_polygons,
                ring_holes: staged.ring_holes,
                polygon_count: staged.polygon_count,
                polygon_operands: staged.polygon_operands,
                polygon_ring_ranges: staged.polygon_ring_ranges,
                source_points: staged.source_points,
                polygon_rings: staged.polygon_rings,
                ring_count: staged.ring_count,
                isolated: staged.isolated,
                isolated_points: staged.isolated_points,
                isolated_operands: staged.isolated_operands,
                line_boundaries: staged.line_boundaries,
            },
            intervals,
        )))
    }

    pub(super) fn from_non_areal_rect_clip(
        shape: &Shape,
        bounds: Bounds,
    ) -> Result<(Self, Vec<SegmentClipInterval>)> {
        let mut inventory = Self::from_shapes(&[shape])?;
        debug_assert_eq!(inventory.polygon_count, 0);
        let mut unique = Vec::with_capacity(inventory.unique.len());
        let mut intervals = Vec::with_capacity(inventory.unique.len());
        let mut offsets = Vec::with_capacity(inventory.unique.len().saturating_add(1));
        let mut entries = Vec::new();
        offsets.push(0);
        for (&segment, owners) in inventory.unique.iter().zip(inventory.provenance.iter()) {
            if let Some(interval) = clip_segment_rect_interval(segment, bounds)? {
                unique.push(segment);
                intervals.push(interval);
                entries.extend_from_slice(owners);
                offsets
                    .push(u32::try_from(entries.len()).map_err(|_| ResourceError::SizeOverflow)?);
            }
        }
        inventory.unique = unique;
        inventory.provenance = SegmentProvenance::from_csr(offsets, entries);
        let mut retained = 0_usize;
        #[expect(
            clippy::indexing_slicing,
            reason = "staging admits the parallel point columns together; compaction writes at or before the current source row"
        )]
        for row in 0..inventory.isolated.len() {
            if bounds.contains_point(inventory.isolated[row]) {
                inventory.isolated[retained] = inventory.isolated[row];
                inventory.isolated_points[retained] = inventory.isolated_points[row];
                inventory.isolated_operands[retained] = inventory.isolated_operands[row];
                retained = retained.wrapping_add(1);
            }
        }
        inventory.isolated.truncate(retained);
        inventory.isolated_points.truncate(retained);
        inventory.isolated_operands.truncate(retained);
        Ok((inventory, intervals))
    }

    pub(in crate::geometry) fn from_segments(segments: &[Segment]) -> Result<Self> {
        let mut staged = StagedExactSources::new(1);
        staged.reserve_segments(segments.len());
        for &segment in segments {
            staged.push_segment(
                segment,
                &[segment.start.point(), segment.end.point()],
                0,
                None,
            );
        }
        Self::from_staged(staged)
    }

    pub(in crate::geometry) fn from_raw_loops(loops: &[(Vec<f64>, Vec<f64>)]) -> Result<Self> {
        let mut staged = StagedExactSources::new(1);
        for (xs, ys) in loops {
            let ring_id =
                u32::try_from(staged.ring_count).map_err(|_| ResourceError::SizeOverflow)?;
            staged.ring_count = staged
                .ring_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            // Raw winding callers pass open coordinate columns for a cyclic
            // loop; the final-to-first edge is part of that source contract.
            // This ingress is not the open-line inventory used by overlay.
            let closed = xs.len() >= 3;
            let coordinate_pairs = || xs.iter().copied().zip(ys.iter().copied());
            let mut endpoints = coordinate_pairs();
            let first = endpoints.next().map(|(x, y)| XY::new(x, y));
            let last = endpoints.next_back().map(|(x, y)| XY::new(x, y));
            let repeated_closure = closed
                && first
                    .zip(last)
                    .is_some_and(|(first, last)| same_point(first, last));
            let count = xs.len().saturating_sub(usize::from(repeated_closure));
            staged.ring_kinds.push(RingKind::Winding);
            staged.reserve_segments(count);
            let points = || {
                coordinate_pairs().take(count).map(|(x, y)| {
                    let point = Point::new_raw_xy(x, y);
                    (point.xy(), point)
                })
            };
            if let Some(first) = points().next() {
                for ((start_xy, start), (end_xy, end)) in
                    points().zip(points().skip(1).chain(std::iter::once(first)))
                {
                    staged.push_segment(
                        Segment {
                            start: start_xy,
                            end: end_xy,
                        },
                        &[start, end],
                        0,
                        closed.then_some(ring_id),
                    );
                }
            }
        }
        Self::from_staged(staged)
    }

    pub(in crate::geometry) fn source_segments(&self) -> &[Segment] {
        &self.source_segments
    }

    pub(in crate::geometry) fn source_operands(&self) -> &[u32] {
        &self.source_operands
    }
}

impl StagedExactSources {
    pub(super) fn new(operand_count: usize) -> Self {
        Self::with_xy_only(operand_count, false)
    }

    pub(super) fn with_xy_only(operand_count: usize, xy_only: bool) -> Self {
        let mut polygon_rings = Vec::with_capacity(operand_count);
        polygon_rings.resize_with(operand_count, Vec::new);
        Self {
            segments: Vec::new(),
            source_points: None,
            xy_only,
            source_operands: Vec::new(),
            source_rings: Vec::new(),
            ring_kinds: Vec::new(),
            ring_polygons: Vec::new(),
            ring_holes: Vec::new(),
            polygon_count: 0,
            polygon_operands: Vec::new(),
            polygon_ring_ranges: Vec::new(),
            polygon_rings,
            ring_count: 0,
            isolated: Vec::new(),
            isolated_points: Vec::new(),
            isolated_operands: Vec::new(),
            line_boundaries: Vec::new(),
            relation_components: None,
        }
    }

    pub(super) fn from_rows<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
    ) -> Result<Self> {
        Self::stage_rows(
            rows,
            false,
            |_| {},
            |staged, row, operand| {
                let mut owners = SharedShapeOwners::for_collections();
                staged.append_row(row, operand, &mut owners)
            },
        )
    }

    /// Stage every source occurrence. XY-only consumers can skip retaining
    /// Z/M endpoint columns while keeping the same source walk and XY data.
    pub(in crate::geometry) fn from_rows_with_occurrences<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
        xy_only: bool,
    ) -> Result<Self> {
        Self::stage_rows(rows, xy_only, |_| {}, Self::append_occurrence_row)
    }

    /// Relation staging keeps each component boundary without partition
    /// deduplication, including closed lines with no endpoint occurrences.
    pub(in crate::geometry) fn from_rows_for_relation<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
    ) -> Result<Self> {
        Self::stage_rows(
            rows,
            true,
            |staged| staged.relation_components = Some(Box::default()),
            Self::append_occurrence_row,
        )
    }

    pub(super) fn append_occurrence_row(
        staged: &mut Self,
        row: storage::ShapeRow<'_>,
        operand: u32,
    ) -> Result<()> {
        match row {
            storage::ShapeRow::Shape(shape) => staged.append_shape_with_occurrences(shape, operand),
            storage::ShapeRow::Handle(plan) => {
                staged.append_shape_with_occurrences(plan.shape(), operand)
            },
            row @ (storage::ShapeRow::Point(_) | storage::ShapeRow::Packed(_)) => {
                staged.append_row(row, operand, &mut SharedShapeOwners::for_collections())
            },
        }
    }

    pub(super) fn stage_rows<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
        xy_only: bool,
        initialize: impl FnOnce(&mut Self),
        mut append: impl FnMut(&mut Self, storage::ShapeRow<'a>, u32) -> Result<()>,
    ) -> Result<Self> {
        let mut staged = Self::with_xy_only(rows.len(), xy_only);
        initialize(&mut staged);
        for (operand, row) in rows.enumerate() {
            let operand = u32::try_from(operand).map_err(|_| ResourceError::SizeOverflow)?;
            append(&mut staged, row, operand)?;
        }
        Ok(staged)
    }

    pub(in crate::geometry) fn segments(&self) -> &[Segment] {
        &self.segments
    }

    #[cfg(test)]
    pub(in crate::geometry) fn source_operands(&self) -> &[u32] {
        &self.source_operands
    }

    #[cfg(test)]
    pub(in crate::geometry) fn source_rings(&self) -> &[Option<u32>] {
        &self.source_rings
    }

    pub(in crate::geometry) fn ring_kinds(&self) -> &[RingKind] {
        &self.ring_kinds
    }

    #[cfg(test)]
    pub(in crate::geometry) fn ring_polygons(&self) -> &[u32] {
        &self.ring_polygons
    }

    #[cfg(test)]
    pub(in crate::geometry) fn ring_holes(&self) -> &[bool] {
        &self.ring_holes
    }

    #[cfg(test)]
    pub(in crate::geometry) fn polygon_operands(&self) -> &[u32] {
        &self.polygon_operands
    }

    #[cfg(test)]
    pub(in crate::geometry) fn polygon_ring_ranges(&self) -> &[Range<usize>] {
        &self.polygon_ring_ranges
    }

    #[cfg(test)]
    pub(in crate::geometry) fn polygon_rings(&self) -> &[Vec<Range<usize>>] {
        &self.polygon_rings
    }

    pub(in crate::geometry) fn isolated(&self) -> &[XY] {
        &self.isolated
    }

    #[cfg(test)]
    pub(in crate::geometry) fn isolated_operands(&self) -> &[u32] {
        &self.isolated_operands
    }

    #[cfg(test)]
    pub(in crate::geometry) fn line_boundaries(&self) -> &[(u32, PointKey)] {
        &self.line_boundaries
    }

    pub(in crate::geometry) fn relation_components(&self) -> Option<&[StagedSourceComponent]> {
        self.relation_components
            .as_deref()
            .map(|layout| layout.components.as_slice())
    }

    pub(in crate::geometry) fn relation_source_order(&self) -> Option<&[StagedRelationItem]> {
        self.relation_components
            .as_deref()
            .map(|layout| layout.source_order.as_slice())
    }

    pub(super) fn append_row(
        &mut self,
        row: storage::ShapeRow<'_>,
        operand: u32,
        owners: &mut SharedShapeOwners,
    ) -> Result<()> {
        use crate::geometry::storage::{PackedRow, ShapeRow};
        match row {
            ShapeRow::Handle(plan) => self.append_shape(plan.shape(), operand, owners),
            ShapeRow::Shape(shape) => self.append_shape(shape, operand, owners),
            ShapeRow::Point(point) => {
                self.append_isolated_point(point, operand);
                Ok(())
            },
            ShapeRow::Packed(PackedRow::Points {
                coords, physical, ..
            }) => {
                self.append_isolated_point(coords.point_at(physical), operand);
                Ok(())
            },
            ShapeRow::Packed(row @ PackedRow::Lines { coords, .. }) => {
                let coords = crate::geometry::storage::row::CoordRow::new(
                    coords,
                    row.vertex_window().expect("line row has a vertex window"),
                );
                self.append_line_coords(&coords, operand);
                Ok(())
            },
            ShapeRow::Packed(row @ PackedRow::Polygons { coords, .. }) => {
                let rings = row.polygon_rings().expect("polygon row has a ring window");
                if rings.is_empty() {
                    return Ok(());
                }
                self.append_polygon_rings(
                    rings.map(|ring| {
                        crate::geometry::storage::row::CoordRow::new(
                            coords,
                            row.polygon_ring_window(ring)
                                .expect("admitted polygon ring"),
                        )
                    }),
                    operand,
                )
            },
        }
    }

    pub(super) fn append_shape(
        &mut self,
        shape: &Shape,
        operand: u32,
        owners: &mut SharedShapeOwners,
    ) -> Result<()> {
        if let Shape::GeometryCollection(collection) = shape {
            if owners.first_collection(collection.as_slice(), collection.members_are_shared()) {
                for member in collection {
                    self.append_shape(member, operand, owners)?;
                }
            }
            return Ok(());
        }
        self.append_shape_with_occurrences(shape, operand)
    }

    pub(super) fn append_shape_with_occurrences(
        &mut self,
        shape: &Shape,
        operand: u32,
    ) -> Result<()> {
        shape.try_for_each_component(&mut |component| self.append_component(component, operand))
    }

    pub(super) fn append_component(
        &mut self,
        component: crate::geometry::Component<'_>,
        operand: u32,
    ) -> Result<()> {
        match component {
            crate::geometry::Component::Point(point) => {
                self.append_isolated_point(point, operand);
                Ok(())
            },
            crate::geometry::Component::LineString(line) => {
                self.append_line_coords(line.as_coords(), operand);
                Ok(())
            },
            crate::geometry::Component::Polygon(polygon) => self.append_polygon(polygon, operand),
        }
    }

    pub(super) fn append_occupied_area_shape(
        &mut self,
        shape: &Shape,
        operand: u32,
        collection_owners: &mut SharedShapeOwners,
    ) -> Result<()> {
        match shape {
            Shape::Point(..)
            | Shape::MultiPoint(..)
            | Shape::LineString(..)
            | Shape::MultiLineString(..)
            | Shape::Empty(..) => {},
            Shape::Polygon(polygon) => self.append_polygon(polygon, operand)?,
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    self.append_polygon(polygon, operand)?;
                }
            },
            Shape::GeometryCollection(collection) => {
                let members = collection.as_slice();
                if !collection_owners.first_collection(members, collection.members_are_shared()) {
                    return Ok(());
                }
                if !members.iter().any(|member| {
                    matches!(
                        member,
                        Shape::Polygon(..)
                            | Shape::MultiPolygon(..)
                            | Shape::GeometryCollection(..)
                    )
                }) {
                    return Ok(());
                }
                for member in members {
                    self.append_occupied_area_shape(member, operand, collection_owners)?;
                }
            },
        }
        Ok(())
    }

    /// One point with no segment to carry it: the three isolated columns move
    /// together, so they reserve together.
    pub(super) fn append_isolated_point(&mut self, point: Point, operand: u32) {
        let index = self.isolated.len();
        self.isolated.push(point.xy());
        self.isolated_points.push(point);
        self.isolated_operands.push(operand);
        if let Some(components) = self.relation_components.as_deref_mut() {
            components
                .source_order
                .push(StagedRelationItem::Isolated(index));
        }
    }

    pub(super) fn append_line_coords(
        &mut self,
        line: &(impl crate::geometry::Coordinates + ?Sized),
        operand: u32,
    ) {
        self.retain_source_ordinates(line);
        self.reserve_segments(line.coord_count().saturating_sub(1));
        let segment_start = self.segments.len();
        let mut first = None;
        let mut last = None;
        for [start, end] in line.segment_pairs() {
            let (start_xy, end_xy) = (start.xy(), end.xy());
            if !same_point(start_xy, end_xy) {
                first.get_or_insert(start_xy);
                last = Some(end_xy);
            }
            self.push_segment(
                Segment {
                    start: start_xy,
                    end: end_xy,
                },
                &[start, end],
                operand,
                None,
            );
        }
        if let Some(components) = self.relation_components.as_deref_mut() {
            let component = components.components.len();
            let has_nonzero_segment = first.is_some();
            components.components.push(StagedSourceComponent {
                operand,
                segments: segment_start..self.segments.len(),
                ring: None,
                ring_kind: None,
                has_nonzero_segment,
            });
            if has_nonzero_segment {
                components
                    .source_order
                    .push(StagedRelationItem::Component(component));
            }
        }
        if first.is_none() {
            self.isolated.reserve(line.coord_count());
            self.isolated_points.reserve(line.coord_count());
            self.isolated_operands.reserve(line.coord_count());
            for point in line.iter_coords() {
                self.append_isolated_point(point, operand);
            }
            return;
        }
        if let (Some(first), Some(last)) = (first, last)
            && !same_point(first, last)
        {
            self.line_boundaries.reserve(2);
            self.line_boundaries.push((operand, PointKey::new(first)));
            self.line_boundaries.push((operand, PointKey::new(last)));
        }
    }

    pub(super) fn append_polygon(&mut self, polygon: &Polygon, operand: u32) -> Result<()> {
        self.append_polygon_rings(polygon.rings(), operand)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "operand IDs are admitted by the source inventory's operand count"
    )]
    pub(super) fn append_polygon_rings<C: crate::geometry::Coordinates>(
        &mut self,
        rings: impl IntoIterator<Item = C>,
        operand: u32,
    ) -> Result<()> {
        let first_ring = self.ring_count;
        let polygon_id =
            u32::try_from(self.polygon_count).map_err(|_| ResourceError::SizeOverflow)?;
        self.polygon_count = self
            .polygon_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        self.polygon_operands.push(operand);
        for (ring_index, ring) in rings.into_iter().enumerate() {
            let ring_id =
                u32::try_from(self.ring_count).map_err(|_| ResourceError::SizeOverflow)?;
            self.ring_count = self
                .ring_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            let kind = if crate::geometry::ring_is_collinear(&ring) {
                RingKind::Collapsed
            } else {
                RingKind::Winding
            };
            let segment_start = self.segments.len();
            self.ring_polygons.push(polygon_id);
            self.ring_holes.push(ring_index > 0);
            self.ring_kinds.push(kind);
            let count = ring.coord_count().saturating_sub(1);
            self.retain_source_ordinates(&ring);
            self.reserve_segments(count);
            let mut pairs = ring.segment_pairs();
            let [first, first_end] = pairs.next().expect("a Ring has at least four coordinates");
            let mut pairs = std::iter::once([first, first_end]).chain(pairs).peekable();
            while let Some([start, mut end]) = pairs.next() {
                if pairs.peek().is_none() {
                    end = first;
                }
                let segment = Segment {
                    start: start.xy(),
                    end: end.xy(),
                };
                self.push_segment(segment, &[start, end], operand, Some(ring_id));
            }
            if let Some(components) = self.relation_components.as_deref_mut() {
                let component = components.components.len();
                let has_nonzero_segment = kind == RingKind::Winding
                    || ring
                        .iter_coords()
                        .any(|point| !same_point(first.xy(), point.xy()));
                components.components.push(StagedSourceComponent {
                    operand,
                    segments: segment_start..self.segments.len(),
                    ring: Some(ring_id),
                    ring_kind: Some(kind),
                    has_nonzero_segment,
                });
                components
                    .source_order
                    .push(StagedRelationItem::Component(component));
            }
        }
        self.polygon_rings[operand as usize].push(first_ring..self.ring_count);
        self.polygon_ring_ranges.push(first_ring..self.ring_count);
        Ok(())
    }

    pub(super) fn reserve_segments(&mut self, count: usize) {
        self.segments.reserve(count);
        if let Some(points) = &mut self.source_points {
            points.reserve(count);
        }
        self.source_operands.reserve(count);
        self.source_rings.reserve(count);
    }

    pub(super) fn retain_source_ordinates(
        &mut self,
        coordinates: &(impl crate::geometry::Coordinates + ?Sized),
    ) {
        if !self.xy_only
            && self.source_points.is_none()
            && (coordinates.z_column().is_some() || coordinates.m_column().is_some())
        {
            // Earlier XY sources have no ordinates, so their stored carriers
            // recover the prefix when a later source first supplies Z/M.
            let mut points = Vec::with_capacity(self.segments.capacity());
            points.extend(
                self.segments.iter().map(|segment| {
                    SourceArcRecord::new([segment.start.point(), segment.end.point()])
                }),
            );
            self.source_points = Some(points);
        }
    }

    pub(super) fn push_segment(
        &mut self,
        segment: Segment,
        source_points: &[Point; 2],
        operand: u32,
        ring: Option<u32>,
    ) {
        if let Some(points) = &mut self.source_points {
            points.push(SourceArcRecord::new(*source_points));
        }
        self.segments.push(segment);
        self.source_operands.push(operand);
        self.source_rings.push(ring);
    }
}
