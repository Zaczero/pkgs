use super::{
    ChartPieceId, ExactChartPiece, ExactChartPoint, ExactGeographicChartInventory,
    ExactLineBoundary, ExactSourceEdge, ExactSourceLine, ExactSourcePoint, ExactSourcePolygon,
    ExactSourceRange, ExactSourceRing, SourceEdgeId, SourceEdgeKind, SourceLineId, SourcePolygonId,
    SourceRingId, SourceRowId, local_longitude_edge,
};
use crate::error::{ResourceError, Result};
#[cfg(test)]
use crate::geographic::GeographicInputs;
use crate::geographic::{GeographicFrame, Latitude, RingDomain, RingSummary, ScalarGeographicView};
#[cfg(test)]
use crate::geometry::Unmetered;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    Component, Coordinates as _, GeometryErrorKind, Point, Polygon, RingKind, Shape, Work,
};

impl ExactGeographicChartInventory {
    #[cfg(test)]
    pub(in crate::geometry::topology) fn collect(inputs: &GeographicInputs<'_>) -> Result<Self> {
        Self::collect_views(inputs.frame(), inputs.views().iter().copied())
    }

    #[cfg(test)]
    pub(in crate::geometry::topology) fn collect_views<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = ScalarGeographicView<'a>>,
    ) -> Result<Self> {
        let mut work = Unmetered;
        Self::collect_views_with_work(frame, views, &mut work)
    }

    pub(in crate::geometry::topology) fn collect_views_with_work<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = ScalarGeographicView<'a>>,
        work: &mut impl Work,
    ) -> Result<Self> {
        Self::collect_views_inner(frame, views, false, work)
    }

    pub(in crate::geometry::topology) fn collect_views_as_set_with_work<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = ScalarGeographicView<'a>>,
        work: &mut impl Work,
    ) -> Result<Self> {
        Self::collect_views_inner(frame, views, true, work)
    }

    fn collect_views_inner<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = ScalarGeographicView<'a>>,
        as_set: bool,
        work: &mut impl Work,
    ) -> Result<Self> {
        work.charge_into_error(
            u64::try_from(views.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut inventory = Self::new(frame, views.len());
        for (row, view) in views.enumerate() {
            work.charge_into_error(1)?;
            if view.frame() != frame {
                return Err(GeometryErrorKind::Invalid(
                    "exact geographic source views do not share one frame".into(),
                )
                .into());
            }
            let row = SourceRowId(u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?);
            if as_set {
                inventory.append_shape_as_set(view.shape(), row, work)?;
            } else {
                inventory.append_shape(view.shape(), row, work)?;
            }
        }
        Ok(inventory)
    }

    pub(super) const fn new(frame: GeographicFrame, row_count: usize) -> Self {
        Self {
            frame,
            row_count,
            edges: Vec::new(),
            pieces: Vec::new(),
            lines: Vec::new(),
            rings: Vec::new(),
            polygons: Vec::new(),
            points: Vec::new(),
            line_boundaries: Vec::new(),
        }
    }

    fn append_shape(
        &mut self,
        shape: &Shape,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<()> {
        shape.try_for_each_component(&mut |source| {
            work.charge_into_error(1)?;
            self.append_component(source, row, work)
        })
    }

    fn append_shape_as_set(
        &mut self,
        shape: &Shape,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<()> {
        shape.try_for_each_first_component(&mut |source| {
            work.charge_into_error(1)?;
            self.append_component(source, row, work)
        })
    }

    fn append_component(
        &mut self,
        source: Component<'_>,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<()> {
        match source {
            Component::Point(point) => {
                work.charge_into_error(1)?;
                self.points.push(ExactSourcePoint { point, row });
                Ok(())
            },
            Component::LineString(line) => self.push_line(line.as_coords(), row, work),
            Component::Polygon(polygon) => self.push_polygon_with_work(polygon, row, work),
        }
    }

    fn push_line(
        &mut self,
        coordinates: &crate::geometry::CoordSeq,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<()> {
        let segment_count = coordinates.coord_count().saturating_sub(1);
        work.charge_into_error(
            u64::try_from(segment_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.reserve_source_edges(segment_count, work)?;
        let line =
            SourceLineId(u32::try_from(self.lines.len()).map_err(|_| ResourceError::SizeOverflow)?);
        let edge_start = self.edges.len();
        let piece_start = self.pieces.len();
        let mut first_edge = None;
        let mut last_edge = None;
        for pair in coordinates.segment_pairs() {
            work.charge_into_error(1)?;
            let [start, end] = pair;
            let before = self.pieces.len();
            let (source, _) = self.append_source_edge_with_work(
                start,
                end,
                row,
                SourceEdgeKind::Line { line },
                0,
                work,
            )?;
            if self.pieces.len() != before {
                first_edge.get_or_insert(source);
                last_edge = Some(source);
            }
        }
        if self.pieces.len() == piece_start {
            work.charge_into_error(
                u64::try_from(coordinates.coord_count())
                    .map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            self.points.reserve(coordinates.coord_count());
            for point in coordinates.iter_coords() {
                self.points.push(ExactSourcePoint { point, row });
            }
        } else if let (Some(first), Some(last)) = (first_edge, last_edge) {
            work.charge_into_error(2)?;
            self.line_boundaries.reserve(2);
            self.line_boundaries.push(ExactLineBoundary {
                row,
                line,
                edge: first,
                at_end: false,
            });
            self.line_boundaries.push(ExactLineBoundary {
                row,
                line,
                edge: last,
                at_end: true,
            });
        }
        work.charge_into_error(1)?;
        self.lines.push(ExactSourceLine {
            id: line,
            row,
            source_edges: edge_start..self.edges.len(),
        });
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn push_polygon(&mut self, polygon: &Polygon, row: SourceRowId) -> Result<()> {
        let mut work = Unmetered;
        self.push_polygon_with_work(polygon, row, &mut work)
    }

    fn push_polygon_with_work(
        &mut self,
        polygon: &Polygon,
        row: SourceRowId,
        work: &mut impl Work,
    ) -> Result<()> {
        let polygon_id = SourcePolygonId(
            u32::try_from(self.polygons.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        let ring_count = polygon
            .holes
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(
            u64::try_from(ring_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let edge_count = polygon.rings().try_fold(0_usize, |count, ring| {
            count
                .checked_add(ring.coord_count().saturating_sub(1))
                .ok_or(ResourceError::SizeOverflow)
        })?;
        work.charge_into_error(
            u64::try_from(edge_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.reserve_source_edges(edge_count, work)?;
        work.charge_into_error(1)?;
        self.polygons.reserve(1);
        self.rings.reserve(polygon.holes.len().saturating_add(1));
        let first_ring = self.rings.len();
        self.polygons.push(ExactSourcePolygon {
            row,
            rings: first_ring..first_ring,
        });
        for (ring_index, ring) in polygon.rings().enumerate() {
            work.charge_into_error(1)?;
            work.charge_into_error(
                u64::try_from(ring.coord_count()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let ring_id = SourceRingId(
                u32::try_from(self.rings.len()).map_err(|_| ResourceError::SizeOverflow)?,
            );
            let stored_kind =
                crate::geometry::ring_kind_columns(ring.coords().xs(), ring.coords().ys());
            let summary = RingSummary::try_new(
                self.frame,
                &crate::geographic::GeographicRing::from_ring(ring),
            )
            .expect("the source ring was admitted in this frame");
            let kind = if summary.domain == RingDomain::SeamOnly {
                RingKind::Collapsed
            } else {
                stored_kind
            };
            let first_edge = self.edges.len();
            let mut lift = 0_i64;
            for [start, end] in ring.segment_pairs() {
                work.charge_into_error(1)?;
                let (_, transit) = self.append_source_edge_with_work(
                    start,
                    end,
                    row,
                    SourceEdgeKind::Ring {
                        polygon: polygon_id,
                        ring: ring_id,
                        hole: ring_index != 0,
                        kind,
                    },
                    lift,
                    work,
                )?;
                lift = lift
                    .checked_add(i64::from(transit))
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            debug_assert_eq!(lift, summary.chart_turn);
            self.rings.push(ExactSourceRing {
                id: ring_id,
                polygon: polygon_id,
                hole: ring_index != 0,
                kind,
                summary,
                source_edges: first_edge..self.edges.len(),
            });
        }
        let target = self
            .polygons
            .get_mut(polygon_id.0 as usize)
            .expect("the polygon record was reserved and appended");
        target.rings = first_ring..self.rings.len();
        Ok(())
    }

    fn reserve_source_edges(&mut self, count: usize, work: &mut impl Work) -> Result<()> {
        let edge_total = self
            .edges
            .len()
            .checked_add(count)
            .ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(edge_total).map_err(|_| ResourceError::SizeOverflow)?;
        let piece_capacity = count
            .checked_mul(2)
            .and_then(|additional| self.pieces.len().checked_add(additional))
            .ok_or(ResourceError::SizeOverflow)?;
        let additional_pieces = piece_capacity
            .checked_sub(self.pieces.len())
            .expect("piece capacity includes the existing chart pieces");
        let reservation_work = count
            .checked_add(additional_pieces)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge_into_error(
            u64::try_from(reservation_work).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        self.edges.reserve(count);
        self.pieces.reserve(additional_pieces);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn append_source_edge(
        &mut self,
        start: Point,
        end: Point,
        row: SourceRowId,
        kind: SourceEdgeKind,
        ring_lift: i64,
    ) -> Result<(SourceEdgeId, i8)> {
        let mut work = Unmetered;
        self.append_source_edge_with_work(start, end, row, kind, ring_lift, &mut work)
    }

    fn append_source_edge_with_work(
        &mut self,
        start: Point,
        end: Point,
        row: SourceRowId,
        kind: SourceEdgeKind,
        ring_lift: i64,
        work: &mut impl Work,
    ) -> Result<(SourceEdgeId, i8)> {
        work.charge_into_error(1)?;
        let id =
            SourceEdgeId(u32::try_from(self.edges.len()).map_err(|_| ResourceError::SizeOverflow)?);
        let connection = self
            .frame
            .longitude()
            .chart_connection(start.x, end.x)
            .expect("source longitude was admitted");
        self.edges.push(ExactSourceEdge {
            id,
            source: [start, end],
            longitude: connection.retained(),
            row,
            kind,
        });
        self.push_chart_pieces_with_work(id, ring_lift, work)?;
        Ok((id, connection.seam_transit()))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "SourceEdgeId is issued by append_source_edge for this inventory's edge vector"
    )]
    fn push_chart_pieces_with_work(
        &mut self,
        source: SourceEdgeId,
        ring_lift: i64,
        work: &mut impl Work,
    ) -> Result<()> {
        work.charge_into_error(1)?;
        let edge = self.edges[source.0 as usize];
        let [start, end] = edge.source;
        let local_edge = local_longitude_edge(self.frame, start.x, edge.retained_longitude());
        let start_x = local_edge.start_x;
        let delta_x = local_edge.delta_x(self.frame.longitude().period())?;
        let start_alias = local_edge.start_turn_adjust;
        let piece_lift = ring_lift
            .checked_sub(start_alias)
            .ok_or(ResourceError::SizeOverflow)?;
        let start_y = ExactDyadic::from_f64(Latitude::from_admitted(self.frame, start.y).get());
        let end_y = ExactDyadic::from_f64(Latitude::from_admitted(self.frame, end.y).get());
        let start_y_rational = ExactRational::from_dyadic(start_y.clone());
        let delta_y = ExactRational::from_dyadic(end_y.clone()).try_subtract(&start_y_rational)?;
        let start_x = ExactDyadic::from_f64(start_x);
        let end_x = start_x.clone().try_add(delta_x.clone())?;
        let half = ExactDyadic::from_f64(self.frame.longitude().half_period());
        let negative_half = half.clone().neg();
        let zero = ExactRational::zero();
        let one = ExactRational::one();

        if end_x.cmp(&half).is_gt() {
            let parameter =
                ExactRational::new(ExactDyadic::try_subtract_refs(&half, &start_x)?, delta_x)?;
            let seam_y = interpolate_rational(&start_y_rational, &delta_y, &parameter)?;
            let seam =
                ExactChartPoint::from_rationals(ExactRational::from_dyadic(half), seam_y.clone());
            self.push_chart_piece_with_work(
                source,
                ExactChartPoint::from_dyadics(start_x, start_y),
                seam,
                zero,
                parameter.clone(),
                piece_lift,
                work,
            )?;
            let end_x =
                end_x.try_subtract(ExactDyadic::from_f64(self.frame.longitude().period()))?;
            self.push_chart_piece_with_work(
                source,
                ExactChartPoint::from_rationals(ExactRational::from_dyadic(negative_half), seam_y),
                ExactChartPoint::from_dyadics(end_x, end_y),
                parameter,
                one,
                piece_lift
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?,
                work,
            )?;
        } else if end_x.cmp(&negative_half).is_lt() {
            let parameter = ExactRational::new(
                ExactDyadic::try_subtract_refs(&negative_half, &start_x)?,
                delta_x,
            )?;
            let seam_y = interpolate_rational(&start_y_rational, &delta_y, &parameter)?;
            let seam = ExactChartPoint::from_rationals(
                ExactRational::from_dyadic(negative_half),
                seam_y.clone(),
            );
            self.push_chart_piece_with_work(
                source,
                ExactChartPoint::from_dyadics(start_x, start_y),
                seam,
                zero,
                parameter.clone(),
                piece_lift,
                work,
            )?;
            let end_x = end_x.try_add(ExactDyadic::from_f64(self.frame.longitude().period()))?;
            self.push_chart_piece_with_work(
                source,
                ExactChartPoint::from_rationals(ExactRational::from_dyadic(half), seam_y),
                ExactChartPoint::from_dyadics(end_x, end_y),
                parameter,
                one,
                piece_lift
                    .checked_sub(1)
                    .ok_or(ResourceError::SizeOverflow)?,
                work,
            )?;
        } else {
            self.push_chart_piece_with_work(
                source,
                ExactChartPoint::from_dyadics(start_x, start_y),
                ExactChartPoint::from_dyadics(end_x, end_y),
                zero,
                one,
                piece_lift,
                work,
            )?;
        }
        Ok(())
    }

    fn push_chart_piece_with_work(
        &mut self,
        source_edge: SourceEdgeId,
        start: ExactChartPoint,
        end: ExactChartPoint,
        source_start: ExactRational,
        source_end: ExactRational,
        lift: i64,
        work: &mut impl Work,
    ) -> Result<()> {
        work.charge_into_error(1)?;
        if start.same_position(&end)? {
            return Ok(());
        }
        let id = ChartPieceId(
            u32::try_from(self.pieces.len()).map_err(|_| ResourceError::SizeOverflow)?,
        );
        self.pieces.push(ExactChartPiece {
            id,
            source_edge,
            start,
            end,
            source_range: ExactSourceRange {
                start: source_start,
                end: source_end,
            },
            lift,
        });
        Ok(())
    }
}
fn interpolate_rational(
    start: &ExactRational,
    delta: &ExactRational,
    parameter: &ExactRational,
) -> Result<ExactRational> {
    start.try_add(&delta.try_product(parameter)?)
}
