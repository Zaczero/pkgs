use super::ChartEdge;
use crate::error::Result;
use crate::geographic::retained_exact::edges::{
    retained_source_interval_initial_piece_count, retained_source_interval_reads_as,
};
use crate::geographic::retained_exact::sites::retained_source_sheet_site;
use crate::geometry::segments::projection::segment_projection;
use crate::geometry::{GeometryErrorKind, Point, Segment, SegmentProjection, XY};

impl ChartEdge {
    /// Intended position on the chart edge before canonical point emission.
    pub(crate) fn chart_position(&self, t: f64) -> Result<crate::geometry::Point> {
        let axis = self.frame.longitude();
        let longitude = self.retained_longitude();
        let site: crate::geographic::RetainedLongitudeSite = axis.native_site_at(&longitude, t)?;
        let longitude = axis.lifted_site_longitude(site)?;
        let latitude = crate::geometry::interpolate_subdivision_ordinate(
            self.start_latitude,
            self.end_latitude,
            t,
        );
        Ok(crate::geometry::Point::new_raw_xy(longitude, latitude))
    }

    /// Publish one point in the original source sheet while keeping the
    /// relative retained branch authoritative for the metric curve.
    #[expect(
        clippy::float_cmp,
        reason = "exact endpoints and constant longitude preserve authentic source spellings"
    )]
    pub(crate) fn source_sheet_point(
        &self,
        source_start: Point,
        source_end: Point,
        parameter: f64,
        canonical_point: Point,
    ) -> Result<Point> {
        if parameter == 0.0 {
            return Ok(source_start);
        }
        if parameter == 1.0 {
            return Ok(source_end);
        }
        let retained = self.retained_longitude();
        let longitude = if source_start.x == source_end.x && retained.periods() == 0 {
            source_start.x
        } else {
            retained_source_sheet_site(source_start.x, retained, self.frame.longitude(), parameter)?
        };
        Ok(canonical_point.with_xy_raw(longitude, canonical_point.y))
    }

    pub(crate) fn source_interval_reads_correctly(
        &self,
        from_parameter: f64,
        to_parameter: f64,
        start: Point,
        end: Point,
    ) -> Result<bool> {
        let readback = self
            .frame
            .longitude()
            .chart_step(start.x, end.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        retained_source_interval_reads_as(
            self.retained_longitude(),
            self.frame,
            [self.start_latitude, self.end_latitude],
            from_parameter,
            to_parameter,
            readback,
            start,
            end,
        )
    }

    pub(crate) fn source_interval_initial_piece_count(&self, from: f64, to: f64) -> Result<usize> {
        retained_source_interval_initial_piece_count(
            self.retained_longitude(),
            self.frame.longitude(),
            from,
            to,
        )
    }

    /// Publish and verify the source-sheet chain over one retained parameter
    /// interval. The exact edge branch and parameter endpoints remain the
    /// certificate while output is subdivided until each stored edge reads
    /// along that branch.
    #[expect(
        clippy::float_cmp,
        reason = "source parameters identify exact binary64 endpoints and splits"
    )]
    pub(crate) fn publish_source_interval(
        &self,
        source_start: Point,
        source_end: Point,
        from: f64,
        to: f64,
        mut canonical_at: impl FnMut(f64) -> Result<Point>,
        mut emit: impl FnMut(f64, Point, bool) -> Result<()>,
        operation: &'static str,
    ) -> Result<()> {
        if !(from.is_finite()
            && to.is_finite()
            && (0.0..=1.0).contains(&from)
            && (0.0..=1.0).contains(&to)
            && from <= to)
        {
            return Err(GeometryErrorKind::unrepresentable_output(operation));
        }

        let mut point_at = |parameter: f64| {
            if parameter == 0.0 {
                Ok(source_start)
            } else if parameter == 1.0 {
                Ok(source_end)
            } else {
                self.source_sheet_point(
                    source_start,
                    source_end,
                    parameter,
                    canonical_at(parameter)?,
                )
            }
        };

        let mut previous_parameter = from;
        let mut previous = point_at(from)?;
        emit(from, previous, false)?;
        let end = point_at(to)?;
        if self.source_interval_reads_correctly(from, to, previous, end)? {
            return emit(to, end, false);
        }
        if previous.x == end.x {
            let axis = self.frame.longitude();
            let retained = self.retained_longitude();
            let start_site = retained_source_sheet_site(source_start.x, retained, axis, from)?;
            let end_site = retained_source_sheet_site(source_start.x, retained, axis, to)?;
            // RN is monotone over the exact affine source longitude. When both
            // written endpoints are those rounded sites and are identical, no
            // interior parameter can produce a readable nonzero longitude.
            if start_site.to_bits() == previous.x.to_bits() && end_site.to_bits() == end.x.to_bits()
            {
                return Err(GeometryErrorKind::unrepresentable_output(operation));
            }
        }

        let pieces = self.source_interval_initial_piece_count(from, to)?;
        for step in 1..=pieces {
            let parameter = if step == pieces {
                to
            } else {
                crate::geometry::interpolate_subdivision_ordinate(
                    from,
                    to,
                    step as f64 / pieces as f64,
                )
            };
            if parameter <= previous_parameter || (step < pieces && parameter >= to) {
                return Err(GeometryErrorKind::unrepresentable_output(operation));
            }
            let point = if parameter == to {
                end
            } else {
                point_at(parameter)?
            };
            self.publish_source_piece(
                previous_parameter,
                parameter,
                previous,
                point,
                to,
                &mut point_at,
                &mut emit,
                operation,
            )?;
            previous_parameter = parameter;
            previous = point;
        }
        Ok(())
    }

    #[expect(
        clippy::float_cmp,
        reason = "source parameters identify exact binary64 progress and longitude collapse"
    )]
    fn publish_source_piece(
        &self,
        from: f64,
        to: f64,
        start: Point,
        end: Point,
        planned_end: f64,
        point_at: &mut impl FnMut(f64) -> Result<Point>,
        emit: &mut impl FnMut(f64, Point, bool) -> Result<()>,
        operation: &'static str,
    ) -> Result<()> {
        if self.source_interval_reads_correctly(from, to, start, end)? {
            return emit(to, end, to != planned_end);
        }
        if from > 0.0 && to < 1.0 && start.x == end.x {
            return Err(GeometryErrorKind::unrepresentable_output(operation));
        }
        let mut pending = vec![(from, to, start, end)];
        while let Some((from, to, start, end)) = pending.pop() {
            if self.source_interval_reads_correctly(from, to, start, end)? {
                emit(to, end, to != planned_end)?;
                continue;
            }
            let middle = f64::midpoint(from, to);
            if middle == from || middle == to {
                return Err(GeometryErrorKind::unrepresentable_output(operation));
            }
            let middle_point = point_at(middle)?;
            pending.push((middle, to, middle_point, end));
            pending.push((from, middle, start, middle_point));
        }
        Ok(())
    }

    /// Coordinate-plane projection proposal expressed in this edge's lifted
    /// chart. The minimizer still evaluates its geodesic objective before the
    /// parameter can affect the attained result.
    pub(crate) fn projection_parameter(self, point: Point) -> Result<f64> {
        Ok(self.project_source_chart(point)?.0.fraction())
    }

    /// Project through the retained source branch, returning the local query
    /// and segment for callers that must test source membership as well.
    /// Coordinates in that local chart are rounded `f64`s, so an exact contact
    /// predicate certifies only this represented segment, not exact membership
    /// in the original source geometry.
    pub(crate) fn project_source_chart(
        self,
        point: Point,
    ) -> Result<(SegmentProjection, XY, Segment)> {
        let axis = self.frame.longitude();
        // The start alias folds a full-turn edge onto one endpoint. The stored
        // chart-step delta makes the midpoint the reference for its full lifted span.
        let longitude = self.retained_longitude();
        let midpoint: crate::geographic::RetainedLongitudeSite = axis
            .native_site_at(&longitude, 0.5)
            .map_err(|error| crate::crs::CrsError::message(error.to_string()))?;
        let midpoint_longitude = axis
            .lifted_site_longitude(midpoint)
            .map_err(|error| crate::crs::CrsError::message(error.to_string()))?;
        let lifted_longitude = axis
            .nearest_longitude_alias(midpoint_longitude, point.x)
            .map_err(|error| crate::crs::CrsError::message(error.to_string()))?;
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(self.longitude_delta_native, self.latitude_delta),
        };
        let start_longitude = self.longitude.endpoints()[0].get();
        let query = XY::new(
            lifted_longitude - start_longitude,
            point.y - self.start_latitude,
        );
        Ok((segment_projection(query, segment)?, query, segment))
    }
}
