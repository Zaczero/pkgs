use super::chart::Chart;
use super::output::{OutputColumns, push_interpolated};
use super::plan::{Route, segmentize_steps};
use super::{
    CoordSeq, ExpansionBudget, GENERATED_ITEM_LIMIT, GeographicFrame, GeometryErrorKind, Point,
    Result, Segment, SegmentPlacement, Target, XY, axis_pow2_scale,
    interpolate_subdivision_ordinate, point_distance, same_point, scaled_residual, subdivide,
};

/// Plan the ideal planar piece count without allowing an overflowing endpoint
/// difference to masquerade as an already-complete segment. The common path
/// keeps the established distance arithmetic; only a non-finite distance uses
/// a common power-of-two frame, so the endpoint residual and the limit are
/// compared in the same scaled units without forming an overflowing length.
#[cfg(test)]
pub(super) fn planar_segmentize_steps(segment: Segment, max_segment_length: f64) -> usize {
    if !max_segment_length.is_finite() || max_segment_length <= 0.0 {
        return 1;
    }
    let length = point_distance(segment.start, segment.end);
    planar_segmentize_steps_from_length(segment, length, max_segment_length)
}

fn planar_segmentize_steps_from_length(
    segment: Segment,
    length: f64,
    max_segment_length: f64,
) -> usize {
    if !max_segment_length.is_finite() || max_segment_length <= 0.0 {
        return 1;
    }
    if length.is_finite() {
        return segmentize_steps(length, max_segment_length);
    }

    let maximum = segment
        .start
        .x
        .abs()
        .max(segment.start.y.abs())
        .max(segment.end.x.abs())
        .max(segment.end.y.abs());
    let scale = axis_pow2_scale(maximum);
    let dx = scaled_residual(segment.end.x, segment.start.x, scale);
    let dy = scaled_residual(segment.end.y, segment.start.y, scale);
    let scaled_limit = max_segment_length * scale;
    let ratio = if scaled_limit > 0.0 {
        dx.hypot(dy) / scaled_limit
    } else {
        f64::INFINITY
    };
    let pieces = ratio.ceil();
    // A huge quotient must not wrap to a small step count, which would admit
    // output beyond the shared 16M generated-work budget.
    if !pieces.is_finite() || pieces > (GENERATED_ITEM_LIMIT + 1) as f64 {
        // The sink charges inserted vertices (`pieces - 1`), so its sentinel
        // must remain one past that limit rather than merely one past pieces.
        GENERATED_ITEM_LIMIT + 2
    } else {
        pieces.max(1.0) as usize
    }
}

#[derive(Clone, Copy)]
pub(super) struct DirectedLength {
    bearing: f64,
    length: f64,
    longitude_delta_native: f64,
}

pub(super) struct Planar;

pub(super) const fn point_from_xy(point: XY) -> Point {
    Point::new_raw_xy(point.x, point.y)
}

/// Geodesic and rhumb paths are defined by their endpoints, so an explicit
/// full-turn chart edge spans no longitude here; only the chart route reads it
/// as a circle.
fn lifted_longitude_delta(frame: GeographicFrame, segment: Segment) -> f64 {
    let axis = frame.longitude();
    axis.shortest_alias_step(segment.start.x, segment.end.x)
        .expect("geographic subdivision received admitted coordinates")
        .delta(axis)
}

impl Route for Planar {
    type Solution = ();

    fn solve(&self, segment: Segment, target: Target<'_>) -> Result<(f64, Self::Solution)> {
        let length = match target {
            Target::MaxLength(max_length) if max_length.is_finite() && max_length > 0.0 => {
                point_distance(segment.start, segment.end)
            },
            Target::MaxLength(_) | Target::Fraction(_) | Target::Pieces(_) => 0.0,
        };
        Ok((length, ()))
    }

    fn splittable(&self, segment: Segment, _length: f64) -> bool {
        !same_point(segment.start, segment.end)
    }

    fn pieces(&self, segment: Segment, length: f64, max_length: f64) -> usize {
        planar_segmentize_steps_from_length(segment, length, max_length)
    }

    fn interior(
        &mut self,
        segment: Segment,
        (): Self::Solution,
        pieces: usize,
        source: &CoordSeq,
        source_segment: usize,
        output: &mut OutputColumns,
        _budget: &ExpansionBudget,
        _planned_generated: usize,
        _refined_generated: &mut usize,
    ) -> Result<()> {
        push_interpolated(&mut output.xs, segment.start.x, segment.end.x, pieces);
        push_interpolated(&mut output.ys, segment.start.y, segment.end.y, pieces);
        output.push_ordinates(source, source_segment, pieces);
        Ok(())
    }

    fn measure(&self, segment: Segment) -> Result<f64> {
        Ok(point_distance(segment.start, segment.end))
    }
}

pub(super) struct GeodesicRoute<'a> {
    pub(super) geodesic: &'a geographiclib_rs::Geodesic,
    pub(super) frame: GeographicFrame,
}

pub(super) struct Rhumb<'a> {
    pub(super) crs: &'a str,
    pub(super) state: &'a crate::crs::RuntimeState,
    pub(super) frame: GeographicFrame,
}

impl Route for GeodesicRoute<'_> {
    type Solution = DirectedLength;

    fn solve(&self, segment: Segment, _target: Target<'_>) -> Result<(f64, Self::Solution)> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("geographic subdivision received admitted coordinates");
        let end = self
            .frame
            .to_geodesic_point(point_from_xy(segment.end))
            .expect("geographic subdivision received admitted coordinates");
        let longitude_delta_native = lifted_longitude_delta(self.frame, segment);
        let route = crate::crs::checked_geodesic_route(self.geodesic, start, end)?;
        Ok((route.length, DirectedLength {
            bearing: route.azimuth,
            length: route.length,
            longitude_delta_native,
        }))
    }

    fn interior(
        &mut self,
        segment: Segment,
        solution: Self::Solution,
        pieces: usize,
        source: &CoordSeq,
        source_segment: usize,
        output: &mut OutputColumns,
        _budget: &ExpansionBudget,
        _planned_generated: usize,
        _refined_generated: &mut usize,
    ) -> Result<()> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("geographic subdivision received admitted coordinates");
        let end = self
            .frame
            .to_geodesic_point(point_from_xy(segment.end))
            .expect("geographic subdivision received admitted coordinates");
        emit_geodesic_plan(
            self.geodesic,
            self.frame,
            segment,
            start,
            end,
            solution,
            pieces,
            &mut output.xs,
            &mut output.ys,
        )
        .map(|()| output.push_ordinates(source, source_segment, pieces))
    }

    fn measure(&self, segment: Segment) -> Result<f64> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("geographic verification received admitted coordinates");
        let end = self
            .frame
            .to_geodesic_point(point_from_xy(segment.end))
            .expect("geographic verification received admitted coordinates");
        Ok(crate::crs::checked_geodesic_route(self.geodesic, start, end)?.length)
    }
}

impl Route for Rhumb<'_> {
    type Solution = DirectedLength;

    fn solve(&self, segment: Segment, _target: Target<'_>) -> Result<(f64, Self::Solution)> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("rhumb subdivision received admitted coordinates");
        let end = self
            .frame
            .to_geodesic_point(point_from_xy(segment.end))
            .expect("rhumb subdivision received admitted coordinates");
        let length = crate::crs::distance_rhumb_crs_at(
            self.crs, start.x, start.y, end.x, end.y, self.state,
        )?;
        let bearing =
            crate::crs::rhumb_bearing_crs_at(self.crs, start.x, start.y, end.x, end.y, self.state)?;
        Ok((length, DirectedLength {
            bearing,
            length,
            longitude_delta_native: lifted_longitude_delta(self.frame, segment),
        }))
    }

    fn interior(
        &mut self,
        segment: Segment,
        solution: Self::Solution,
        pieces: usize,
        source: &CoordSeq,
        source_segment: usize,
        output: &mut OutputColumns,
        _budget: &ExpansionBudget,
        _planned_generated: usize,
        _refined_generated: &mut usize,
    ) -> Result<()> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("rhumb subdivision received admitted coordinates");
        let lifted_end = segment.start.x + solution.longitude_delta_native;
        for step in 1..pieces {
            let fraction = step as f64 / pieces as f64;
            let Some((longitude, latitude)) = crate::crs::rhumb_destination_crs_at(
                self.crs,
                start.x,
                start.y,
                solution.bearing,
                solution.length * fraction,
                self.state,
            )?
            else {
                return Err(GeometryErrorKind::message(
                    "rhumb subdivision calculation crosses a pole",
                ));
            };
            let lifted_reference =
                interpolate_subdivision_ordinate(segment.start.x, lifted_end, fraction);
            let point = self
                .frame
                .point_from_geodesic_in_lifted_chart(
                    Point::new_raw_xy(longitude, latitude),
                    lifted_reference,
                )
                .expect("rhumb subdivision produced a finite coordinate");
            if !(point.x.is_finite() && point.y.is_finite()) {
                return Err(GeometryErrorKind::message(
                    "rhumb subdivision calculation produced a non-finite coordinate",
                ));
            }
            output.xs.push(point.x);
            output.ys.push(point.y);
        }
        output.push_ordinates(source, source_segment, pieces);
        Ok(())
    }

    fn measure(&self, segment: Segment) -> Result<f64> {
        let start = self
            .frame
            .to_geodesic_point(point_from_xy(segment.start))
            .expect("rhumb verification received admitted coordinates");
        let end = self
            .frame
            .to_geodesic_point(point_from_xy(segment.end))
            .expect("rhumb verification received admitted coordinates");
        crate::crs::distance_rhumb_crs_at(self.crs, start.x, start.y, end.x, end.y, self.state)
    }
}

pub(super) fn subdivide_placed(
    points: &CoordSeq,
    placement: &SegmentPlacement<'_>,
    target: Target<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    match placement {
        SegmentPlacement::Planar => {
            let mut route = Planar;
            subdivide(&mut route, points, target, budget)
        },
        SegmentPlacement::Chart(geodesic, frame) => {
            let mut route = Chart {
                geodesic,
                frame: *frame,
            };
            subdivide(&mut route, points, target, budget)
        },
        SegmentPlacement::Geodesic(geodesic, frame) => {
            let mut route = GeodesicRoute {
                geodesic,
                frame: *frame,
            };
            subdivide(&mut route, points, target, budget)
        },
        SegmentPlacement::Rhumb { crs, state, frame } => {
            let mut route = Rhumb {
                crs,
                state,
                frame: *frame,
            };
            subdivide(&mut route, points, target, budget)
        },
    }
}

fn emit_geodesic_plan(
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    segment: Segment,
    start: Point,
    end: Point,
    solution: DirectedLength,
    pieces: usize,
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
) -> Result<()> {
    let line = (start.x, start.y, solution.bearing, solution.length);
    let equatorial = start.y == 0.0 && end.y == 0.0 && (end.x - start.x).abs() < 180.0;
    let lifted_end = segment.start.x + solution.longitude_delta_native;
    for step in 1..pieces {
        let fraction = step as f64 / pieces as f64;
        let (mut longitude, mut latitude) = if equatorial {
            (
                interpolate_subdivision_ordinate(start.x, end.x, fraction),
                0.0,
            )
        } else {
            let info = crate::crs::geodesic_interpolate_on_line(geodesic, line, fraction)?;
            (info.longitude, info.latitude)
        };
        let lifted_reference =
            interpolate_subdivision_ordinate(segment.start.x, lifted_end, fraction);
        let point = frame
            .point_from_geodesic_in_lifted_chart(
                Point::new_raw_xy(longitude, latitude),
                lifted_reference,
            )
            .expect("geographic subdivision produced a finite coordinate");
        (longitude, latitude) = (point.x, point.y);
        if !(longitude.is_finite() && latitude.is_finite()) {
            return Err(GeometryErrorKind::message(
                "geodesic subdivision calculation produced a non-finite coordinate",
            ));
        }
        xs.push(longitude);
        ys.push(latitude);
    }
    Ok(())
}
