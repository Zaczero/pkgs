use std::simd::cmp::SimdPartialOrd as _;
use std::simd::num::SimdFloat as _;

use crate::boundary::metadata::Frame;
use crate::crs::CrsError;
use crate::geographic::{GeographicFrame, ScalarGeographicView, SeamIdentity};
use crate::geometry::{
    Coordinates, Point, REDUCE_LANES, ReduceSimd, Shape, ShapeData, TopologyDomain,
    canonical_f64_bits, shape_has_polar_ring_in_frame, simd_mask_any,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GeographicTopologyRoute {
    ExactPartition,
    Quotient,
}

impl GeographicTopologyRoute {
    pub(crate) const fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::ExactPartition, Self::ExactPartition) => Self::ExactPartition,
            _ => Self::Quotient,
        }
    }
}

/// Classify an admitted geographic view for exact planar topology.
///
/// ExactPartition can use stored longitude/latitude coordinates only when
/// they name one open chart: canonical ordinary vertices, no seam-crossing
/// edges, and no full-longitude ring. Seam and pole topology remains Q-owned.
pub(crate) fn classify_geographic_chart(view: ScalarGeographicView<'_>) -> GeographicTopologyRoute {
    let frame = view.frame();
    let mut canonical_ordinary_coordinates = true;
    view.for_each_coordinate(|coordinate| {
        canonical_ordinary_coordinates &= coordinate.identity(frame) == SeamIdentity::Ordinary
            && canonical_f64_bits(coordinate.native().x)
                == canonical_f64_bits(coordinate.longitude().get());
    });
    if canonical_ordinary_coordinates
        && !crosses_antimeridian_in_frame(view.shape(), frame)
        && !shape_has_polar_ring_in_frame(view.shape(), frame)
    {
        GeographicTopologyRoute::ExactPartition
    } else {
        GeographicTopologyRoute::Quotient
    }
}

pub(crate) fn geographic_topology_route<'a>(
    views: impl IntoIterator<Item = ScalarGeographicView<'a>>,
) -> GeographicTopologyRoute {
    for view in views {
        if classify_geographic_chart(view) == GeographicTopologyRoute::Quotient {
            return GeographicTopologyRoute::Quotient;
        }
    }
    GeographicTopologyRoute::ExactPartition
}

/// Resolve the complete topology domain from CRS metadata. WGS84 uses the
/// constant native frame; other geographic CRSs use their cached native frame.
pub(crate) fn topology_domain(frame: &Frame) -> crate::error::Result<TopologyDomain> {
    Ok(match frame.crs_str() {
        Some(crs) if crate::crs::is_wgs84_lonlat(crs) => {
            TopologyDomain::Geographic(GeographicFrame::crs84())
        },
        Some(crs) => crate::crs::info(crs)?
            .geographic_frame()
            .map_or(TopologyDomain::Cartesian, TopologyDomain::Geographic),
        None => TopologyDomain::Cartesian,
    })
}

/// The longitude frame of an antimeridian operation: a geographic CRS's own,
/// or CRS84 for CRS-free lon/lat. A projected CRS has no antimeridian.
pub(crate) fn antimeridian_frame(
    frame: &Frame,
    domain: TopologyDomain,
    op: &str,
) -> crate::error::Result<GeographicFrame> {
    match domain {
        TopologyDomain::Geographic(frame) => Ok(frame),
        TopologyDomain::Cartesian if frame.crs_ref().is_none() => Ok(GeographicFrame::crs84()),
        TopologyDomain::Cartesian => Err(CrsError::invalid(format!(
            "{op} requires a geographic CRS; use set_crs(...) or to_crs(...) to attach one"
        ))),
    }
}

/// Whether any chain of `shape` crosses the seam of `frame`
/// ([`chain_crosses_antimeridian`]).
pub(crate) fn crosses_antimeridian_in_frame(shape: &Shape, frame: GeographicFrame) -> bool {
    match shape {
        Shape::LineString(chain) => chain_crosses_antimeridian(chain, frame),
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => false,
        Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_) => matches!(
            shape.for_each_first_xy_segment_chain(|chain| {
                if chain_crosses_antimeridian(chain, frame) {
                    std::ops::ControlFlow::Break(())
                } else {
                    std::ops::ControlFlow::Continue(())
                }
            }),
            std::ops::ControlFlow::Break(())
        ),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SeamSide {
    East,
    West,
}

#[derive(Clone, Copy)]
struct SeamSegment {
    start_side: Option<SeamSide>,
    end_side: Option<SeamSide>,
    crosses: bool,
}

fn inspect_seam_segment(
    start: Point,
    end: Point,
    axis: crate::geographic::PeriodicAxis,
    half: f64,
) -> Option<SeamSegment> {
    let raw_delta = (end.x - start.x).abs();
    let (start_longitude, end_longitude) = (
        axis.canonicalize(start.x).ok()?,
        axis.canonicalize(end.x).ok()?,
    );
    let start_side = seam_side(start, start_longitude, axis);
    let end_side = seam_side(end, end_longitude, axis);
    let crosses = if start_side.is_some() || end_side.is_some() {
        raw_delta > half && raw_delta < axis.period().next_up()
    } else {
        (end_longitude.get() - start_longitude.get()).abs() > half
    };
    Some(SeamSegment {
        start_side,
        end_side,
        crosses,
    })
}

/// Whether a stored chain leaves the planar chart of `frame`: an edge takes
/// the long way between canonical longitudes, or runs more than a half turn
/// to or from the seam.
///
/// This is a property of the stored spelling, and deliberately so: it decides
/// whether planar evaluation of the stored coordinates is faithful. A ring
/// that touches the seam from the east but spells that vertex `-180` stores a
/// 350-degree edge and needs seam-aware handling, as does a stored edge that
/// winds past the seam around a pole.
pub(crate) fn chain_crosses_antimeridian<C: Coordinates + ?Sized>(
    chain: &C,
    frame: GeographicFrame,
) -> bool {
    let axis = frame.longitude();
    let half = axis.half_period();
    if let Some((xs, _)) = chain.xy_columns()
        && xs.iter().all(|x| x.abs() < half)
    {
        // Canonical longitudes off the seam: only a long step crosses.
        return steps_exceed(xs, half);
    }
    chain.segment_pairs().any(|[start, end]| {
        inspect_seam_segment(start, end, axis, half).is_some_and(|segment| segment.crosses)
    })
}

#[derive(Default)]
struct SeamContacts {
    east: Vec<(f64, f64)>,
    west: Vec<(f64, f64)>,
}

impl SeamContacts {
    fn observe_segment(
        &mut self,
        start: Point,
        start_side: Option<SeamSide>,
        end: Point,
        end_side: Option<SeamSide>,
    ) {
        match (start_side, end_side) {
            (Some(start_side), Some(end_side)) if start_side == end_side => {
                self.push(start_side, start.y.min(end.y), start.y.max(end.y));
            },
            (Some(start_side), Some(end_side)) => {
                self.push(start_side, start.y, start.y);
                self.push(end_side, end.y, end.y);
            },
            (Some(side), None) => self.push(side, start.y, start.y),
            (None, Some(side)) => self.push(side, end.y, end.y),
            (None, None) => {},
        }
    }

    fn push(&mut self, side: SeamSide, start: f64, end: f64) {
        match side {
            SeamSide::East => self.east.push((start, end)),
            SeamSide::West => self.west.push((start, end)),
        }
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "advance a cursor only after its corresponding interval was present"
    )]
    fn connects_across_seam(mut self) -> bool {
        if self.east.is_empty() || self.west.is_empty() {
            return false;
        }
        self.east
            .sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
        self.west
            .sort_unstable_by(|left, right| left.0.total_cmp(&right.0));

        let (mut east, mut west) = (0, 0);
        while let (Some(&(east_start, east_end)), Some(&(west_start, west_end))) =
            (self.east.get(east), self.west.get(west))
        {
            if east_start <= west_end && west_start <= east_end {
                return true;
            }
            if east_end < west_end {
                east += 1;
            } else {
                west += 1;
            }
        }
        false
    }
}

/// A single-pass seam scan for multipart derived-point routing. It preserves
/// the SIMD crossing check for chains whose coordinates stay off the seam,
/// and records seam contacts only for chains that contain one.
pub(crate) struct GeographicDerivedPointScan {
    frame: GeographicFrame,
    contacts: SeamContacts,
}

impl GeographicDerivedPointScan {
    pub(crate) fn new(frame: GeographicFrame) -> Self {
        Self {
            frame,
            contacts: SeamContacts::default(),
        }
    }

    /// Returns early when a chain itself crosses the seam; otherwise retains
    /// its seam-side contact intervals for comparison with other chains.
    pub(crate) fn scan_chain<C: Coordinates + ?Sized>(&mut self, chain: &C) -> bool {
        let frame = self.frame;
        chain_crosses_with_seam_visit(chain, frame, |start, start_side, end, end_side| {
            self.contacts
                .observe_segment(start, start_side, end, end_side);
        })
    }

    pub(crate) fn connects_across_seam(self) -> bool {
        self.contacts.connects_across_seam()
    }
}

fn seam_side(
    point: Point,
    longitude: crate::geographic::CanonicalLongitude,
    axis: crate::geographic::PeriodicAxis,
) -> Option<SeamSide> {
    (longitude.identity(axis) == SeamIdentity::Seam).then_some(if point.x > 0.0 {
        SeamSide::East
    } else {
        SeamSide::West
    })
}

fn chain_crosses_with_seam_visit<C, F>(
    chain: &C,
    frame: GeographicFrame,
    mut visit_seam_segment: F,
) -> bool
where
    C: Coordinates + ?Sized,
    F: FnMut(Point, Option<SeamSide>, Point, Option<SeamSide>),
{
    let axis = frame.longitude();
    let half = axis.half_period();
    if let Some((xs, _)) = chain.xy_columns()
        && xs.iter().all(|x| x.abs() < half)
    {
        // Canonical longitudes off the seam: only a long step crosses.
        return steps_exceed(xs, half);
    }
    let mut has_segment = false;
    for [start, end] in chain.segment_pairs() {
        has_segment = true;
        let Some(segment) = inspect_seam_segment(start, end, axis, half) else {
            continue;
        };
        if segment.crosses {
            return true;
        }
        visit_seam_segment(start, segment.start_side, end, segment.end_side);
    }
    if !has_segment && chain.coord_count() == 1 {
        let point = chain.nth_coord(0);
        if let Ok(longitude) = axis.canonicalize(point.x) {
            let side = seam_side(point, longitude, axis);
            if side.is_some() {
                visit_seam_segment(point, side, point, side);
            }
        }
    }
    false
}

/// Whether any consecutive pair of `xs` differs by more than `limit`.
fn steps_exceed(xs: &[f64], limit: f64) -> bool {
    let segments = xs.len().saturating_sub(1);
    if segments == 0 {
        return false;
    }
    let (x0, _) = xs[..segments].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=segments].as_chunks::<REDUCE_LANES>();
    simd_mask_any(
        segments,
        |index| (xs[index + 1] - xs[index]).abs() > limit,
        |start| {
            let chunk = start / REDUCE_LANES;
            (ReduceSimd::from_array(x1[chunk]) - ReduceSimd::from_array(x0[chunk]))
                .abs()
                .simd_gt(ReduceSimd::splat(limit))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, Polygon, Ring};

    fn polygon(points: &[(f64, f64)]) -> Shape {
        let points = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>();
        Shape::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    fn line(points: &[(f64, f64)]) -> Shape {
        let points = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>();
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap())
    }

    fn route(shape: &Shape) -> GeographicTopologyRoute {
        let view = ScalarGeographicView::try_new(GeographicFrame::crs84(), shape)
            .expect("fixture coordinates are admitted");
        classify_geographic_chart(view)
    }

    #[test]
    fn exact_partition_chart_admits_an_ordinary_closed_chart() {
        let ordinary = polygon(&[
            (-10.0, -10.0),
            (10.0, -10.0),
            (10.0, 10.0),
            (-10.0, 10.0),
            (-10.0, -10.0),
        ]);
        let frame = GeographicFrame::crs84();
        let view = ScalarGeographicView::try_new(frame, &ordinary).unwrap();

        assert_eq!(
            classify_geographic_chart(view),
            GeographicTopologyRoute::ExactPartition
        );
        assert_eq!(
            geographic_topology_route([view, view]),
            GeographicTopologyRoute::ExactPartition
        );
    }

    #[test]
    fn exact_partition_chart_rejects_each_quotient_boundary() {
        let seam_crossing = line(&[(170.0, 0.0), (-170.0, 0.0)]);
        let seam_touching = line(&[(180.0, 0.0), (179.0, 0.0)]);
        let seam_alias = Shape::Point(Point::new_raw_xy(360.0, 0.0));
        let pole_vertex = Shape::Point(Point::new_raw_xy(0.0, 90.0));
        let pole_enclosing_ring = polygon(&[
            (-135.0, 80.0),
            (-45.0, 80.0),
            (45.0, 80.0),
            (135.0, 80.0),
            (-135.0, 80.0),
        ]);

        for shape in [
            &seam_crossing,
            &seam_touching,
            &seam_alias,
            &pole_vertex,
            &pole_enclosing_ring,
        ] {
            assert_eq!(route(shape), GeographicTopologyRoute::Quotient, "{shape:?}",);
        }

        let ordinary = polygon(&[
            (-10.0, -10.0),
            (10.0, -10.0),
            (10.0, 10.0),
            (-10.0, 10.0),
            (-10.0, -10.0),
        ]);
        let ordinary_view =
            ScalarGeographicView::try_new(GeographicFrame::crs84(), &ordinary).unwrap();
        let seam_view =
            ScalarGeographicView::try_new(GeographicFrame::crs84(), &seam_touching).unwrap();
        assert_eq!(
            geographic_topology_route([ordinary_view, seam_view]),
            GeographicTopologyRoute::Quotient
        );
    }
}

/// Whether a planar topology op must split-normalize ``shape`` at ±180 before
/// evaluating. Cartesian domains skip the coordinate scan entirely.
pub(crate) fn geographic_crossing(domain: TopologyDomain, shape: &ShapeData) -> bool {
    match domain {
        TopologyDomain::Cartesian => false,
        TopologyDomain::Geographic(frame) => crosses_antimeridian_in_frame(shape.shape(), frame),
    }
}
