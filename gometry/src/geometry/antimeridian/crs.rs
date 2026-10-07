use std::simd::cmp::SimdPartialOrd as _;
use std::simd::num::SimdFloat as _;

use crate::crs::CrsError;
use crate::crs::frame::Frame;
use crate::geographic::{GeographicFrame, ScalarGeographicView, SeamIdentity};
use crate::geometry::{
    Coordinates, Point, REDUCE_LANES, ReduceSimd, Shape, TopologyDomain, canonical_f64_bits,
    shape_has_polar_ring_in_frame,
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
    let has_noncanonical_coordinate = view.any_first_xy_coordinate(|coordinate| {
        coordinate.identity(frame) != SeamIdentity::Ordinary
            || canonical_f64_bits(coordinate.native_xy().x)
                != canonical_f64_bits(coordinate.longitude().get())
    });
    if !has_noncanonical_coordinate
        && !crosses_antimeridian_in_frame(view.shape(), frame)
        && !shape_has_polar_ring_in_frame(view.shape(), frame)
    {
        GeographicTopologyRoute::ExactPartition
    } else {
        GeographicTopologyRoute::Quotient
    }
}

/// Unary predicates can use stored linework in one certified open chart without
/// moving its coordinates. Binary predicates still require aligned charts.
pub(crate) fn classify_geographic_validity_chart(
    view: ScalarGeographicView<'_>,
) -> GeographicTopologyRoute {
    let faithful = match view.shape() {
        Shape::LineString(line) => {
            faithful_geographic_chain(line.as_coords().xs(), line.as_coords().ys(), view.frame())
        },
        Shape::Polygon(polygon) if polygon.holes.is_empty() => faithful_geographic_chain(
            polygon.shell.coords().xs(),
            polygon.shell.coords().ys(),
            view.frame(),
        ),
        Shape::MultiPolygon(polygons)
            if polygons.iter().all(|polygon| polygon.holes.is_empty()) =>
        {
            faithful_geographic_chains(
                polygons.iter().map(|polygon| {
                    let coords = polygon.shell.coords();
                    (coords.xs(), coords.ys())
                }),
                view.frame(),
            )
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => false,
    };
    if faithful {
        GeographicTopologyRoute::ExactPartition
    } else {
        classify_geographic_chart(view)
    }
}

/// Certify one unchanged source chain inside a canonical or adjacent open
/// longitude chart, with no pole, seam vertex, or long stored edge.
pub(crate) fn faithful_geographic_chain(xs: &[f64], ys: &[f64], frame: GeographicFrame) -> bool {
    faithful_geographic_chains(std::iter::once((xs, ys)), frame)
}

fn faithful_geographic_chains<'a>(
    chains: impl Iterator<Item = (&'a [f64], &'a [f64])>,
    frame: GeographicFrame,
) -> bool {
    let mut chains = chains.filter(|(xs, ys)| !xs.is_empty() || !ys.is_empty());
    let Some((xs, ys)) = chains.next() else {
        return true;
    };
    let Some(&first) = xs.first() else {
        return false;
    };
    let axis = frame.longitude();
    let bound = axis.ordinary_longitude_bound();
    // Rounded native periods can give integer aliases a different translation
    // from the represented period. Only exact integral periods certify an
    // adjacent chart without canonicalizing its vertices.
    if first.abs() >= bound && axis.period().fract() != 0.0 {
        return false;
    }
    let (lower, upper) = if first.abs() < bound {
        (-bound, bound)
    } else if first > 0.0 {
        (
            (axis.period() - bound).next_up(),
            (axis.period() + bound).next_down(),
        )
    } else {
        (
            (-axis.period() - bound).next_up(),
            (-axis.period() + bound).next_down(),
        )
    };
    if !(lower.is_finite() && upper.is_finite() && first > lower && first < upper) {
        return false;
    }
    std::iter::once((xs, ys)).chain(chains).all(|(xs, ys)| {
        let Some((&first, rest)) = xs.split_first() else {
            return ys.is_empty();
        };
        if !(first > lower && first < upper) {
            return false;
        }
        let mut previous = first;
        rest.iter().all(|&x| {
            let admitted = x > lower && x < upper && (x - previous).abs() <= axis.half_period();
            previous = x;
            admitted
        }) && ys.iter().all(|&y| y.abs() < frame.pole())
    })
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
    let state = frame
        .crs_str()
        .filter(|crs| !crate::crs::is_wgs84_lonlat(crs))
        .map(|_| crate::crs::runtime_state_snapshot())
        .transpose()?;
    topology_domain_at(frame, state.as_deref())
}

pub(crate) fn topology_domain_at(
    frame: &Frame,
    state: Option<&crate::crs::RuntimeState>,
) -> crate::error::Result<TopologyDomain> {
    Ok(match frame.crs_str() {
        Some(crs) if crate::crs::is_wgs84_lonlat(crs) => {
            TopologyDomain::Geographic(GeographicFrame::crs84())
        },
        Some(crs) => {
            let state = state.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "a non-WGS84 topology frame requires its CRS runtime snapshot",
                )
            })?;
            crate::crs::info_with_state(crs, state)?
                .geographic_frame()
                .map_or(TopologyDomain::Cartesian, TopologyDomain::Geographic)
        },
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
) -> Option<SeamSegment> {
    let connection = axis.chart_connection(start.x, end.x).ok()?;
    let (start_longitude, end_longitude) = (connection.start, connection.end);
    let start_side = seam_side(start, start_longitude, axis);
    let end_side = seam_side(end, end_longitude, axis);
    Some(SeamSegment {
        start_side,
        end_side,
        crosses: connection.crosses_antimeridian(axis, start.x, end.x),
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
    if let Some((xs, ys)) = chain.xy_columns() {
        if let Some(crosses) = canonical_chart_crossing(xs, half) {
            return crosses;
        }
        for (&[x0, x1], &[y0, y1]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
            if inspect_seam_segment(Point::new_raw_xy(x0, y0), Point::new_raw_xy(x1, y1), axis)
                .is_some_and(|segment| segment.crosses)
            {
                return true;
            }
        }
        return false;
    }
    chain.segment_pairs().any(|[start, end]| {
        inspect_seam_segment(start, end, axis).is_some_and(|segment| segment.crosses)
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
        && let Some(crosses) = canonical_chart_crossing(xs, half)
    {
        return crosses;
    }
    let mut has_segment = false;
    for [start, end] in chain.segment_pairs() {
        has_segment = true;
        let Some(segment) = inspect_seam_segment(start, end, axis) else {
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

/// Classify crossings only when every longitude is in the open canonical chart.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "floating longitude differences classify chart steps without integer overflow"
)]
fn canonical_chart_crossing(xs: &[f64], limit: f64) -> Option<bool> {
    let segments = xs.len().saturating_sub(1);
    if segments == 0 {
        return xs.iter().all(|x| x.abs() < limit).then_some(false);
    }
    let (lefts, _) = xs.split_at(segments);
    let (_, rights) = xs.split_at(1);
    let (left_chunks, left_tail) = lefts.as_chunks::<REDUCE_LANES>();
    let (right_chunks, right_tail) = rights.as_chunks::<REDUCE_LANES>();
    let limit_value = limit;
    let limit_lanes = ReduceSimd::splat(limit_value);
    let mut crosses = false;
    for (&left, &right) in left_chunks.iter().zip(right_chunks) {
        let left = ReduceSimd::from_array(left);
        let right = ReduceSimd::from_array(right);
        let ordinary = left.abs().simd_lt(limit_lanes) & right.abs().simd_lt(limit_lanes);
        if !ordinary.all() {
            return None;
        }
        crosses |= (right - left).abs().simd_gt(limit_lanes).any();
    }
    for (&left, &right) in left_tail.iter().zip(right_tail) {
        if !(left.abs() < limit_value && right.abs() < limit_value) {
            return None;
        }
        crosses |= (right - left).abs() > limit_value;
    }
    Some(crosses)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, Polygon, Ring};

    #[test]
    fn canonical_crossing_scan_checks_every_shifted_pair_and_the_entire_chart() {
        for len in 2..=3 * REDUCE_LANES + 2 {
            for crossing in 0..len - 1 {
                let mut xs = vec![170.0; len];
                xs[crossing + 1] = -170.0;
                assert_eq!(canonical_chart_crossing(&xs, 180.0), Some(true));
            }
        }
        for alias in [180.0, 360.0, f64::NAN] {
            let mut xs = vec![170.0; 3 * REDUCE_LANES + 2];
            xs[1] = -170.0;
            *xs.last_mut().unwrap() = alias;
            assert_eq!(canonical_chart_crossing(&xs, 180.0), None);
        }
    }

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
    fn unary_chart_certifies_aliases_without_aligning_binary_charts() {
        let frame = GeographicFrame::crs84();
        for west in [200.0, -210.0, 350.0, -10.0] {
            let shape = polygon(&[
                (west, 0.0),
                (west + 5.0, 0.0),
                (west + 5.0, 5.0),
                (west, 5.0),
                (west, 0.0),
            ]);
            let view = ScalarGeographicView::try_new(frame, &shape).unwrap();
            assert_eq!(
                classify_geographic_validity_chart(view),
                GeographicTopologyRoute::ExactPartition
            );
            if west.abs() > 180.0 {
                assert_eq!(
                    classify_geographic_chart(view),
                    GeographicTopologyRoute::Quotient
                );
            }
            let exact =
                crate::geometry::ExactGeographicTopology::from_views(frame, std::iter::once(view))
                    .unwrap();
            assert_eq!(
                shape.validate().unwrap(),
                exact.validity_issue(&shape).unwrap()
            );
        }
        for shape in [
            line(&[(170.0, 0.0), (190.0, 0.0)]),
            line(&[(200.0, 0.0), (560.0, 0.0)]),
            line(&[(180.0, 0.0), (190.0, 0.0)]),
            line(&[(200.0, 90.0), (210.0, 80.0)]),
            line(&[(200.0, 0.0), (500.0, 0.0)]),
        ] {
            let view = ScalarGeographicView::try_new(frame, &shape).unwrap();
            assert_eq!(
                classify_geographic_validity_chart(view),
                GeographicTopologyRoute::Quotient,
                "{shape:?}"
            );
        }
    }

    #[test]
    fn unary_multipart_certificate_requires_one_common_chart() {
        let rectangle = |west| {
            let Shape::Polygon(polygon) = polygon(&[
                (west, 0.0),
                (west + 5.0, 0.0),
                (west + 5.0, 5.0),
                (west, 5.0),
                (west, 0.0),
            ]) else {
                unreachable!("polygon helper returns a polygon");
            };
            polygon
        };
        let frame = GeographicFrame::crs84();
        for (west, east, route, expected_code) in [
            (200.0, 220.0, GeographicTopologyRoute::ExactPartition, None),
            (
                -220.0,
                -200.0,
                GeographicTopologyRoute::ExactPartition,
                None,
            ),
            (
                200.0,
                202.0,
                GeographicTopologyRoute::ExactPartition,
                Some("shared_linework"),
            ),
            (
                200.0,
                -160.0,
                GeographicTopologyRoute::Quotient,
                Some("shared_linework"),
            ),
            (200.0, -140.0, GeographicTopologyRoute::Quotient, None),
            (200.0, 0.0, GeographicTopologyRoute::Quotient, None),
        ] {
            let shape = Shape::MultiPolygon(vec![rectangle(west), rectangle(east)].into());
            let view = ScalarGeographicView::try_new(frame, &shape).unwrap();
            assert_eq!(classify_geographic_validity_chart(view), route);
            let exact =
                crate::geometry::ExactGeographicTopology::from_views(frame, std::iter::once(view))
                    .unwrap();
            let quotient = exact.validity_issue(&shape).unwrap();
            assert_eq!(
                quotient.as_ref().map(|issue| issue.code),
                expected_code,
                "{west}, {east}"
            );
            if route == GeographicTopologyRoute::ExactPartition {
                let planar = shape.validate().unwrap();
                assert_eq!(planar.as_ref().map(|issue| issue.code), expected_code);
                assert_eq!(
                    planar.as_ref().and_then(|issue| issue.path.as_deref()),
                    quotient.as_ref().and_then(|issue| issue.path.as_deref())
                );
            }
        }
    }

    #[test]
    fn unary_alias_certificate_declines_rounded_periods_and_seam_neighbors() {
        for factor in [
            (std::f64::consts::PI / 180.0).next_down(),
            (std::f64::consts::PI / 180.0).next_up(),
            std::f64::consts::TAU / 400.0,
        ] {
            let unit = crate::geographic::AngularFactor::try_new("native", factor).unwrap();
            let frame = GeographicFrame::try_new(unit, unit, 0.0).unwrap();
            if frame.longitude().period().fract() != 0.0 {
                assert!(!faithful_geographic_chain(
                    &[350.0, 360.0, 370.0],
                    &[0.0; 3],
                    frame
                ));
                assert!(!faithful_geographic_chain(
                    &[-350.0, -360.0, -370.0],
                    &[0.0; 3],
                    frame
                ));
            }
        }
        let frame = GeographicFrame::crs84();
        assert!(faithful_geographic_chain(&[200.0, 380.0], &[0.0; 2], frame));
        assert!(!faithful_geographic_chain(
            &[200.0, 380.0_f64.next_up()],
            &[0.0; 2],
            frame
        ));
        for x in [180.0_f64.next_down(), 180.0, 540.0, 540.0_f64.next_up()] {
            assert!(!faithful_geographic_chain(&[200.0, x], &[0.0; 2], frame));
        }
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
            assert_eq!(route(shape), GeographicTopologyRoute::Quotient, "{shape:?}");
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

    #[test]
    fn explicit_period_circle_is_an_antimeridian_crossing() {
        let frame = GeographicFrame::crs84();
        let circle = line(&[(0.0, 0.0), (360.0, 0.0)]);
        let Shape::LineString(circle_line) = &circle else {
            unreachable!("line helper constructs a LineString");
        };
        assert!(chain_crosses_antimeridian(circle_line, frame,));
        assert_eq!(
            classify_geographic_chart(
                crate::geographic::ScalarGeographicView::try_new(frame, &circle).unwrap()
            ),
            GeographicTopologyRoute::Quotient
        );

        let seam_pair = line(&[(-180.0, 0.0), (180.0, 0.0)]);
        let Shape::LineString(seam_line) = &seam_pair else {
            unreachable!("line helper constructs a LineString");
        };
        assert!(
            chain_crosses_antimeridian(seam_line, frame),
            "the raw planar seam-to-seam spelling still spans the chart"
        );
        assert_eq!(
            frame.longitude().chart_step(-180.0, 180.0).unwrap(),
            crate::geographic::ChartStep::Shortest(crate::geographic::EdgeDirection::Minor(0.0))
        );
    }

    #[test]
    fn seam_crossing_from_xy_columns_is_independent_of_ordinate_layout() {
        let frame = GeographicFrame::crs84();
        let ordinate_columns = [
            (None, None),
            (Some(vec![3.0, 7.0]), None),
            (None, Some(vec![11.0, 19.0])),
            (Some(vec![3.0, 7.0]), Some(vec![11.0, 19.0])),
        ];
        for (zs, ms) in ordinate_columns {
            let coords =
                CoordSeq::try_from_vecs(vec![170.0, -170.0], vec![2.0, 5.0], zs, ms).unwrap();
            let line = LineSeq::try_new(coords).unwrap();
            assert!(chain_crosses_antimeridian(&line, frame));
        }
    }

    #[test]
    fn ordinary_width_does_not_certify_a_noncanonical_seam_alias() {
        let frame = GeographicFrame::crs84();
        let shape = polygon(&[
            (179.0, -10.0),
            (181.0, -10.0),
            (181.0, 10.0),
            (179.0, 10.0),
            (179.0, -10.0),
        ]);
        assert!(
            !frame.bounds_certify_ordinary_support(shape.bounds().unwrap()),
            "a two-degree raw span may still cross the canonical seam"
        );

        let domain = TopologyDomain::Geographic(frame);
        let expected = crate::geometry::unary_antimeridian_derived(
            &shape,
            domain,
            crate::geometry::DerivedPointStrategy::Centroid,
            Shape::centroid,
        )
        .unwrap();
        assert_eq!(
            crate::geometry::unary_antimeridian_centroid(&shape, domain).unwrap(),
            expected
        );
    }
}
