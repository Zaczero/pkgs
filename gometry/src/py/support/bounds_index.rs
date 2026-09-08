use rstar::Envelope as _;

use crate::geographic::{
    CircularInterval, GeographicFrame, GeographicRing, GeographicViewError, LongitudeWindows,
    RingDomain, RingSummary, RingTurn, ScalarGeographicView, SeamIdentity,
    union_circular_intervals,
};
use crate::geometry::TopologyDomain;
use crate::predicates::engine::IndexEnvelope;
use crate::py::errors::InvalidGeometryError;
use crate::py::support::{AABB, Bounds, Point, Shape};

pub(crate) fn bounds_envelope(bounds: Bounds) -> AABB<[f64; 2]> {
    // Bounds are ordered by construction — skip from_corners' min/max pass.
    AABB::from_bounds([bounds.minx(), bounds.miny()], [
        bounds.maxx(),
        bounds.maxy(),
    ])
}

/// How many windows a cover may hold before further parts merge into the ones
/// it has.
///
/// A descent is cheap and a loose window is not, so this sits high enough that
/// an ordinary multipart geometry keeps its parts: a cap that forces merges
/// sprawls them across the gaps, and the cover is then rejected outright for
/// covering no less ground than one box did. What the cap exists for is the
/// row that would otherwise put a window on every vertex — a MultiPoint of a
/// hundred thousand points — since each window is another tree entry that the
/// index holds for as long as the row lives.
const MAX_COVER_WINDOWS: usize = 64;

/// Where a shape sits, as the index sees it.
///
/// One box around a scattered multipart geometry also covers the ground
/// between its parts, which for widely separated parts is nearly all of the
/// box — a search admits every row lying in the gaps and pays an exact test on
/// each. Covering the parts themselves ties selectivity to the ground a
/// geometry occupies rather than to how its parts happen to be packaged, so a
/// MultiPolygon narrows a search as well as an array of its polygons does.
#[derive(Clone, Debug)]
pub(crate) enum EnvelopeCover {
    /// Nothing worth decomposing; these windows serve every lookup.
    Whole(Vec<AABB<[f64; 2]>>),
    /// Parts far enough apart to be worth searching separately, and the
    /// whole-shape windows containment still needs.
    Scattered {
        whole: Vec<AABB<[f64; 2]>>,
        parts: Vec<AABB<[f64; 2]>>,
    },
}

impl EnvelopeCover {
    /// The windows a candidate search visits under `policy`.
    ///
    /// Containment reads the whole shape even when a cover is available: a row
    /// spanning two parts lies inside the whole and inside no single part, so
    /// searching the parts would drop it.
    pub(crate) fn windows(&self, policy: IndexEnvelope) -> &[AABB<[f64; 2]>] {
        match self {
            Self::Whole(whole) => whole,
            Self::Scattered { whole, parts } => match policy {
                IndexEnvelope::Intersecting => parts,
                IndexEnvelope::ContainedInQuery => whole,
            },
        }
    }

    /// The windows stored for an indexed row, where parts are sound for every
    /// lookup: each is a subset of the whole, so a containment search gains
    /// candidates to refine away rather than losing any, and a nearest lower
    /// bound only tightens.
    pub(crate) fn entries(&self) -> &[AABB<[f64; 2]>] {
        match self {
            Self::Whole(whole) => whole,
            Self::Scattered { parts, .. } => parts,
        }
    }
}

/// Minimal conservative index windows in the coordinate system of `frame`.
/// Periodic frames split an arc at the canonical seam and retain the logical
/// row id on every physical entry.
pub(crate) fn index_envelopes(
    shape: &Shape,
    bounds: Bounds,
    domain: TopologyDomain,
) -> pyo3::PyResult<EnvelopeCover> {
    let planar = bounds_envelope(bounds);
    let mut whole = Vec::new();
    match domain {
        TopologyDomain::Cartesian => crate::try_push_vec(&mut whole, planar)?,
        TopologyDomain::Geographic(frame) => {
            whole = geographic_index_envelopes(shape, frame)?;
        },
    }
    // Coordinate bounds describe a part's ground faithfully only where the
    // frame had nothing to add about the whole shape. A seam-crossing or
    // pole-enclosing geometry gets windows coordinate bounds cannot express —
    // an enclosed pole reaches a latitude no vertex holds — and parts taken
    // from coordinates could then miss ground the shape covers.
    if whole.len() != 1 || whole[0] != planar {
        return Ok(EnvelopeCover::Whole(whole));
    }
    match part_cover(shape, planar)? {
        Some(parts) => Ok(EnvelopeCover::Scattered { whole, parts }),
        None => Ok(EnvelopeCover::Whole(whole)),
    }
}

/// A cover of `shape`'s parts, or `None` where splitting it up buys nothing.
fn part_cover(shape: &Shape, whole: AABB<[f64; 2]>) -> pyo3::PyResult<Option<Vec<AABB<[f64; 2]>>>> {
    if !matches!(
        shape,
        Shape::MultiPoint(_)
            | Shape::MultiLineString(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
    ) {
        return Ok(None);
    }
    let mut cover = Vec::new();
    for_each_part(shape, &mut |part| admit(&mut cover, part))?;
    // The extra entries and descents have to buy something. Adjacent parts
    // merge back into one window, and a compact geometry then covers exactly
    // what its single box already did.
    let covered: f64 = cover.iter().map(margin).sum();
    Ok((cover.len() > 1 && covered < margin(&whole)).then_some(cover))
}

/// Fold `part` into `cover`, holding it to [`MAX_COVER_WINDOWS`] windows.
///
/// A merge spending no more margin than the two windows already spend is free:
/// overlapping and adjacent parts collapse into one window and distant parts
/// stay apart. Once the cover is full the cheapest merge is taken instead.
fn admit(cover: &mut Vec<AABB<[f64; 2]>>, part: AABB<[f64; 2]>) -> pyo3::PyResult<()> {
    let part_margin = margin(&part);
    let mut free = None;
    let mut cheapest: Option<(usize, f64)> = None;
    for (index, window) in cover.iter().enumerate() {
        let merged = margin(&window.merged(&part));
        if merged <= margin(window) + part_margin {
            free = Some(index);
            break;
        }
        let growth = merged - margin(window);
        if cheapest.is_none_or(|(_, best)| growth < best) {
            cheapest = Some((index, growth));
        }
    }
    if let Some(index) = free {
        cover[index] = cover[index].merged(&part);
        return Ok(());
    }
    if cover.len() < MAX_COVER_WINDOWS {
        return Ok(crate::try_push_vec(cover, part)?);
    }
    let (index, _) = cheapest.expect("a full cover holds a window to merge into");
    cover[index] = cover[index].merged(&part);
    Ok(())
}

/// Half the perimeter — the standard R-tree margin. Unlike area it stays
/// discriminating for a degenerate window, so a run of collinear points does
/// not read as free to merge into the segment through them.
fn margin(envelope: &AABB<[f64; 2]>) -> f64 {
    let lower = envelope.lower();
    let upper = envelope.upper();
    (upper[0] - lower[0]) + (upper[1] - lower[1])
}

/// Every part of `shape` that occupies ground, as its own window.
fn for_each_part(
    shape: &Shape,
    visit: &mut impl FnMut(AABB<[f64; 2]>) -> pyo3::PyResult<()>,
) -> pyo3::PyResult<()> {
    match shape {
        Shape::MultiPoint(points) => {
            for point in points {
                visit(bounds_envelope(Bounds::from_point(point)))?;
            }
        },
        Shape::MultiLineString(lines) => {
            for line in lines.iter() {
                if let Some(bounds) = Bounds::from_coords(line) {
                    visit(bounds_envelope(bounds))?;
                }
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                // The shell bounds the polygon; its holes lie inside.
                if let Some(bounds) = Bounds::from_coords(polygon.shell.coords()) {
                    visit(bounds_envelope(bounds))?;
                }
            }
        },
        Shape::GeometryCollection(members) => {
            for member in members {
                for_each_part(member, visit)?;
            }
        },
        _ => {
            if let Some(bounds) = shape.bounds() {
                visit(bounds_envelope(bounds))?;
            }
        },
    }
    Ok(())
}

fn geographic_view_py_error(error: GeographicViewError) -> pyo3::PyErr {
    match error {
        GeographicViewError::Resource => {
            crate::error::Error::from(crate::error::ResourceError::Allocation).into()
        },
        GeographicViewError::Diagnostic(diagnostic) => {
            InvalidGeometryError::new_err(diagnostic.to_string())
        },
    }
}

fn geographic_index_envelopes(
    shape: &Shape,
    frame: GeographicFrame,
) -> pyo3::PyResult<Vec<AABB<[f64; 2]>>> {
    let view = ScalarGeographicView::try_new(frame, shape).map_err(geographic_view_py_error)?;
    let mut intervals = Vec::new();
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    let mut full_longitude = false;
    let mut allocation_error = None;
    view.for_each_coordinate(|coordinate| {
        south = south.min(coordinate.latitude().get());
        north = north.max(coordinate.latitude().get());
        if coordinate.identity(frame) == SeamIdentity::Pole {
            full_longitude = true;
            if coordinate.latitude().get().is_sign_negative() {
                south = -frame.pole();
            } else {
                north = frame.pole();
            }
        }
        let interval =
            CircularInterval::try_arc(frame.longitude(), coordinate.longitude().get(), 0.0)
                .expect("admitted longitude makes a point interval");
        if let Err(error) = crate::try_push_vec(&mut intervals, interval) {
            allocation_error = Some(error);
        }
    });
    if let Some(error) = allocation_error {
        return Err(error.into());
    }
    let mut edge_error = None;
    shape.for_each_segment_chain(|chain| {
        for pair in chain.xs().windows(2) {
            if edge_error.is_some() {
                break;
            }
            let delta = frame
                .longitude()
                .shortest_edge(pair[0], pair[1])
                .expect("the geographic view admitted every chain coordinate")
                .delta(frame.longitude());
            let (start, sweep) = if delta.is_sign_negative() {
                (pair[1], -delta)
            } else {
                (pair[0], delta)
            };
            let interval = CircularInterval::try_arc(frame.longitude(), start, sweep)
                .expect("a shortest edge is a valid circular arc");
            if let Err(error) = crate::try_push_vec(&mut intervals, interval) {
                edge_error = Some(error);
            }
        }
    });
    if let Some(error) = edge_error {
        return Err(error.into());
    }
    if intervals.is_empty() {
        return Ok(Vec::new());
    }
    try_for_each_ring_summary(shape, frame, |summary| {
        if matches!(summary.domain, RingDomain::FullLongitude(_)) {
            full_longitude = true;
        }
        if matches!(summary.turn, RingTurn::Full(_)) {
            if summary.south.get().is_sign_positive() {
                north = frame.pole();
            } else if summary.north.get().is_sign_negative() {
                south = -frame.pole();
            }
        }
    })?;
    let longitude = if full_longitude {
        CircularInterval::Full
    } else {
        union_circular_intervals(frame.longitude(), intervals)?
    };
    let mut envelopes = Vec::new();
    let mut push = |window: [f64; 2]| {
        crate::try_push_vec(
            &mut envelopes,
            AABB::from_bounds([window[0], south], [window[1], north]),
        )
    };
    match longitude.windows(frame.longitude()) {
        Some(LongitudeWindows::One(window)) => push(window)?,
        Some(LongitudeWindows::Two(left, right)) => {
            push(left)?;
            push(right)?;
        },
        Some(LongitudeWindows::Full) => {
            push([
                -frame.longitude().half_period(),
                frame.longitude().half_period(),
            ])?;
        },
        None => {},
    }
    Ok(envelopes)
}

fn try_for_each_ring_summary(
    shape: &Shape,
    frame: GeographicFrame,
    mut visit: impl FnMut(RingSummary),
) -> pyo3::PyResult<()> {
    fn recurse(
        shape: &Shape,
        frame: GeographicFrame,
        visit: &mut impl FnMut(RingSummary),
    ) -> pyo3::PyResult<()> {
        fn summarize(
            polygon: &crate::geometry::Polygon,
            frame: GeographicFrame,
            visit: &mut impl FnMut(RingSummary),
        ) -> pyo3::PyResult<()> {
            for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
                let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
                    .map_err(geographic_view_py_error)?;
                visit(summary);
            }
            Ok(())
        }
        match shape {
            Shape::Polygon(polygon) => summarize(polygon, frame, visit)?,
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons.iter() {
                    summarize(polygon, frame, visit)?;
                }
            },
            Shape::GeometryCollection(members) => {
                for member in members {
                    recurse(member, frame, visit)?;
                }
            },
            _ => {},
        }
        Ok(())
    }
    recurse(shape, frame, &mut visit)
}

pub(crate) fn global_geographic_candidate_envelope() -> AABB<[f64; 2]> {
    AABB::from_bounds([-f64::MAX, -f64::MAX], [f64::MAX, f64::MAX])
}

pub(crate) fn point_from_bounds(bounds: Bounds) -> Option<Point> {
    (bounds.minx().total_cmp(&bounds.maxx()).is_eq()
        && bounds.miny().total_cmp(&bounds.maxy()).is_eq())
    .then_some(Point::new_raw_xy(bounds.minx(), bounds.miny()))
}

#[cfg(test)]
mod periodic_tests {
    use super::*;
    use crate::geographic::AngularFactor;
    use crate::geometry::{CoordinateAxes, MultiMember, MultiPoint, Polygon, Ring};

    fn frame(radians_per_unit: f64) -> GeographicFrame {
        let unit = AngularFactor::try_new("test unit", radians_per_unit).unwrap();
        GeographicFrame::try_new(unit, unit, 0.0).unwrap()
    }

    fn multipoint(points: &[(f64, f64)]) -> Shape {
        Shape::MultiPoint(
            MultiPoint::try_new(
                CoordinateAxes::XY,
                points
                    .iter()
                    .map(|&(x, y)| MultiMember::Value(Point::new(x, y).unwrap()))
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn polygon(xs: &[f64], ys: &[f64], holes: Vec<Ring>) -> Shape {
        Shape::Polygon(Polygon {
            shell: Ring::closed(
                xs.iter()
                    .zip(ys)
                    .map(|(&x, &y)| Point::new(x, y).unwrap())
                    .collect(),
            )
            .unwrap(),
            holes: crate::geometry::SharedSlice::try_from_vec(holes)
                .expect("test polygon holes are allocatable"),
        })
    }

    #[test]
    fn aliases_collapse_and_seam_neighbors_split_minimally() {
        for native in [
            frame(std::f64::consts::PI / 180.0),
            frame(1.0),
            frame(std::f64::consts::TAU / 400.0),
        ] {
            let period = native.longitude().period();
            let aliases =
                geographic_index_envelopes(&multipoint(&[(0.0, 0.0), (period, 0.0)]), native)
                    .unwrap();
            assert_eq!(aliases.len(), 1);
            assert_eq!(aliases[0].lower(), [0.0, 0.0]);
            assert_eq!(aliases[0].upper(), [0.0, 0.0]);
        }

        let degrees = frame(std::f64::consts::PI / 180.0);
        let seam = geographic_index_envelopes(&multipoint(&[(-179.0, 0.0), (179.0, 0.0)]), degrees)
            .unwrap();
        assert_eq!(seam.len(), 2);
        assert_eq!((seam[0].upper()[0] - seam[0].lower()[0]).abs(), 1.0);
        assert_eq!((seam[1].upper()[0] - seam[1].lower()[0]).abs(), 1.0);
    }

    #[test]
    fn typed_poles_caps_holes_and_axis_bands_own_full_longitude() {
        for native in [
            frame(std::f64::consts::PI / 180.0),
            frame(1.0),
            frame(std::f64::consts::TAU / 400.0),
        ] {
            let half = native.longitude().half_period();
            let pole = native.pole();
            for sign in [-1.0, 1.0] {
                let latitude = sign * pole;
                let point = geographic_index_envelopes(
                    &Shape::Point(Point::new(half / 3.0, latitude).unwrap()),
                    native,
                )
                .unwrap();
                assert_eq!(point[0].lower(), [-half, latitude]);
                assert_eq!(point[0].upper(), [half, latitude]);

                let exterior = if sign.is_sign_positive() {
                    latitude.next_up()
                } else {
                    latitude.next_down()
                };
                let admitted = geographic_index_envelopes(
                    &Shape::Point(Point::new(-half / 4.0, exterior).unwrap()),
                    native,
                )
                .unwrap();
                assert_eq!(admitted[0].lower(), [-half, latitude]);
                assert_eq!(admitted[0].upper(), [half, latitude]);

                let rejected = if sign.is_sign_positive() {
                    exterior.next_up()
                } else {
                    exterior.next_down()
                };
                geographic_index_envelopes(
                    &Shape::Point(Point::new(0.0, rejected).unwrap()),
                    native,
                )
                .unwrap_err();
            }

            let cap = polygon(
                &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
                &[pole / 2.0; 4],
                Vec::new(),
            );
            let cap = geographic_index_envelopes(&cap, native).unwrap();
            assert_eq!(cap[0].lower(), [-half, pole / 2.0]);
            assert_eq!(cap[0].upper(), [half, pole]);

            let south_cap = polygon(
                &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
                &[-pole / 2.0; 4],
                Vec::new(),
            );
            let south_cap = geographic_index_envelopes(&south_cap, native).unwrap();
            assert_eq!(south_cap[0].lower(), [-half, -pole]);
            assert_eq!(south_cap[0].upper(), [half, -pole / 2.0]);

            let band = polygon(
                &[-half, half, half, -half, -half],
                &[
                    -pole / 2.0,
                    -pole / 2.0,
                    pole / 2.0,
                    pole / 2.0,
                    -pole / 2.0,
                ],
                Vec::new(),
            );
            let band = geographic_index_envelopes(&band, native).unwrap();
            assert_eq!(band[0].lower(), [-half, -pole / 2.0]);
            assert_eq!(band[0].upper(), [half, pole / 2.0]);

            let hole = Ring::closed(vec![
                Point::new(0.0, pole * 3.0 / 4.0).unwrap(),
                Point::new(half * 2.0 / 3.0, pole * 3.0 / 4.0).unwrap(),
                Point::new(half * 4.0 / 3.0, pole * 3.0 / 4.0).unwrap(),
                Point::new(half * 2.0, pole * 3.0 / 4.0).unwrap(),
            ])
            .unwrap();
            let annulus = polygon(
                &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
                &[pole / 2.0; 4],
                vec![hole],
            );
            let annulus = geographic_index_envelopes(&annulus, native).unwrap();
            assert_eq!(annulus[0].lower(), [-half, pole / 2.0]);
            assert_eq!(annulus[0].upper()[0], half);
            assert_eq!(annulus[0].upper()[1], pole);
        }
    }
}
