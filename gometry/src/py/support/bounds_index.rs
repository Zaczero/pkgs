use rstar::Envelope as _;

use crate::geographic::{
    CircularInterval, GeographicAdmissionError, GeographicFrame, GeographicSupportEnvelope,
    GeographicViewError, LongitudeWindows,
};
use crate::geometry::{Shape, TopologyDomain};
use crate::predicates::engine::IndexEnvelope;
use crate::py::errors::InvalidGeometryError;
use crate::py::support::{AABB, Bounds, Point};

/// Keep each part in the index until extra windows stop paying for their tree
/// entries; then merge the least costly pair.
const MAX_COVER_WINDOWS: usize = 64;

/// Where a shape sits, as the index sees it.
#[derive(Clone, Debug)]
pub(crate) enum EnvelopeCover {
    /// One conservative cover serves every lookup.
    Whole(Vec<AABB<[f64; 2]>>),
    /// Intersecting queries can use scattered parts; containment still needs
    /// the whole-shape windows.
    Scattered {
        whole: Vec<AABB<[f64; 2]>>,
        parts: Vec<AABB<[f64; 2]>>,
    },
}

impl EnvelopeCover {
    /// Select query windows according to the relation being searched.
    pub(crate) fn windows(&self, policy: IndexEnvelope) -> &[AABB<[f64; 2]>] {
        match self {
            Self::Whole(whole) => whole,
            Self::Scattered { whole, parts } => match policy {
                IndexEnvelope::Intersecting => parts,
                IndexEnvelope::ContainedInQuery => whole,
            },
        }
    }

    /// Windows stored for an indexed row. Each part remains inside the whole
    /// cover, so containment and nearest-distance pruning stay conservative.
    pub(crate) fn entries(&self) -> &[AABB<[f64; 2]>] {
        match self {
            Self::Whole(whole) => whole,
            Self::Scattered { parts, .. } => parts,
        }
    }
}

pub(crate) fn bounds_envelope(bounds: Bounds) -> AABB<[f64; 2]> {
    AABB::from_bounds([bounds.minx(), bounds.miny()], [
        bounds.maxx(),
        bounds.maxy(),
    ])
}

/// Minimal conservative index windows in the coordinate system of `frame`.
/// Periodic coverage comes from the native geographic support owner.
pub(crate) fn index_envelopes(
    shape: &Shape,
    bounds: Bounds,
    domain: TopologyDomain,
) -> pyo3::PyResult<EnvelopeCover> {
    let planar = bounds_envelope(bounds);
    let whole = match domain {
        TopologyDomain::Cartesian => vec![planar],
        TopologyDomain::Geographic(frame) => geographic_index_envelopes(shape, frame)?,
    };
    let [only] = whole.as_slice() else {
        return Ok(EnvelopeCover::Whole(whole));
    };
    if *only != planar {
        return Ok(EnvelopeCover::Whole(whole));
    }
    Ok(match part_cover(shape, planar) {
        Some(parts) => EnvelopeCover::Scattered { whole, parts },
        None => EnvelopeCover::Whole(whole),
    })
}

/// Split disconnected multipart support when the tighter windows pay for their
/// additional tree entries.
fn part_cover(shape: &Shape, whole: AABB<[f64; 2]>) -> Option<Vec<AABB<[f64; 2]>>> {
    if !matches!(
        shape,
        Shape::MultiPoint(_)
            | Shape::MultiLineString(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
    ) {
        return None;
    }
    let mut cover = Vec::new();
    for_each_part(shape, &mut |part| admit(&mut cover, part));
    let covered: f64 = cover.iter().map(margin).sum();
    (cover.len() > 1 && covered < margin(&whole)).then_some(cover)
}

fn for_each_part(shape: &Shape, visit: &mut impl FnMut(AABB<[f64; 2]>)) {
    match shape {
        Shape::MultiPoint(points) => {
            for point in points {
                visit(bounds_envelope(Bounds::from_point(point)));
            }
        },
        Shape::MultiLineString(lines) => {
            for line in lines.iter() {
                if let Some(bounds) = Bounds::from_coords(line) {
                    visit(bounds_envelope(bounds));
                }
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                if let Some(bounds) = Bounds::from_coords(polygon.shell.coords()) {
                    visit(bounds_envelope(bounds));
                }
            }
        },
        Shape::GeometryCollection(members) => {
            for member in members {
                for_each_part(member, visit);
            }
        },
        Shape::Point(_) | Shape::LineString(_) | Shape::Polygon(_) | Shape::Empty(..) => {
            if let Some(bounds) = shape.bounds() {
                visit(bounds_envelope(bounds));
            }
        },
    }
}

/// Merge overlaps for free, then spend the smallest margin increase at the cap.
fn admit(cover: &mut Vec<AABB<[f64; 2]>>, part: AABB<[f64; 2]>) {
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
        let window = cover
            .get_mut(index)
            .expect("free merge index came from the cover");
        *window = window.merged(&part);
        return;
    }
    if cover.len() < MAX_COVER_WINDOWS {
        cover.push(part);
        return;
    }
    let (index, _) = cheapest.expect("a full cover holds a window to merge into");
    let window = cover
        .get_mut(index)
        .expect("cheapest merge index came from the cover");
    *window = window.merged(&part);
}

/// Half-perimeter margin keeps degenerate windows discriminating.
fn margin(envelope: &AABB<[f64; 2]>) -> f64 {
    let lower = envelope.lower();
    let upper = envelope.upper();
    (upper[0] - lower[0]) + (upper[1] - lower[1])
}

fn geographic_view_py_error(error: &GeographicViewError) -> pyo3::PyErr {
    InvalidGeometryError::new_err(error.to_string())
}

#[expect(
    clippy::float_cmp,
    reason = "exact canonical seam endpoints determine mirrored index windows"
)]
fn geographic_index_envelopes(
    shape: &Shape,
    frame: GeographicFrame,
) -> pyo3::PyResult<Vec<AABB<[f64; 2]>>> {
    let support =
        GeographicSupportEnvelope::try_from_shape(frame, shape).map_err(|error| match error {
            GeographicAdmissionError::Geographic(error) => geographic_view_py_error(&error),
            GeographicAdmissionError::Resource(error) => error.into(),
        })?;
    let Some((south, north)) = support.latitude_bounds() else {
        return Ok(Vec::new());
    };
    let longitude = if support.touches_pole(frame) {
        CircularInterval::Full
    } else {
        support.longitude
    };
    let mut envelopes = Vec::with_capacity(2);
    let half_period = frame.longitude().half_period();
    let mut push = |window: [f64; 2]| {
        envelopes.push(AABB::from_bounds([window[0], south], [window[1], north]));
    };
    match longitude.windows(frame.longitude()) {
        Some(LongitudeWindows::One(window)) => {
            let [west, east] = window;
            push(window);
            if west == -half_period && east != half_period {
                push([half_period, half_period]);
            } else if east == half_period && west != -half_period {
                push([-half_period, -half_period]);
            }
        },
        Some(LongitudeWindows::Two(left, right)) => {
            push(left);
            push(right);
        },
        Some(LongitudeWindows::Full) => {
            push([-half_period, half_period]);
        },
        None => {},
    }
    Ok(envelopes)
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
mod tests {
    use super::*;
    use crate::geographic::{AngularFactor, GeographicError};
    use crate::geometry::{CoordSeq, CoordinateAxes, LineSeq, MultiMember, MultiPoint};

    fn frame() -> GeographicFrame {
        frame_with_unit(std::f64::consts::PI / 180.0)
    }

    fn frame_with_unit(radians_per_unit: f64) -> GeographicFrame {
        let unit = AngularFactor::try_new("test unit", radians_per_unit).unwrap();
        GeographicFrame::try_new(unit, unit, 0.0).unwrap()
    }

    fn native_frames() -> [GeographicFrame; 3] {
        [
            frame_with_unit(std::f64::consts::PI / 180.0),
            frame_with_unit(std::f64::consts::TAU / 400.0),
            frame_with_unit(1.0),
        ]
    }

    fn assert_close(actual: f64, expected: f64, period: f64) {
        assert!(
            (actual - expected).abs() <= period * 2.0e-14,
            "{actual} != {expected}"
        );
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

    fn line(xs: &[f64], ys: &[f64]) -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(xs.to_vec(), ys.to_vec(), None, None).unwrap(),
            )
            .unwrap(),
        )
    }

    fn window_bits(windows: &[AABB<[f64; 2]>]) -> Vec<([u64; 2], [u64; 2])> {
        windows
            .iter()
            .map(|window| {
                let [lower_x, lower_y] = window.lower();
                let [upper_x, upper_y] = window.upper();
                ([lower_x.to_bits(), lower_y.to_bits()], [
                    upper_x.to_bits(),
                    upper_y.to_bits(),
                ])
            })
            .collect()
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn shared_invalid_index_owner_keeps_its_original_diagnostic_path() {
        let frame = frame();
        let shared_bad_owner = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::Point(Point::new_raw_xy(f64::INFINITY, 100.0)),
        ])
        .unwrap();
        let shape = Shape::geometry_collection(vec![
            Shape::geometry_collection(vec![shared_bad_owner.clone()]).unwrap(),
            Shape::geometry_collection(vec![shared_bad_owner]).unwrap(),
        ])
        .unwrap();
        let Err(GeographicAdmissionError::Geographic(error)) =
            GeographicSupportEnvelope::try_from_shape(frame, &shape)
        else {
            panic!("invalid coordinates retain their geographic admission diagnostic");
        };
        assert_eq!(error.location.members.as_slice(), [0, 0, 1]);
        assert!(matches!(
            error.source,
            GeographicError::NonFinite {
                name: "longitude",
                value
            } if value == f64::INFINITY
        ));

        pyo3::Python::initialize();
        let public_error = pyo3::Python::attach(|_| {
            geographic_index_envelopes(&shape, frame)
                .expect_err("index admission reports the original invalid coordinate")
                .to_string()
        });
        assert!(public_error.contains("member 0, member 0, member 1"));
        assert!(public_error.contains("longitude"));
    }

    #[test]
    fn pole_touching_index_windows_are_conservatively_widened() {
        for frame in native_frames() {
            let half = frame.longitude().half_period();
            let pole = frame.pole();
            for latitude in [pole, pole.next_up(), -pole, (-pole).next_down()] {
                let point = Shape::Point(Point::new(half / 3.0, latitude).unwrap());
                let windows = geographic_index_envelopes(&point, frame).unwrap();
                assert_eq!(windows.len(), 1);
                assert_eq!(windows[0].lower(), [-half, latitude.signum() * pole]);
                assert_eq!(windows[0].upper(), [half, latitude.signum() * pole]);
            }
            for latitude in [pole.next_up().next_up(), (-pole).next_down().next_down()] {
                let point = Shape::Point(Point::new_raw_xy(0.0, latitude));
                let _error = geographic_index_envelopes(&point, frame)
                    .expect_err("two pole ULPs outside the axis are rejected");
            }
        }
    }

    #[test]
    fn periodic_index_windows_collapse_aliases_and_split_seam_neighbors() {
        for frame in native_frames() {
            let axis = frame.longitude();
            let period = axis.period();
            let aliases =
                geographic_index_envelopes(&multipoint(&[(0.0, 0.0), (period, 0.0)]), frame)
                    .unwrap();
            assert_eq!(aliases.len(), 1);
            assert_eq!(aliases[0].lower(), [0.0, 0.0]);
            assert_eq!(aliases[0].upper(), [0.0, 0.0]);

            let unit = axis.half_period() / 180.0;
            let seam = geographic_index_envelopes(
                &multipoint(&[(-179.0 * unit, 0.0), (179.0 * unit, 0.0)]),
                frame,
            )
            .unwrap();
            assert_eq!(seam.len(), 2);
            for window in seam {
                assert_close(window.upper()[0] - window.lower()[0], unit, period);
            }
        }
    }

    #[test]
    fn periodic_index_window_keeps_the_authentic_terminal_endpoint() {
        let frame = frame();
        let shape = multipoint(&[(0.1, 0.0), (0.2, 0.0)]);
        let windows = geographic_index_envelopes(&shape, frame).unwrap();
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].lower()[0].to_bits(), 0.1_f64.to_bits());
        assert_eq!(windows[0].upper()[0].to_bits(), 0.2_f64.to_bits());
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "exact canonical seam endpoints determine the mirrored candidate window"
    )]
    fn seam_touching_segment_keeps_a_mirrored_endpoint_window() {
        for frame in native_frames() {
            let axis = frame.longitude();
            let half = axis.half_period();
            let unit = half / 180.0;
            let shape = line(&[half * 0.75, half, half, half * 0.75, half * 0.75], &[
                0.0, 0.0, unit, unit, 0.0,
            ]);
            let windows = geographic_index_envelopes(&shape, frame).unwrap();
            assert_eq!(windows.len(), 2);
            assert!(
                windows.iter().any(|window| {
                    window.lower()[0] == half * 0.75 && window.upper()[0] == half
                })
            );
            assert!(
                windows
                    .iter()
                    .any(|window| { window.lower()[0] == -half && window.upper()[0] == -half })
            );
        }
    }

    #[rstest::rstest]
    #[case::depth_16_usize(16_usize)]
    #[case::depth_128(128)]
    fn geographic_index_windows_stay_linear_on_shared_dags(#[case] depth: usize) {
        let frame = frame();
        let base = line(&[170.0, -170.0], &[0.0, 1.0]);
        let expected = window_bits(&geographic_index_envelopes(&base, frame).unwrap());

        let mut shared = base;
        for _ in 0..depth {
            shared = Shape::geometry_collection(vec![shared.clone(), shared])
                .expect("nested shared collection stays within the depth limit");
        }
        assert_eq!(
            window_bits(&geographic_index_envelopes(&shared, frame).unwrap()),
            expected
        );
    }

    #[test]
    fn shared_nested_collections_match_expanded_primitive_occurrences() {
        let frame = frame();
        let shared_members = || {
            vec![
                multipoint(&[(-179.0, 0.0), (179.0, 0.0)]),
                line(&[170.0, -170.0], &[5.0, 5.0]),
            ]
        };
        let shared = Shape::geometry_collection(shared_members()).unwrap();
        let left = Shape::geometry_collection(vec![
            shared.clone(),
            Shape::Point(Point::new_raw_xy(-30.0, 0.0)),
        ])
        .unwrap();
        let right =
            Shape::geometry_collection(vec![Shape::Point(Point::new_raw_xy(50.0, 0.0)), shared])
                .unwrap();
        let repeated = Shape::geometry_collection(vec![left, right]).unwrap();
        let expanded = Shape::geometry_collection(vec![
            Shape::geometry_collection(vec![
                Shape::geometry_collection(shared_members()).unwrap(),
                Shape::Point(Point::new_raw_xy(-30.0, 0.0)),
            ])
            .unwrap(),
            Shape::geometry_collection(vec![
                Shape::Point(Point::new_raw_xy(50.0, 0.0)),
                Shape::geometry_collection(shared_members()).unwrap(),
            ])
            .unwrap(),
        ])
        .unwrap();
        assert_eq!(
            window_bits(&geographic_index_envelopes(&repeated, frame).unwrap()),
            window_bits(&geographic_index_envelopes(&expanded, frame).unwrap())
        );
    }

    #[test]
    fn part_cover_coalesces_overlapping_parts_before_splitting_distant_support() {
        let shape = Shape::geometry_collection(vec![
            line(&[0.0, 1.0], &[0.0, 1.0]),
            line(&[0.0, 1.0], &[0.0, 1.0]),
            line(&[1000.0, 1001.0], &[1000.0, 1001.0]),
        ])
        .unwrap();
        let whole = bounds_envelope(shape.bounds().unwrap());
        let parts = part_cover(&shape, whole).expect("distant parts benefit from a cover");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].lower(), [0.0, 0.0]);
        assert_eq!(parts[0].upper(), [1.0, 1.0]);
    }

    #[test]
    fn part_cover_replays_repeated_owners_in_source_order() {
        let repeated_member = line(&[10.0, 10.0], &[10.0, 11.0]);
        let shared = Shape::geometry_collection(vec![repeated_member]).unwrap();
        let shape = Shape::geometry_collection(vec![
            line(&[0.0, 0.0], &[0.0, 1.0]),
            shared.clone(),
            line(&[0.0, 9.5], &[0.0, 11.0]),
            shared,
            Shape::Point(Point::new_raw_xy(1000.0, 1000.0)),
        ])
        .unwrap();
        let whole = bounds_envelope(shape.bounds().unwrap());
        let parts = part_cover(&shape, whole).expect("distant parts benefit from a cover");
        assert_eq!(parts[0].lower(), [0.0, 0.0]);
        assert_eq!(parts[0].upper(), [10.0, 11.0]);
    }
}
