//! Shared topology staging and membership primitives.
use std::ops::Range;
use std::simd::cmp::SimdPartialOrd as _;
use std::simd::num::SimdFloat as _;

use crate::geometry::access::crossover::RAY_CROSSING;
use crate::geometry::{
    CCW_ERRBOUND_A, Point, PointKey, REDUCE_LANES, ReduceSimd, XY, ray_crossing_is_right,
    simd_reduce_f64,
};

/// Coordinate domain of a topology operation. Geographic operations carry
/// the frame that defines their longitude quotient and poles.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum TopologyDomain {
    Cartesian,
    Geographic(crate::geographic::GeographicFrame),
}

impl TopologyDomain {
    pub(crate) const fn is_geographic(self) -> bool {
        matches!(self, Self::Geographic(_))
    }
}

/// Identity of a stored vertex in a topology operation's coordinate domain.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum VertexKey {
    Cartesian(PointKey),
    Geographic(crate::geographic::QuotientVertex),
}

impl VertexKey {
    pub(crate) fn new(point: Point, domain: TopologyDomain) -> Self {
        match domain {
            TopologyDomain::Cartesian => Self::Cartesian(PointKey::new(point)),
            TopologyDomain::Geographic(frame) => {
                Self::Geographic(frame.quotient_vertex(point.x, point.y))
            },
        }
    }
}

mod exact;
pub(in crate::geometry) use exact::{
    ExactChartPoint, point_on_exact_segment_with_work, vector_cross_with_work,
    vector_difference_with_work,
};
mod emission;
pub(in crate::geometry) use emission::{
    EmissionPortId, FeatureFamilyRegistry, PreparedNativeSource, QSourceImage, SourceTrace,
};
mod coverage;
pub(in crate::geometry) mod linework;
pub(in crate::geometry) mod overlay;
pub(crate) use coverage::{
    GeographicCoverageAnalysis, analyze_geographic_coverage, analyze_geographic_coverage_with_work,
};
mod q;
pub(in crate::geometry) use q::exact_segments_intersect_with_work;
pub(in crate::geometry) mod q_embedding;
pub(crate) use q::{ExactGeographicRectangleLocation, ExactGeographicTopology};

/// SIMD ray-crossing decision for one chunk of ring edges, or `None` when any
/// straddling lane's crossing sign cannot be trusted in plain `f64` (the
/// caller re-runs that chunk through the exact scalar path).
///
/// The old division form materialized the intersection X, so opposite-sign
/// huge coordinates overflowed `bx - ax` to infinity and corrupted the parity
/// (direction-dependent wrong `contains` near |x| ~ 1e308). The sign form
/// evaluates `orient2d(a, b, p)` per lane and trusts it only past the
/// all-input Ozaki filter, using Shewchuk's conservative A-stage constant:
/// straddling lanes with
/// `|det| <= errbound * (|t1| + |t2| + MIN_POSITIVE)`
/// (ties, near-degenerate, overflowed, or NaN terms all land here) demote the
/// whole chunk to [`ray_crossing_is_right`], mirroring the guarded-sqrt
/// chunk-rescue pattern in the length kernels. Division-free, so the fast
/// path also drops the one `vdivpd` per chunk.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the certified Ozaki filter retains its evaluation grouping; any untrusted straddling lane demotes the whole chunk to exact scalar crossing"
)]
fn ray_crossing_lanes(
    ax: ReduceSimd,
    ay: ReduceSimd,
    bx: ReduceSimd,
    by: ReduceSimd,
    px: ReduceSimd,
    py: ReduceSimd,
) -> Option<std::simd::Mask<i64, REDUCE_LANES>> {
    let straddles = ay.simd_gt(py) ^ by.simd_gt(py);
    let t1 = (bx - ax) * (py - ay);
    let t2 = (px - ax) * (by - ay);
    let det = t1 - t2;
    let trusted = det.abs().simd_gt(
        ((t1.abs() + t2.abs()) + ReduceSimd::splat(f64::MIN_POSITIVE))
            * ReduceSimd::splat(CCW_ERRBOUND_A),
    );
    if (straddles & !trusted).any() {
        return None;
    }
    let upward = by.simd_gt(ay);
    let zero = ReduceSimd::splat(0.0);
    Some(straddles & ((upward & det.simd_gt(zero)) | (!upward & det.simd_lt(zero))))
}

/// SIMD even-odd ray-crossing parity over a CLOSED ring's `SoA` columns — `xs`
/// and `ys` carry the repeated closing vertex, so consecutive pairs are exactly
/// the ring edges. Native columns load DIRECTLY (`Simd::from_slice`, no
/// per-lane gather — unlike the `&[XY]` `AoS` path), making this the fast lane
/// for any `Coordinates`-backed point-in-ring. Counting crossings is
/// order-independent, so the SIMD body + scalar tail give parity bit-identical
/// to the scalar toggle loop (same per-edge `f64` math in every lane).
pub(super) fn ring_contains_interior_columns(xs: &[f64], ys: &[f64], point: XY) -> bool {
    let segments = xs.len().saturating_sub(1);
    if segments < RAY_CROSSING {
        return ring_crossing_columns_scalar(xs, ys, point, 0..segments) % 2 == 1;
    }
    let px = ReduceSimd::splat(point.x);
    let py = ReduceSimd::splat(point.y);
    let crossings = simd_reduce_f64(
        segments,
        (),
        0_u32,
        |(), crossings, start| {
            #[expect(
                clippy::indexing_slicing,
                reason = "simd_reduce_f64 supplies a full chunk within the admitted parallel x/y columns"
            )]
            let ax = ReduceSimd::from_slice(&xs[start..start.wrapping_add(REDUCE_LANES)]);
            #[expect(
                clippy::indexing_slicing,
                reason = "simd_reduce_f64 supplies a full chunk within the admitted parallel x/y columns"
            )]
            let ay = ReduceSimd::from_slice(&ys[start..start.wrapping_add(REDUCE_LANES)]);
            #[expect(
                clippy::indexing_slicing,
                reason = "the shifted full chunk follows the unshifted driver range within the admitted parallel x/y columns"
            )]
            let bx = ReduceSimd::from_slice(
                &xs[start.wrapping_add(1)..start.wrapping_add(1).wrapping_add(REDUCE_LANES)],
            );
            #[expect(
                clippy::indexing_slicing,
                reason = "the shifted full chunk follows the unshifted driver range within the admitted parallel x/y columns"
            )]
            let by = ReduceSimd::from_slice(
                &ys[start.wrapping_add(1)..start.wrapping_add(1).wrapping_add(REDUCE_LANES)],
            );
            ray_crossing_lanes(ax, ay, bx, by, px, py).map_or_else(
                || {
                    (
                        (),
                        crossings.wrapping_add(ring_crossing_columns_scalar(
                            xs,
                            ys,
                            point,
                            start..start.wrapping_add(REDUCE_LANES),
                        )),
                    )
                },
                |crosses| {
                    (
                        (),
                        crossings.wrapping_add(crosses.to_bitmask().count_ones()),
                    )
                },
            )
        },
        |crossings, range| {
            crossings.wrapping_add(ring_crossing_columns_scalar(xs, ys, point, range))
        },
        |(), crossings| crossings,
    );
    crossings % 2 == 1
}

// Callers pass a range of admitted parallel CoordSeq columns. Their i32 vertex
// bound keeps this exact crossing count below u32::MAX.
fn ring_crossing_columns_scalar(xs: &[f64], ys: &[f64], point: XY, range: Range<usize>) -> u32 {
    if range.is_empty() {
        return 0;
    }
    let xs = xs
        .get(range.start..=range.end)
        .expect("ring edge range fits the x column");
    let ys = ys
        .get(range.start..=range.end)
        .expect("ring edge range fits the y column");
    let mut crossings = 0_u32;
    for (&[ax, bx], &[ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
        if (ay > point.y) != (by > point.y)
            && ray_crossing_is_right(ax, ay, bx, by, point.x, point.y)
        {
            crossings = crossings.wrapping_add(1);
        }
    }
    crossings
}

/// SIMD ray-crossing count PLUS a boundary-candidate flag over a CLOSED ring's
/// `SoA` columns — the fast lane behind `ring_classify_point` (the
/// `contains`/`within`/`covers` predicate). The flag is set iff the point lies
/// inside SOME edge's bounding box, the necessary precondition for an exact
/// boundary hit; the caller resolves those rare near-edge cases with the exact
/// `orientation_xy` test. Far-from-boundary points (the common case) take this
/// pure-SIMD path: the crossing parity decides interior/exterior with the same
/// sign-form decision as the scalar loop (see [`ray_crossing_lanes`]), and the
/// boundary envelope is four packed compares per lane.
pub(super) fn ring_classify_crossings_columns(xs: &[f64], ys: &[f64], point: XY) -> (u32, bool) {
    let segments = xs.len().saturating_sub(1);
    if segments < RAY_CROSSING {
        return ring_classify_crossings_scalar(xs, ys, point, 0..segments);
    }
    let px = ReduceSimd::splat(point.x);
    let py = ReduceSimd::splat(point.y);
    simd_reduce_f64(
        segments,
        (),
        (0_u32, false),
        |(), (crossings, candidate), start| {
            #[expect(
                clippy::indexing_slicing,
                reason = "simd_reduce_f64 supplies a full chunk within the admitted parallel x/y columns"
            )]
            let ax = ReduceSimd::from_slice(&xs[start..start.wrapping_add(REDUCE_LANES)]);
            #[expect(
                clippy::indexing_slicing,
                reason = "simd_reduce_f64 supplies a full chunk within the admitted parallel x/y columns"
            )]
            let ay = ReduceSimd::from_slice(&ys[start..start.wrapping_add(REDUCE_LANES)]);
            #[expect(
                clippy::indexing_slicing,
                reason = "the shifted full chunk follows the unshifted driver range within the admitted parallel x/y columns"
            )]
            let bx = ReduceSimd::from_slice(
                &xs[start.wrapping_add(1)..start.wrapping_add(1).wrapping_add(REDUCE_LANES)],
            );
            #[expect(
                clippy::indexing_slicing,
                reason = "the shifted full chunk follows the unshifted driver range within the admitted parallel x/y columns"
            )]
            let by = ReduceSimd::from_slice(
                &ys[start.wrapping_add(1)..start.wrapping_add(1).wrapping_add(REDUCE_LANES)],
            );
            // Boundary candidate: point inside this edge's bbox (four packed
            // compares); collapse the `REDUCE_LANES` lanes with `.any()`.
            let in_x = px.simd_ge(ax.simd_min(bx)) & px.simd_le(ax.simd_max(bx));
            let in_y = py.simd_ge(ay.simd_min(by)) & py.simd_le(ay.simd_max(by));
            let near = (in_x & in_y).any();
            // Crossing parity (same sign-form decision as the scalar loop).
            ray_crossing_lanes(ax, ay, bx, by, px, py).map_or_else(
                || {
                    let (c, n) = ring_classify_crossings_scalar(
                        xs,
                        ys,
                        point,
                        start..start.wrapping_add(REDUCE_LANES),
                    );
                    ((), (crossings.wrapping_add(c), candidate || near || n))
                },
                |crosses| {
                    (
                        (),
                        (
                            crossings.wrapping_add(crosses.to_bitmask().count_ones()),
                            candidate || near,
                        ),
                    )
                },
            )
        },
        |(crossings, candidate), range| {
            let (c, near) = ring_classify_crossings_scalar(xs, ys, point, range);
            (crossings.wrapping_add(c), candidate || near)
        },
        |(), acc| acc,
    )
}

// The range is either the whole small ring or a disjoint driver portion of its
// admitted parallel CoordSeq columns, so the i32 vertex bound keeps its count exact.
fn ring_classify_crossings_scalar(
    xs: &[f64],
    ys: &[f64],
    point: XY,
    range: Range<usize>,
) -> (u32, bool) {
    if range.is_empty() {
        return (0, false);
    }
    let xs = xs
        .get(range.start..=range.end)
        .expect("ring edge range fits the x column");
    let ys = ys
        .get(range.start..=range.end)
        .expect("ring edge range fits the y column");
    let mut crossings = 0_u32;
    let mut candidate = false;
    for (&[ax, bx], &[ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
        if point.x >= ax.min(bx)
            && point.x <= ax.max(bx)
            && point.y >= ay.min(by)
            && point.y <= ay.max(by)
        {
            candidate = true;
        }
        if (ay > point.y) != (by > point.y)
            && ray_crossing_is_right(ax, ay, bx, by, point.x, point.y)
        {
            crossings = crossings.wrapping_add(1);
        }
    }
    (crossings, candidate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::CoordSeq;
    use crate::geometry::predicates::{RingClass, ring_classify_point, ring_contains_interior};

    /// The closed-column membership kernel over an open ring's vertices.
    fn ring_contains_interior_open(ring: &[XY], point: XY) -> bool {
        let (xs, ys) = closed_columns(ring);
        ring_contains_interior_columns(&xs, &ys, point)
    }

    fn closed_columns(ring: &[XY]) -> (Vec<f64>, Vec<f64>) {
        let first = ring.first().expect("test rings are non-empty");
        let xs = ring
            .iter()
            .chain(std::iter::once(first))
            .map(|vertex| vertex.x)
            .collect();
        let ys = ring
            .iter()
            .chain(std::iter::once(first))
            .map(|vertex| vertex.y)
            .collect();
        (xs, ys)
    }

    fn scalar_reference(ring: &[XY], point: XY) -> bool {
        let first = ring.first().expect("test rings are non-empty");
        let closed: Vec<XY> = ring
            .iter()
            .copied()
            .chain(std::iter::once(*first))
            .collect();
        ring_contains_interior(closed.as_slice(), point)
    }

    fn classify_columns(xs: &[f64], ys: &[f64], point: XY) -> RingClass {
        let points: Vec<XY> = xs
            .iter()
            .copied()
            .zip(ys.iter().copied())
            .map(|(x, y)| XY::new(x, y))
            .collect();
        let coordinates = CoordSeq::try_from_xy(&points).expect("test columns are parallel");
        ring_classify_point(&coordinates, point.point())
    }

    fn assert_columns_match_scalar_reference(ring: &[XY], probes: &[XY]) {
        for &point in probes {
            assert_eq!(
                ring_contains_interior_open(ring, point),
                scalar_reference(ring, point),
                "ring={ring:?} point={point:?}",
            );
        }
    }

    fn assert_simd_matches_scalar_reference(ring: &[XY], probes: &[XY]) {
        assert!(
            ring.len() >= RAY_CROSSING,
            "test ring must reach the ray-crossing SIMD crossover",
        );
        assert_columns_match_scalar_reference(ring, probes);
    }

    fn edge_probes(ring: &[XY]) -> Vec<XY> {
        let mut probes = Vec::with_capacity(
            ring.len()
                .checked_mul(4)
                .expect("each test-ring edge produces four probes"),
        );
        for (a, b) in ring
            .iter()
            .copied()
            .zip(ring.iter().copied().cycle().skip(1))
            .take(ring.len())
        {
            let mid = XY::new(a.x.midpoint(b.x), a.y.midpoint(b.y));
            probes.push(a);
            probes.push(XY::new(a.x - 10.0, a.y));
            probes.push(mid);
            probes.push(XY::new(mid.x - 10.0, mid.y));
        }
        probes
    }

    #[test]
    fn open_ring_columns_match_scalar_reference_on_adversarial_rings() {
        let rings: Vec<Vec<XY>> = vec![
            vec![
                XY::new(0.0, 0.0),
                XY::new(4.0, 0.0),
                XY::new(8.0, 0.0),
                XY::new(8.0, 3.0),
                XY::new(6.0, 3.0),
                XY::new(6.0, 6.0),
                XY::new(2.0, 6.0),
                XY::new(2.0, 3.0),
                XY::new(0.0, 3.0),
            ],
            vec![
                XY::new(0.0, 0.0),
                XY::new(5.0, 5.0),
                XY::new(10.0, 0.0),
                XY::new(7.0, 0.0),
                XY::new(7.0, -4.0),
                XY::new(3.0, -4.0),
                XY::new(3.0, 0.0),
                XY::new(10.0, 5.0),
                XY::new(0.0, 5.0),
            ],
            vec![
                XY::new(-5.0, -1.0),
                XY::new(-2.0, -1.0),
                XY::new(-2.0, 2.0),
                XY::new(1.0, 2.0),
                XY::new(1.0, -1.0),
                XY::new(4.0, -1.0),
                XY::new(4.0, 4.0),
                XY::new(-5.0, 4.0),
            ],
        ];
        for ring in rings {
            let mut probes = edge_probes(&ring);
            probes.extend([
                XY::new(1.0, 1.0),
                XY::new(3.0, 0.0),
                XY::new(6.0, 0.0),
                XY::new(6.0, 3.0),
                XY::new(100.0, 0.0),
                XY::new(-100.0, 0.0),
            ]);
            assert_columns_match_scalar_reference(&ring, &probes);
        }
    }

    #[test]
    fn open_ring_columns_match_scalar_reference_on_review_divergences() {
        let huge = vec![
            XY::new(0.0, -1e155),
            XY::new(1e155, 1e155),
            XY::new(5e154, 1e155),
            XY::new(0.0, 1e155),
            XY::new(-5e154, 1e155),
            XY::new(-1e155, 1e155),
            XY::new(-1e155, 0.0),
            XY::new(-1e155, -1e155),
        ];
        let huge_probe = XY::new(1e154, 0.0);
        assert!(scalar_reference(&huge, huge_probe));
        assert_eq!(
            ring_contains_interior_open(&huge, huge_probe),
            scalar_reference(&huge, huge_probe),
        );

        let on_edge = vec![
            XY::new(0.0, 1.0),
            XY::new(0.0, -1.0),
            XY::new(1.0, -1.0),
            XY::new(1.0, -0.5),
            XY::new(1.0, 0.0),
            XY::new(1.0, 0.5),
            XY::new(1.0, 1.0),
            XY::new(0.5, 1.0),
        ];
        let on_edge_probe = XY::new(0.0, 0.0);
        assert!(scalar_reference(&on_edge, on_edge_probe));
        assert_eq!(
            ring_contains_interior_open(&on_edge, on_edge_probe),
            scalar_reference(&on_edge, on_edge_probe),
        );
    }

    /// The `(bx - ax) -> inf` overflow class: a triangle with opposite-sign
    /// coordinates at the top of the finite range used to give
    /// DIRECTION-DEPENDENT wrong parity (the division form materialized an
    /// infinite intersection X, counting or dropping a genuine crossing by
    /// edge order). The sign-form decision must classify exactly like the
    /// same triangle scaled to a safe magnitude, in both vertex orders.
    #[test]
    fn ray_crossing_parity_is_scale_and_direction_stable_at_extreme_coordinates() {
        for scale in [1e307_f64, 1e308_f64] {
            let s = scale / 1e308;
            let triangle = [
                XY::new(-1e308 * s, -1.0),
                XY::new(1e308 * s, 1.0),
                XY::new(1e308 * s, -1.0),
            ];
            let mut reversed = triangle;
            reversed.reverse();
            // (0, 0.5) sits ABOVE the long diagonal (its y at x=0 is ~0):
            // exterior. (5e307 * s, 0.0) sits below it and above the bottom
            // edge: interior.
            let exterior = [XY::new(0.0, 0.5), XY::new(-1e308 * s, 0.9999)];
            let interior = [XY::new(5e307 * s, 0.0)];
            for ring in [&triangle, &reversed] {
                for probe in exterior {
                    assert!(
                        !ring_contains_interior_open(ring, probe),
                        "scale={scale:e} ring={ring:?} probe={probe:?} must be exterior",
                    );
                }
                for probe in interior {
                    assert!(
                        ring_contains_interior_open(ring, probe),
                        "scale={scale:e} ring={ring:?} probe={probe:?} must be interior",
                    );
                }
            }
        }
    }

    /// The filter must rescue a whole chunk containing an overflowing
    /// straddling edge, both in the first chunk and after trusted chunks.
    #[test]
    fn ray_crossing_simd_rescues_first_and_later_chunks_before_the_tail() {
        let n = 300_usize;
        let mut first_chunk = vec![XY::new(-1e308, -1.0), XY::new(1e308, 1.0)];
        // Bottom edge from (1e308, -1) back towards (-1e308, -1), densified.
        for i in 0..n {
            let fraction = (i as f64) / (n as f64);
            first_chunk.push(XY::new(1e308 * (1.0 - 2.0 * fraction), -1.0));
        }
        let mut later_chunk = first_chunk.clone();
        later_chunk.rotate_left(REDUCE_LANES.wrapping_mul(3));
        let probes = [
            (XY::new(0.0, 0.5), 2, true, false),
            (XY::new(5e307, 0.0), 1, true, true),
            // Above the diagonal (its y at x = -5e307 is exactly -0.5).
            (XY::new(-5e307, -0.25), 2, true, false),
            // Below the diagonal, above the bottom edge.
            (XY::new(-5e307, -0.75), 1, true, true),
            (XY::new(0.0, 2.0), 0, false, false),
        ];
        for (name, ring) in [("first chunk", first_chunk), ("later chunk", later_chunk)] {
            let (xs, ys) = closed_columns(&ring);
            let segments = xs.len().saturating_sub(1);
            assert!(segments >= RAY_CROSSING, "{name} reaches SIMD crossover");
            assert_ne!(segments % REDUCE_LANES, 0, "{name} has a scalar tail");
            let reference_probes = probes.map(|(point, ..)| point);
            assert_columns_match_scalar_reference(&ring, &reference_probes);
            for (probe, expected_count, expected_candidate, expected_inside) in probes {
                assert_eq!(
                    ring_contains_interior_open(&ring, probe),
                    expected_inside,
                    "{name} parity probe={probe:?}",
                );
                assert_eq!(
                    ring_classify_crossings_columns(&xs, &ys, probe),
                    (expected_count, expected_candidate),
                    "{name} crossing count and bbox probe={probe:?}",
                );
            }
        }
    }

    #[test]
    fn ray_crossing_simd_declines_wrong_nonzero_underflow_sign() {
        let mu = f64::from_bits(1);
        let splat = ReduceSimd::splat;
        assert!(
            ray_crossing_lanes(
                splat(1.0),
                splat(-4096.0 * mu),
                splat(-(2.0_f64.powi(-12))),
                splat(6144.0 * mu),
                splat(f64::from_bits(0x3FD9_9733_3333_3333)),
                splat(2048.0 * mu),
            )
            .is_none()
        );
    }

    #[test]
    fn open_ring_simd_matches_scalar_reference_on_curated_rings() {
        let base_ring = |n: usize| {
            (0..n)
                .map(|i| {
                    let phase = ((i * 17) % 11) as f64 * 0.01 - 0.05;
                    let angle = (i as f64 + phase) * std::f64::consts::TAU / n as f64;
                    let radius = 8.0 + ((i * 37) % 19) as f64;
                    XY::new(angle.cos() * radius, angle.sin() * radius)
                })
                .collect::<Vec<_>>()
        };
        let mut rings = Vec::new();
        rings.push(("n256", base_ring(256), false, false));
        rings.push(("n263_tail", base_ring(263), false, false));
        let mut rotated_reversed = base_ring(257);
        rotated_reversed.rotate_left(37);
        rotated_reversed.reverse();
        rings.push(("n257_rotated_reversed", rotated_reversed, false, false));
        let mut swapped = base_ring(264);
        swapped.swap(1, 132);
        swapped.swap(2, 133);
        rings.push(("n264_swapped", swapped, false, false));
        let mut horizontal = base_ring(265);
        horizontal[3].y = horizontal[2].y;
        horizontal[4].y = horizontal[2].y;
        rings.push(("n265_horizontal", horizontal, false, false));
        let mut huge = base_ring(266);
        for point in &mut huge {
            point.x *= 1e150;
            point.y *= 1e150;
        }
        rings.push(("n266_huge", huge, true, false));
        let mut rounded_vertical = base_ring(267);
        for point in &mut rounded_vertical {
            point.x = point.x.round();
            point.y = point.y.round();
        }
        rounded_vertical[1].x = rounded_vertical[0].x;
        rounded_vertical[1].y = rounded_vertical[0].y - 2.0;
        rings.push(("n267_rounded_vertical", rounded_vertical, false, true));
        let mut combined = base_ring(271);
        combined.reverse();
        combined.swap(1, 135);
        combined.swap(2, 136);
        combined[3].y = combined[2].y;
        combined[4].y = combined[2].y;
        for point in &mut combined {
            point.x = (point.x * 1e150).round();
            point.y = (point.y * 1e150).round();
        }
        combined[1].x = combined[0].x;
        combined[1].y = combined[0].y - 2e150;
        rings.push(("n271_combined", combined, true, true));

        for (name, ring, huge, vertical) in rings {
            assert!(
                ring.len() >= RAY_CROSSING,
                "{name} must exercise the ray-crossing SIMD crossover",
            );
            let mut probes = edge_probes(&ring);
            if huge {
                probes.extend([
                    XY::new(0.0, 0.0),
                    XY::new(1e151, 1e151),
                    XY::new(-1e151, -1e151),
                    XY::new(7e150, -1.1e151),
                ]);
            } else {
                probes.extend([
                    XY::new(0.0, 0.0),
                    XY::new(32.0, 32.0),
                    XY::new(-32.0, -32.0),
                    XY::new(7.0, -11.0),
                ]);
            }
            if vertical {
                probes.push(XY::new(ring[0].x, ring[0].y.midpoint(ring[1].y)));
            }
            assert_simd_matches_scalar_reference(&ring, &probes);
        }
    }

    #[test]
    fn ring_columns_keep_scalar_windows_counts_and_bbox_refinement() {
        let corners = [
            XY::new(10.0, 20.0),
            XY::new(17.0, 20.0),
            XY::new(16.0, 25.0),
            XY::new(12.0, 24.0),
        ];
        let probes = [
            (XY::new(13.0, 22.0), 1, false, RingClass::Interior, true),
            (XY::new(18.0, 22.0), 0, false, RingClass::Exterior, false),
            (XY::new(11.5, 22.0), 1, true, RingClass::Interior, true),
            (XY::new(11.0, 22.0), 1, true, RingClass::Boundary, true),
            (XY::new(10.0, 22.0), 2, true, RingClass::Exterior, false),
        ];

        for vertex_count in [corners.len(), RAY_CROSSING, RAY_CROSSING.wrapping_add(3)] {
            let padding = vertex_count.wrapping_sub(corners.len());
            let mut ring = Vec::with_capacity(vertex_count);
            ring.push(corners[0]);
            ring.extend(std::iter::repeat_n(corners[0], padding));
            ring.extend_from_slice(&corners[1..]);
            assert_eq!(ring.len(), vertex_count);

            let (xs, ys) = closed_columns(&ring);
            for (probe, expected_count, expected_candidate, expected_class, expected_inside) in
                probes
            {
                assert_eq!(
                    ring_classify_crossings_columns(&xs, &ys, probe),
                    (expected_count, expected_candidate),
                    "vertices={vertex_count} probe={probe:?}",
                );
                assert_eq!(
                    classify_columns(&xs, &ys, probe),
                    expected_class,
                    "vertices={vertex_count} probe={probe:?}",
                );
                assert_eq!(
                    ring_contains_interior_open(&ring, probe),
                    expected_inside,
                    "vertices={vertex_count} probe={probe:?}",
                );
                assert_eq!(
                    ring_contains_interior_open(&ring, probe),
                    scalar_reference(&ring, probe),
                    "vertices={vertex_count} scalar differential probe={probe:?}",
                );
            }
        }
    }

    #[test]
    fn ring_classify_crossings_returns_zero_for_empty_and_single_vertex_columns() {
        for (xs, ys) in [(&[][..], &[][..]), (&[3.0][..], &[4.0][..])] {
            let point = XY::new(0.0, 0.0);
            assert!(!ring_contains_interior_columns(xs, ys, point));
            assert_eq!(ring_classify_crossings_columns(xs, ys, point), (0, false),);
            assert_eq!(ring_crossing_columns_scalar(xs, ys, point, 0..0), 0,);
            assert_eq!(
                ring_classify_crossings_scalar(xs, ys, point, 0..0),
                (0, false),
            );
        }
    }

    #[test]
    fn ring_classify_crossings_pins_rectangle_counts_and_inclusive_boxes() {
        let xs = [0.0, 4.0, 4.0, 0.0, 0.0];
        let ys = [0.0, 0.0, 4.0, 4.0, 0.0];
        for (probe, expected_count, expected_candidate, expected_class) in [
            (XY::new(2.0, 2.0), 1, false, RingClass::Interior),
            (XY::new(4.0, 2.0), 0, true, RingClass::Boundary),
            (XY::new(5.0, 2.0), 0, false, RingClass::Exterior),
            (XY::new(4.0, 4.0), 0, true, RingClass::Boundary),
        ] {
            assert_eq!(
                ring_classify_crossings_columns(&xs, &ys, probe),
                (expected_count, expected_candidate),
                "probe={probe:?}",
            );
            assert_eq!(
                classify_columns(&xs, &ys, probe),
                expected_class,
                "probe={probe:?}",
            );
        }
    }
}
