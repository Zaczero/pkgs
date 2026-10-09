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
    ExactChartPoint, point_on_exact_segment, vector_cross, vector_difference,
};
mod emission;
pub(in crate::geometry) use emission::{
    FeatureFamilyRegistry, PreparedNativeSource, SourceTrace, TracedLine,
};
#[cfg(test)]
mod graph;
pub(in crate::geometry) mod overlay;
mod q;
pub(in crate::geometry) use q::exact_segments_intersect;
pub(in crate::geometry) mod q_embedding;
#[cfg(test)]
mod staging;
#[cfg(test)]
pub(crate) use graph::TopologyGraph;
#[cfg(test)]
pub(super) use graph::{AreaSupport, TopologyEdge, TopologyKey, TopologyLocation, TopologyNodeId};
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
            let ax = ReduceSimd::from_slice(&xs[start..start + REDUCE_LANES]);
            let ay = ReduceSimd::from_slice(&ys[start..start + REDUCE_LANES]);
            let bx = ReduceSimd::from_slice(&xs[start + 1..start + 1 + REDUCE_LANES]);
            let by = ReduceSimd::from_slice(&ys[start + 1..start + 1 + REDUCE_LANES]);
            ray_crossing_lanes(ax, ay, bx, by, px, py).map_or_else(
                || {
                    (
                        (),
                        crossings
                            + ring_crossing_columns_scalar(
                                xs,
                                ys,
                                point,
                                start..start + REDUCE_LANES,
                            ),
                    )
                },
                |crosses| ((), crossings + crosses.to_bitmask().count_ones()),
            )
        },
        |crossings, range| crossings + ring_crossing_columns_scalar(xs, ys, point, range),
        |(), crossings| crossings,
    );
    crossings % 2 == 1
}

fn ring_crossing_columns_scalar(xs: &[f64], ys: &[f64], point: XY, range: Range<usize>) -> u32 {
    let mut crossings = 0_u32;
    for i in range {
        let (ax, ay, bx, by) = (xs[i], ys[i], xs[i + 1], ys[i + 1]);
        if (ay > point.y) != (by > point.y)
            && ray_crossing_is_right(ax, ay, bx, by, point.x, point.y)
        {
            crossings += 1;
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
            let ax = ReduceSimd::from_slice(&xs[start..start + REDUCE_LANES]);
            let ay = ReduceSimd::from_slice(&ys[start..start + REDUCE_LANES]);
            let bx = ReduceSimd::from_slice(&xs[start + 1..start + 1 + REDUCE_LANES]);
            let by = ReduceSimd::from_slice(&ys[start + 1..start + 1 + REDUCE_LANES]);
            // Boundary candidate: point inside this edge's bbox (four packed
            // compares); collapse the `REDUCE_LANES` lanes with `.any()`.
            let in_x = px.simd_ge(ax.simd_min(bx)) & px.simd_le(ax.simd_max(bx));
            let in_y = py.simd_ge(ay.simd_min(by)) & py.simd_le(ay.simd_max(by));
            let near = (in_x & in_y).any();
            // Crossing parity (same sign-form decision as the scalar loop).
            ray_crossing_lanes(ax, ay, bx, by, px, py).map_or_else(
                || {
                    let (c, n) =
                        ring_classify_crossings_scalar(xs, ys, point, start..start + REDUCE_LANES);
                    ((), (crossings + c, candidate || near || n))
                },
                |crosses| {
                    (
                        (),
                        (
                            crossings + crosses.to_bitmask().count_ones(),
                            candidate || near,
                        ),
                    )
                },
            )
        },
        |(crossings, candidate), range| {
            let (c, near) = ring_classify_crossings_scalar(xs, ys, point, range);
            (crossings + c, candidate || near)
        },
        |(), acc| acc,
    )
}

fn ring_classify_crossings_scalar(
    xs: &[f64],
    ys: &[f64],
    point: XY,
    range: Range<usize>,
) -> (u32, bool) {
    let mut crossings = 0_u32;
    let mut candidate = false;
    for i in range {
        let (ax, ay, bx, by) = (xs[i], ys[i], xs[i + 1], ys[i + 1]);
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
            crossings += 1;
        }
    }
    (crossings, candidate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::predicates::ring_contains_interior;
    use crate::geometry::{CoordSeq, REDUCE_SIMD_MIN};

    /// The closed-column membership kernel over an open ring's vertices.
    fn ring_contains_interior_open(ring: &[XY], point: XY) -> bool {
        let closed = || ring.iter().chain(std::iter::once(&ring[0]));
        let xs: Vec<f64> = closed().map(|vertex| vertex.x).collect();
        let ys: Vec<f64> = closed().map(|vertex| vertex.y).collect();
        ring_contains_interior_columns(&xs, &ys, point)
    }

    fn division_reference(ring: &[XY], point: XY) -> bool {
        let mut closed = ring.to_vec();
        closed.push(ring[0]);
        ring_contains_interior(&CoordSeq::try_from_xy(&closed).unwrap(), point)
    }

    fn assert_vector_matches_division_reference(ring: &[XY], probes: &[XY]) {
        assert!(
            ring.len() >= REDUCE_SIMD_MIN,
            "test ring should exercise SIMD path",
        );
        for &point in probes {
            assert_eq!(
                ring_contains_interior_open(ring, point),
                division_reference(ring, point),
                "ring={ring:?} point={point:?}",
            );
        }
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
    fn open_ring_simd_matches_scalar_on_adversarial_rings() {
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
            assert_vector_matches_division_reference(&ring, &probes);
        }
    }

    #[test]
    fn open_ring_simd_matches_division_reference_on_review_divergences() {
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
        assert!(division_reference(&huge, huge_probe));
        assert_eq!(
            ring_contains_interior_open(&huge, huge_probe),
            division_reference(&huge, huge_probe),
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
        assert!(division_reference(&on_edge, on_edge_probe));
        assert_eq!(
            ring_contains_interior_open(&on_edge, on_edge_probe),
            division_reference(&on_edge, on_edge_probe),
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

    /// The same overflow class through the SIMD chunk path: densify the safe
    /// bottom edge so the ring crosses the SIMD crossover while the huge
    /// opposite-sign diagonal lane demotes its chunk to the exact fallback.
    #[test]
    fn ray_crossing_simd_chunks_demote_untrusted_lanes_to_exact() {
        let n = 96_usize;
        let mut ring = vec![XY::new(-1e308, -1.0), XY::new(1e308, 1.0)];
        // Bottom edge from (1e308, -1) back towards (-1e308, -1), densified.
        for i in 0..n {
            let fraction = (i as f64) / (n as f64);
            ring.push(XY::new(1e308 * (1.0 - 2.0 * fraction), -1.0));
        }
        let columns: Vec<XY> = ring.clone();
        let xs: Vec<f64> = columns
            .iter()
            .map(|p| p.x)
            .chain(std::iter::once(columns[0].x))
            .collect();
        let ys: Vec<f64> = columns
            .iter()
            .map(|p| p.y)
            .chain(std::iter::once(columns[0].y))
            .collect();
        for (probe, expected) in [
            (XY::new(0.0, 0.5), false),
            (XY::new(5e307, 0.0), true),
            // Above the diagonal (its y at x = -5e307 is exactly -0.5).
            (XY::new(-5e307, -0.25), false),
            // Below the diagonal, above the bottom edge.
            (XY::new(-5e307, -0.75), true),
        ] {
            assert_eq!(
                ring_contains_interior_open(&ring, probe),
                expected,
                "AoS SIMD path probe={probe:?}",
            );
            assert_eq!(
                ring_contains_interior_columns(&xs, &ys, probe),
                expected,
                "columns SIMD path probe={probe:?}",
            );
            let (crossings, _) = ring_classify_crossings_columns(&xs, &ys, probe);
            assert_eq!(
                crossings % 2 == 1,
                expected,
                "classify columns path probe={probe:?}",
            );
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
    fn open_ring_simd_matches_division_reference_on_curated_rings() {
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
            assert_vector_matches_division_reference(&ring, &probes);
        }
    }
}
