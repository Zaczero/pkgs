//! Hierarchical prepared point-in-polygon membership.
//!
//! Ownership mirrors geometry structure:
//! - [`PointBatchTester`] — multipolygon parts via [`YStabbingIndex`]
//! - [`PolygonPointIndex`] — shell ring + holes via [`YStabbingIndex`]
//! - [`RingPointIndex`] — ring edges via the same index type
//!
//! Every descent applies a full X/Y bounds check before exact classification.
//! Semantics are exact: polygon = shell minus its holes; multipolygon = any
//! Interior wins, else Boundary, else Exterior (never global XOR across parts).
//! Crossing arithmetic reuses [`ray_crossing_is_right`] / [`orientation_xy`].

use std::ops::ControlFlow;

use crate::HeapSize;
use crate::geometry::segment_index::{EdgeYIndex, YStabbingIndex};
use crate::geometry::{
    Bounds, Orientation, Point, Polygon, RingClass, Shape, orientation_xy, ray_crossing_is_right,
    xy_bounds_columns,
};

/// One closed ring's Y-indexed edges for prepared point membership.
pub(crate) struct RingPointIndex {
    bounds: Bounds,
    xs: Box<[f64]>,
    ys: Box<[f64]>,
    /// Edge start indices (`0..n-1`) stabbed by Y — dense CSR stores edge
    /// ids directly (no items-vector indirection).
    edges: EdgeYIndex,
}

impl RingPointIndex {
    fn build(xs: &[f64], ys: &[f64]) -> Self {
        let n = xs.len();
        debug_assert_eq!(n, ys.len());
        let bounds = if n == 0 {
            Bounds::new_unchecked(
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            )
        } else {
            let [minx, miny, maxx, maxy] = xy_bounds_columns(xs, ys);
            Bounds::new_unchecked(minx, miny, maxx, maxy)
        };
        Self {
            bounds,
            xs: xs.to_vec().into_boxed_slice(),
            ys: ys.to_vec().into_boxed_slice(),
            edges: EdgeYIndex::build(ys),
        }
    }

    fn bounds_contains(&self, x: f64, y: f64) -> bool {
        x >= self.bounds.minx()
            && x <= self.bounds.maxx()
            && y >= self.bounds.miny()
            && y <= self.bounds.maxy()
    }

    /// Exact classification without the ring envelope gate.
    fn classify_at(&self, x: f64, y: f64) -> RingClass {
        let xs = &self.xs;
        let ys = &self.ys;
        // Dense CSR: open loop matching prior RingRaycaster (no closure).
        if let Some(edges) = self.edges.dense_band_edges(y) {
            let mut inside = false;
            for &edge in edges {
                let e = edge as usize;
                let (ax, ay, bx, by) = (xs[e], ys[e], xs[e + 1], ys[e + 1]);
                if x >= ax.min(bx)
                    && x <= ax.max(bx)
                    && y >= ay.min(by)
                    && y <= ay.max(by)
                    && orientation_xy(ax, ay, bx, by, x, y) == Orientation::Collinear
                {
                    return RingClass::Boundary;
                }
                if (ay > y) != (by > y) && ray_crossing_is_right(ax, ay, bx, by, x, y) {
                    inside = !inside;
                }
            }
            return if inside {
                RingClass::Interior
            } else {
                RingClass::Exterior
            };
        }
        // Linear / interval-tree fallback.
        let mut inside = false;
        if self.edges.for_each_edge(y, |edge| {
            let e = edge as usize;
            let (ax, ay, bx, by) = (xs[e], ys[e], xs[e + 1], ys[e + 1]);
            if x >= ax.min(bx)
                && x <= ax.max(bx)
                && y >= ay.min(by)
                && y <= ay.max(by)
                && orientation_xy(ax, ay, bx, by, x, y) == Orientation::Collinear
            {
                return ControlFlow::Break(RingClass::Boundary);
            }
            if (ay > y) != (by > y) && ray_crossing_is_right(ax, ay, bx, by, x, y) {
                inside = !inside;
            }
            ControlFlow::Continue(())
        }) == Some(RingClass::Boundary)
        {
            return RingClass::Boundary;
        }
        if inside {
            RingClass::Interior
        } else {
            RingClass::Exterior
        }
    }
}

impl HeapSize for RingPointIndex {
    fn heap_bytes(&self) -> usize {
        self.xs.heap_bytes() + self.ys.heap_bytes() + self.edges.heap_bytes()
    }
}

/// One polygon: shell minus holes, with hierarchical Y-stabbing on both levels.
pub(crate) struct PolygonPointIndex {
    bounds: Option<Bounds>,
    shell: RingPointIndex,
    holes: YStabbingIndex<RingPointIndex>,
}

impl PolygonPointIndex {
    fn build(polygon: &Polygon) -> Self {
        let shell = RingPointIndex::build(polygon.shell.coords().xs(), polygon.shell.coords().ys());
        let hole_rings: Vec<RingPointIndex> = polygon
            .holes
            .iter()
            .map(|hole| RingPointIndex::build(hole.coords().xs(), hole.coords().ys()))
            .collect();
        let holes =
            YStabbingIndex::build(hole_rings, |ring| (ring.bounds.miny(), ring.bounds.maxy()));
        Self {
            bounds: Bounds::from_coords(polygon.shell.coords()),
            shell,
            holes,
        }
    }

    fn in_bounds_xy(&self, x: f64, y: f64) -> bool {
        self.bounds.is_some_and(|bounds| {
            x >= bounds.minx() && x <= bounds.maxx() && y >= bounds.miny() && y <= bounds.maxy()
        })
    }

    fn contains_xy(&self, x: f64, y: f64) -> bool {
        if !self.in_bounds_xy(x, y) {
            return false;
        }
        // Shell envelope ≈ polygon envelope — skip redundant ring gate.
        if self.shell.classify_at(x, y) != RingClass::Interior {
            return false;
        }
        !self.hole_hits_xy(x, y, |class| class != RingClass::Exterior)
    }

    fn covers_xy(&self, x: f64, y: f64) -> bool {
        if !self.in_bounds_xy(x, y) {
            return false;
        }
        if self.shell.classify_at(x, y) == RingClass::Exterior {
            return false;
        }
        !self.hole_hits_xy(x, y, |class| class == RingClass::Interior)
    }

    /// Three-way membership: `Boundary` on shell or any hole ring, `Interior`
    /// if strictly inside the shell and strictly outside every hole, else
    /// `Exterior`. Bounds-gate only rules out `Exterior`.
    fn classify_xy(&self, x: f64, y: f64) -> RingClass {
        if !self.in_bounds_xy(x, y) {
            return RingClass::Exterior;
        }
        match self.shell.classify_at(x, y) {
            RingClass::Exterior => RingClass::Exterior,
            RingClass::Boundary => RingClass::Boundary,
            RingClass::Interior => self.classify_holes_xy(x, y),
        }
    }

    /// Shell already classified Interior; fold hole rings.
    fn classify_holes_xy(&self, x: f64, y: f64) -> RingClass {
        let mut on_hole = false;
        let mut interior_hole = false;
        // Few holes (common): full XY envelope gate per hole without
        // Y-index indirection. Many holes: Y-stab then XY gate.
        if self.holes.len() <= 32 {
            self.holes.for_each(|hole| {
                if interior_hole || !hole.bounds_contains(x, y) {
                    return;
                }
                match hole.classify_at(x, y) {
                    RingClass::Interior => interior_hole = true,
                    RingClass::Boundary => on_hole = true,
                    RingClass::Exterior => {},
                }
            });
        } else {
            let _ = self.holes.for_each_at_y(y, |hole| {
                if interior_hole {
                    return ControlFlow::Break(());
                }
                if !hole.bounds_contains(x, y) {
                    return ControlFlow::Continue(());
                }
                match hole.classify_at(x, y) {
                    RingClass::Interior => {
                        interior_hole = true;
                        ControlFlow::Break(())
                    },
                    RingClass::Boundary => {
                        on_hole = true;
                        ControlFlow::Continue(())
                    },
                    RingClass::Exterior => ControlFlow::Continue(()),
                }
            });
        }
        if interior_hole {
            RingClass::Exterior
        } else if on_hole {
            RingClass::Boundary
        } else {
            RingClass::Interior
        }
    }

    /// True if any hole (after XY bounds gate) satisfies `pred`.
    fn hole_hits_xy(&self, x: f64, y: f64, pred: impl Fn(RingClass) -> bool) -> bool {
        let mut hit = false;
        if self.holes.len() <= 32 {
            self.holes.for_each(|hole| {
                if hit || !hole.bounds_contains(x, y) {
                    return;
                }
                if pred(hole.classify_at(x, y)) {
                    hit = true;
                }
            });
        } else {
            let _ = self.holes.for_each_at_y(y, |hole| {
                if hit {
                    return ControlFlow::Break(());
                }
                if !hole.bounds_contains(x, y) {
                    return ControlFlow::Continue(());
                }
                if pred(hole.classify_at(x, y)) {
                    hit = true;
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            });
        }
        hit
    }
}

impl HeapSize for PolygonPointIndex {
    fn heap_bytes(&self) -> usize {
        self.shell.heap_bytes() + self.holes.heap_bytes()
    }
}

/// Batched point-membership tester for a fixed shape: polygonal shapes get a
/// hierarchical Y-stabbing plan (parts → shell/holes → edges); everything else
/// falls through to the point kernels.
pub(crate) enum PointBatchTester {
    /// Single polygon — no multipolygon Y-index indirection.
    Polygon(PolygonPointIndex),
    /// Multiple parts stabbed by Y; each part is an independent polygon
    /// (union semantics: any Interior wins).
    MultiPolygon(YStabbingIndex<PolygonPointIndex>),
    Generic(Shape),
}

impl PointBatchTester {
    /// Probe count past which building the index beats per-probe ring scans.
    pub(crate) const MIN_PROBES: usize = 64;

    pub(crate) fn new(shape: &Shape) -> Self {
        match shape {
            Shape::Polygon(polygon) => Self::Polygon(PolygonPointIndex::build(polygon)),
            Shape::MultiPolygon(polygons) if polygons.len() == 1 => {
                Self::Polygon(PolygonPointIndex::build(&polygons[0]))
            },
            Shape::MultiPolygon(polygons) => {
                let built: Vec<PolygonPointIndex> =
                    polygons.iter().map(PolygonPointIndex::build).collect();
                let parts = YStabbingIndex::build(built, |p| {
                    p.bounds
                        .map_or((f64::INFINITY, f64::NEG_INFINITY), |b| (b.miny(), b.maxy()))
                });
                Self::MultiPolygon(parts)
            },
            _ => Self::Generic(shape.clone()),
        }
    }

    /// Strict membership — [`Shape::contains_point`] semantics.
    pub(crate) fn contains_point(&self, point: Point) -> bool {
        match self {
            Self::Polygon(polygon) => polygon.contains_xy(point.x, point.y),
            Self::MultiPolygon(parts) => {
                let mut hit = false;
                let _ = parts.for_each_at_y(point.y, |polygon| {
                    if polygon.contains_xy(point.x, point.y) {
                        hit = true;
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                });
                hit
            },
            Self::Generic(shape) => shape.contains_point(point),
        }
    }

    /// Boundary-inclusive membership — [`Shape::covers_point`] semantics.
    pub(crate) fn covers_point(&self, point: Point) -> bool {
        match self {
            Self::Polygon(polygon) => polygon.covers_xy(point.x, point.y),
            Self::MultiPolygon(parts) => {
                let mut hit = false;
                let _ = parts.for_each_at_y(point.y, |polygon| {
                    if polygon.covers_xy(point.x, point.y) {
                        hit = true;
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                });
                hit
            },
            Self::Generic(shape) => shape.covers_point(point),
        }
    }

    /// Three-way areal membership: `Interior` (strictly inside the area),
    /// `Boundary` (on a ring), or `Exterior`. Strictly interior to ANY part
    /// wins over a boundary hit on another (union semantics).
    pub(crate) fn classify_area_point(&self, point: Point) -> Option<RingClass> {
        match self {
            Self::Polygon(polygon) => Some(polygon.classify_xy(point.x, point.y)),
            Self::MultiPolygon(parts) => {
                let mut on_boundary = false;
                let mut interior = false;
                let _ = parts.for_each_at_y(point.y, |polygon| {
                    if interior {
                        return ControlFlow::Break(());
                    }
                    match polygon.classify_xy(point.x, point.y) {
                        RingClass::Interior => {
                            interior = true;
                            ControlFlow::Break(())
                        },
                        RingClass::Boundary => {
                            on_boundary = true;
                            ControlFlow::Continue(())
                        },
                        RingClass::Exterior => ControlFlow::Continue(()),
                    }
                });
                Some(if interior {
                    RingClass::Interior
                } else if on_boundary {
                    RingClass::Boundary
                } else {
                    RingClass::Exterior
                })
            },
            Self::Generic(_) => None,
        }
    }
}

impl HeapSize for PointBatchTester {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Polygon(polygon) => polygon.heap_bytes(),
            Self::MultiPolygon(parts) => parts.heap_bytes(),
            Self::Generic(shape) => shape.coordinate_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::geometry::XY;
    use crate::{CoordSeq, Ring};

    fn closed_ring(pts: &[(f64, f64)]) -> Ring {
        let xy: Vec<XY> = pts.iter().map(|&(x, y)| XY::new(x, y)).collect();
        Ring::from_trusted_closed(CoordSeq::from_xy(&xy))
    }

    fn unit_square() -> Polygon {
        Polygon {
            shell: closed_ring(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
            holes: Arc::from([]),
        }
    }

    fn square_with_hole() -> Polygon {
        Polygon {
            shell: closed_ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            holes: Arc::from([closed_ring(&[
                (3.0, 3.0),
                (7.0, 3.0),
                (7.0, 7.0),
                (3.0, 7.0),
                (3.0, 3.0),
            ])]),
        }
    }

    #[test]
    fn contains_covers_and_classify_match_shape_kernels() {
        let poly = square_with_hole();
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape);
        let probes = [
            (5.0, 5.0),
            (1.0, 1.0),
            (0.0, 5.0),
            (3.0, 5.0),
            (20.0, 20.0),
            (5.0, 1.0),
        ];
        for &(x, y) in &probes {
            let p = Point::new_unchecked_xy(x, y);
            assert_eq!(
                tester.contains_point(p),
                shape.contains_point(p),
                "contains at ({x},{y})"
            );
            assert_eq!(
                tester.covers_point(p),
                shape.covers_point(p),
                "covers at ({x},{y})"
            );
            let class = tester.classify_area_point(p).unwrap();
            let expect = if shape.contains_point(p) {
                RingClass::Interior
            } else if shape.covers_point(p) {
                RingClass::Boundary
            } else {
                RingClass::Exterior
            };
            assert_eq!(class, expect, "classify at ({x},{y})");
        }
    }

    #[test]
    fn multipolygon_is_union_not_xor() {
        let a = unit_square();
        let b = Polygon {
            shell: closed_ring(&[(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5), (0.5, 0.5)]),
            holes: Arc::from([]),
        };
        let shape = Shape::MultiPolygon(vec![a, b]);
        let tester = PointBatchTester::new(&shape);
        let overlap = Point::new_unchecked_xy(0.75, 0.75);
        assert!(tester.contains_point(overlap));
        assert_eq!(
            tester.classify_area_point(overlap),
            Some(RingClass::Interior)
        );
        let out = Point::new_unchecked_xy(3.0, 3.0);
        assert!(!tester.contains_point(out));
        assert_eq!(tester.classify_area_point(out), Some(RingClass::Exterior));
    }

    #[test]
    fn hole_envelope_skips_non_covering_holes() {
        let poly = square_with_hole();
        let tester = PointBatchTester::new(&Shape::Polygon(poly));
        let p = Point::new_unchecked_xy(1.0, 9.0);
        assert!(tester.contains_point(p));
        assert_eq!(tester.classify_area_point(p), Some(RingClass::Interior));
    }

    #[test]
    fn tall_edge_ring_constructs_and_classifies() {
        let n_side = 2000_usize;
        let mut pts = Vec::with_capacity(2 * n_side + 3);
        for i in 0..=n_side {
            pts.push(XY::new(0.0, i as f64));
        }
        pts.push(XY::new(1.0, n_side as f64));
        for i in (0..n_side).rev() {
            pts.push(XY::new(1.0, i as f64));
        }
        pts.push(XY::new(0.0, 0.0));
        let poly = Polygon {
            shell: Ring::from_trusted_closed(CoordSeq::from_xy(&pts)),
            holes: Arc::from([]),
        };
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape);
        assert!(tester.contains_point(Point::new_unchecked_xy(0.5, n_side as f64 * 0.5)));
        assert!(!tester.contains_point(Point::new_unchecked_xy(-1.0, n_side as f64 * 0.5)));
        let bytes = tester.heap_bytes();
        let verts = pts.len();
        assert!(
            bytes < verts * 128,
            "tall-edge prepared index not proportional: {bytes} bytes for {verts} verts"
        );
    }

    #[test]
    fn many_sparse_holes_match_shape() {
        let shell = closed_ring(&[
            (0.0, 0.0),
            (1000.0, 0.0),
            (1000.0, 1000.0),
            (0.0, 1000.0),
            (0.0, 0.0),
        ]);
        let mut holes = Vec::new();
        for i in 0..100_i32 {
            let cx = 50.0 + f64::from(i % 10) * 90.0;
            let cy = 50.0 + f64::from(i / 10) * 90.0;
            let r = 10.0;
            holes.push(closed_ring(&[
                (cx - r, cy - r),
                (cx + r, cy - r),
                (cx + r, cy + r),
                (cx - r, cy + r),
                (cx - r, cy - r),
            ]));
        }
        let poly = Polygon {
            shell,
            holes: Arc::from(holes),
        };
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape);
        for i in 0..200_i32 {
            let x = f64::from(i * 37 + 13) % 1000.0;
            let y = f64::from(i * 53 + 7) % 1000.0;
            let p = Point::new_unchecked_xy(x, y);
            assert_eq!(
                tester.contains_point(p),
                shape.contains_point(p),
                "mismatch at ({x},{y})"
            );
            assert_eq!(
                tester.covers_point(p),
                shape.covers_point(p),
                "covers mismatch at ({x},{y})"
            );
        }
    }
}
