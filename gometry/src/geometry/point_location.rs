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
use std::sync::Arc;

use crate::HeapSize;
use crate::geometry::segment_index::{EdgeId, EdgeYIndex, YStabbingIndex};
use crate::geometry::{
    Bounds, CoordSeq, Point, Polygon, Ring, RingClass, RingEdgeMembership, Shape,
    ring_edge_membership, xy_bounds_columns,
};

/// One closed ring's Y-indexed edges for prepared point membership.
pub(crate) struct RingPointIndex {
    bounds: Bounds,
    xs: Arc<[f64]>,
    ys: Arc<[f64]>,
    /// Edge start indices (`0..n-1`) stabbed by Y — dense CSR stores edge
    /// ids directly (no items-vector indirection).
    edges: EdgeYIndex,
}

impl RingPointIndex {
    fn build(coords: &CoordSeq) -> Self {
        let xs = coords.xs();
        let ys = coords.ys();
        let n = xs.len();
        debug_assert_eq!(n, ys.len());
        let bounds = if n == 0 {
            Bounds::new_raw(
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            )
        } else {
            let [minx, miny, maxx, maxy] = xy_bounds_columns(xs, ys);
            Bounds::new_raw(minx, miny, maxx, maxy)
        };
        Self {
            bounds,
            xs: coords.carried_xs(),
            ys: coords.carried_ys(),
            edges: EdgeYIndex::build(ys),
        }
    }

    fn bounds_contains(&self, x: f64, y: f64) -> bool {
        x >= self.bounds.minx()
            && x <= self.bounds.maxx()
            && y >= self.bounds.miny()
            && y <= self.bounds.maxy()
    }

    fn edge_xy(&self, edge: EdgeId) -> (f64, f64, f64, f64) {
        let e = edge.as_usize();
        debug_assert_eq!(self.xs.len(), self.ys.len());
        debug_assert!(e + 1 < self.xs.len());
        // SAFETY: EdgeYIndex::build creates exactly one id per adjacent Y pair,
        // and its dense/tree plans only emit those ids. The index is built from
        // these same columns, whose equal lengths make every emitted id satisfy
        // `e + 1 < self.xs.len()`.
        unsafe {
            (
                *self.xs.get_unchecked(e),
                *self.ys.get_unchecked(e),
                *self.xs.get_unchecked(e + 1),
                *self.ys.get_unchecked(e + 1),
            )
        }
    }

    /// Exact classification without the ring envelope gate.
    fn classify_at(&self, x: f64, y: f64) -> RingClass {
        if let Some(edges) = self.edges.dense_band_edges(y) {
            let mut inside = false;
            for &edge in edges {
                let (ax, ay, bx, by) = self.edge_xy(edge);
                match ring_edge_membership(ax, ay, bx, by, x, y) {
                    RingEdgeMembership::Boundary => return RingClass::Boundary,
                    RingEdgeMembership::Crossing => inside = !inside,
                    RingEdgeMembership::None => {},
                }
            }
            return if inside {
                RingClass::Interior
            } else {
                RingClass::Exterior
            };
        }

        let mut inside = false;
        if self.edges.for_each_edge(y, |edge| {
            let (ax, ay, bx, by) = self.edge_xy(edge);
            match ring_edge_membership(ax, ay, bx, by, x, y) {
                RingEdgeMembership::Boundary => return ControlFlow::Break(RingClass::Boundary),
                RingEdgeMembership::Crossing => inside = !inside,
                RingEdgeMembership::None => {},
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
    pub(crate) fn build(polygon: &Polygon) -> Self {
        let mut index = Self::build_shell(&polygon.shell);
        let hole_rings: Vec<RingPointIndex> = polygon
            .holes
            .iter()
            .map(|hole| RingPointIndex::build(hole.coords()))
            .collect();
        index.holes = YStabbingIndex::build(hole_rings, |ring| {
            (
                ring.bounds.minx(),
                ring.bounds.maxx(),
                ring.bounds.miny(),
                ring.bounds.maxy(),
            )
        });
        index
    }

    pub(in crate::geometry) fn build_shell(shell: &Ring) -> Self {
        let shell_index = RingPointIndex::build(shell.coords());
        Self {
            bounds: Some(shell_index.bounds),
            shell: shell_index,
            holes: YStabbingIndex::build(Vec::new(), |_| {
                (
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                )
            }),
        }
    }

    fn in_bounds_xy(&self, x: f64, y: f64) -> bool {
        self.bounds.is_some_and(|bounds| {
            x >= bounds.minx() && x <= bounds.maxx() && y >= bounds.miny() && y <= bounds.maxy()
        })
    }

    /// The closed `(min_x, max_x, min_y, max_y)` envelope, absent for an
    /// empty polygon.
    pub(crate) fn envelope(&self) -> Option<(f64, f64, f64, f64)> {
        self.bounds
            .map(|bounds| (bounds.minx(), bounds.maxx(), bounds.miny(), bounds.maxy()))
    }

    /// Strict membership after the envelope candidate gate.
    pub(crate) fn contains_xy(&self, x: f64, y: f64) -> bool {
        if !self.in_bounds_xy(x, y) {
            return false;
        }
        // Shell envelope ≈ polygon envelope — skip redundant ring gate.
        if self.shell.classify_at(x, y) != RingClass::Interior {
            return false;
        }
        !self.hole_hits_xy(x, y, |class| class != RingClass::Exterior)
    }

    pub(crate) fn covers_point(&self, point: Point) -> bool {
        self.covers_xy(point.x, point.y)
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
    #[cfg(test)]
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
    #[cfg(test)]
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
            let _ = self.holes.for_each_at_point(x, y, |hole| {
                if interior_hole {
                    return ControlFlow::Break(());
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
            let _ = self.holes.for_each_at_point(x, y, |hole| {
                if hit {
                    return ControlFlow::Break(());
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
    Polygon {
        index: PolygonPointIndex,
    },
    /// Multiple parts stabbed by Y; each part is an independent polygon
    /// (union semantics: any Interior wins).
    MultiPolygon {
        parts: YStabbingIndex<PolygonPointIndex>,
    },
    Generic(Shape),
}

impl PointBatchTester {
    #[cfg(test)]
    const fn uses_indexed_plan(&self) -> bool {
        match self {
            Self::Polygon { index: polygon, .. } => {
                polygon.shell.edges.uses_indexed_plan() || polygon.holes.uses_indexed_plan()
            },
            Self::MultiPolygon { parts, .. } => parts.uses_indexed_plan(),
            Self::Generic(_) => false,
        }
    }

    pub(crate) fn new(shape: &Shape) -> Self {
        match shape {
            Shape::Polygon(polygon) => Self::from_polygon(polygon),
            Shape::MultiPolygon(polygons) => Self::from_multipolygon(polygons),
            _ => Self::Generic(shape.clone()),
        }
    }

    pub(crate) fn from_polygon(polygon: &Polygon) -> Self {
        Self::Polygon {
            index: PolygonPointIndex::build(polygon),
        }
    }

    pub(crate) fn from_multipolygon(polygons: &crate::geometry::MultiPolygon) -> Self {
        if polygons.len() == 1 && !polygons.is_empty() {
            return Self::Polygon {
                index: PolygonPointIndex::build(&polygons[0]),
            };
        }
        let built: Vec<PolygonPointIndex> = polygons.iter().map(PolygonPointIndex::build).collect();
        let parts = YStabbingIndex::build(built, |part| {
            part.bounds.map_or(
                (
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                ),
                |bounds| (bounds.minx(), bounds.maxx(), bounds.miny(), bounds.maxy()),
            )
        });
        Self::MultiPolygon { parts }
    }

    /// Strict membership — [`Shape::contains_point`] semantics.
    #[cfg(test)]
    pub(crate) fn contains_point(&self, point: Point) -> bool {
        match self {
            Self::Polygon { index: polygon, .. } => polygon.contains_xy(point.x, point.y),
            Self::MultiPolygon { parts, .. } => {
                let mut hit = false;
                let _ = parts.for_each_at_point(point.x, point.y, |polygon| {
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
            Self::Polygon { index: polygon, .. } => polygon.covers_xy(point.x, point.y),
            Self::MultiPolygon { parts, .. } => {
                let mut hit = false;
                let _ = parts.for_each_at_point(point.x, point.y, |polygon| {
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
    #[cfg(test)]
    pub(crate) fn classify_area_point(&self, point: Point) -> Option<RingClass> {
        match self {
            Self::Polygon { index: polygon, .. } => Some(polygon.classify_xy(point.x, point.y)),
            Self::MultiPolygon { parts, .. } => {
                let mut on_boundary = false;
                let mut interior = false;
                let _ = parts.for_each_at_point(point.x, point.y, |polygon| {
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
            Self::Polygon { index: polygon } => polygon.heap_bytes(),
            Self::MultiPolygon { parts } => parts.heap_bytes(),
            Self::Generic(shape) => shape.coordinate_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::geometry::{CoordWindow, XY, ring_classify_point};
    use crate::{CoordSeq, Ring};

    fn closed_ring(pts: &[(f64, f64)]) -> Ring {
        let xy: Vec<XY> = pts.iter().map(|&(x, y)| XY::new(x, y)).collect();
        Ring::from_closed(CoordSeq::from_xy(&xy))
    }

    fn unit_square() -> Polygon {
        Polygon {
            shell: closed_ring(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
            holes: Arc::from([]),
        }
    }

    #[test]
    fn point_probe_policy_indexes_polygon_hierarchy() {
        let mut polygon = unit_square();
        polygon.holes = Arc::from(
            std::iter::repeat_with(|| {
                closed_ring(&[(2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 3.0), (2.0, 2.0)])
            })
            .take(33)
            .collect::<Vec<_>>(),
        );
        let tester = PointBatchTester::new(&Shape::Polygon(polygon));
        assert!(tester.uses_indexed_plan());
    }

    #[test]
    fn point_probe_policy_indexes_33_multipolygon_parts() {
        let parts = (0..33)
            .map(|index| {
                let x = f64::from(index) * 2.0;
                Polygon {
                    shell: closed_ring(&[
                        (x, 0.0),
                        (x + 1.0, 0.0),
                        (x + 1.0, 1.0),
                        (x, 1.0),
                        (x, 0.0),
                    ]),
                    holes: Arc::from([]),
                }
            })
            .collect::<Vec<_>>();
        let tester = PointBatchTester::new(&Shape::MultiPolygon(parts.into()));
        assert!(tester.uses_indexed_plan());
    }

    #[test]
    fn dense_ring_classification_matches_shape_kernel() {
        let mut points = Vec::with_capacity(37);
        points.extend((0..=9).map(|x| XY::new(f64::from(x), 0.0)));
        points.extend((1..=9).map(|y| XY::new(10.0, f64::from(y))));
        points.extend((0..=8).rev().map(|x| XY::new(f64::from(x), 10.0)));
        points.extend((1..=8).rev().map(|y| XY::new(0.0, f64::from(y))));
        points.push(XY::new(0.0, 0.0));
        let coords = CoordSeq::from(points);
        assert_eq!(coords.xs().len() - 1, 36);
        let ring = RingPointIndex::build(&coords);
        assert!(ring.edges.dense_band_edges(5.0).is_some());

        for (x, y) in [(5.0, 0.0), (5.0, 5.0), (9.0, 5.0), (-1.0, 5.0)] {
            assert_eq!(
                ring.classify_at(x, y),
                ring_classify_point(&coords, XY::new(x, y).point()),
                "classification mismatch at ({x}, {y})"
            );
        }
    }

    #[test]
    fn full_window_ring_index_shares_xy_and_survives_source_drop() {
        let coords = CoordSeq::from_columns(
            [0.0, 2.0, 2.0, 0.0, 0.0].into(),
            [0.0, 0.0, 2.0, 2.0, 0.0].into(),
            None,
            None,
        );
        let columns = coords.column_arcs();
        let source_xs = Arc::clone(&columns.xs);
        let source_ys = Arc::clone(&columns.ys);
        let ring = RingPointIndex::build(&coords);

        assert!(Arc::ptr_eq(&ring.xs, &source_xs));
        assert!(Arc::ptr_eq(&ring.ys, &source_ys));

        drop(columns);
        drop(coords);
        drop(source_xs);
        drop(source_ys);

        assert_eq!(ring.classify_at(1.0, 1.0), RingClass::Interior);
        assert_eq!(ring.classify_at(0.0, 1.0), RingClass::Boundary);
        assert_eq!(ring.classify_at(3.0, 1.0), RingClass::Exterior);
    }

    #[test]
    fn windowed_ring_index_classifies_visible_coordinates() {
        let source_xs: Arc<[f64]> = [1000.0, 0.0, 2.0, 2.0, 0.0, 0.0, -1000.0].into();
        let source_ys: Arc<[f64]> = [1000.0, 0.0, 0.0, 2.0, 2.0, 0.0, -1000.0].into();
        let source_xs_weak = Arc::downgrade(&source_xs);
        let source_ys_weak = Arc::downgrade(&source_ys);
        let coords =
            CoordSeq::from_columns(Arc::clone(&source_xs), Arc::clone(&source_ys), None, None);
        let visible = coords.view(CoordWindow::from_range(1..6, coords.len()));
        let ring = RingPointIndex::build(&visible);

        drop(visible);
        drop(coords);
        drop(source_xs);
        drop(source_ys);
        assert!(source_xs_weak.upgrade().is_none());
        assert!(source_ys_weak.upgrade().is_none());

        assert_eq!(ring.classify_at(1.0, 1.0), RingClass::Interior);
        assert_eq!(ring.classify_at(0.0, 1.0), RingClass::Boundary);
        assert_eq!(ring.classify_at(3.0, 1.0), RingClass::Exterior);
    }

    #[test]
    fn xyzm_ring_index_classifies_from_xy_columns() {
        let coords = CoordSeq::from_columns(
            [0.0, 2.0, 2.0, 0.0, 0.0].into(),
            [0.0, 0.0, 2.0, 2.0, 0.0].into(),
            Some([10.0, 11.0, 12.0, 13.0, 10.0].into()),
            Some([20.0, 21.0, 22.0, 23.0, 20.0].into()),
        );
        let columns = coords.column_arcs();
        let z_weak = Arc::downgrade(columns.zs.as_ref().expect("Z column"));
        let m = columns.m_parts().expect("M column").0;
        let m_weak = Arc::downgrade(&m);
        let ring = RingPointIndex::build(&coords);

        assert!(Arc::ptr_eq(&ring.xs, &columns.xs));
        assert!(Arc::ptr_eq(&ring.ys, &columns.ys));
        drop(m);
        drop(columns);
        drop(coords);
        assert!(z_weak.upgrade().is_none());
        assert!(m_weak.upgrade().is_none());
        assert_eq!(ring.classify_at(1.0, 1.0), RingClass::Interior);
        assert_eq!(ring.classify_at(0.0, 1.0), RingClass::Boundary);
        assert_eq!(ring.classify_at(3.0, 1.0), RingClass::Exterior);
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
            let p = Point::new_raw_xy(x, y);
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
        let shape = Shape::MultiPolygon(vec![a, b].into());
        let tester = PointBatchTester::new(&shape);
        let overlap = Point::new_raw_xy(0.75, 0.75);
        assert!(tester.contains_point(overlap));
        assert_eq!(
            tester.classify_area_point(overlap),
            Some(RingClass::Interior)
        );
        let out = Point::new_raw_xy(3.0, 3.0);
        assert!(!tester.contains_point(out));
        assert_eq!(tester.classify_area_point(out), Some(RingClass::Exterior));
    }

    #[test]
    fn hole_envelope_skips_non_covering_holes() {
        let poly = square_with_hole();
        let tester = PointBatchTester::new(&Shape::Polygon(poly));
        let p = Point::new_raw_xy(1.0, 9.0);
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
            shell: Ring::from_closed(CoordSeq::from_xy(&pts)),
            holes: Arc::from([]),
        };
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape);
        assert!(tester.contains_point(Point::new_raw_xy(0.5, n_side as f64 * 0.5)));
        assert!(!tester.contains_point(Point::new_raw_xy(-1.0, n_side as f64 * 0.5)));
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
            let p = Point::new_raw_xy(x, y);
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

    #[test]
    fn degenerate_and_zero_length_edges_fail_open_without_grid() {
        let shape = Shape::Polygon(Polygon {
            shell: closed_ring(&[(0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0)]),
            holes: Arc::from([]),
        });
        let tester = PointBatchTester::new(&shape);
        for point in [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 1.0),
            Point::new_raw_xy(-1.0, 0.0),
        ] {
            assert_eq!(tester.contains_point(point), shape.contains_point(point));
            assert_eq!(tester.covers_point(point), shape.covers_point(point));
        }
    }
}
