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
#[cfg(test)]
use std::sync::Arc;

use crate::HeapSize;
use crate::error::Result;
use crate::geometry::segment_index::{EdgeId, EdgeYIndex, YStabbingIndex};
use crate::geometry::{
    Bounds, CoordSeq, Loc, Point, Polygon, Ring, RingClass, RingView, Shape, SharedSlice, XY,
    cartesian_polygon_views_location, edge_winding, ring_kind_columns, xy_bounds_columns,
};

/// One closed ring's Y-indexed edges for prepared point membership.
pub(crate) struct RingPointIndex {
    bounds: Bounds,
    xs: SharedSlice<f64>,
    ys: SharedSlice<f64>,
    kind: crate::geometry::RingKind,
    /// Edge start indices (`0..n-1`) stabbed by Y — dense CSR stores edge
    /// ids directly (no items-vector indirection).
    edges: EdgeYIndex,
}

impl RingPointIndex {
    fn build(coords: &CoordSeq) -> Result<Self> {
        let xs = coords.xs();
        let ys = coords.ys();
        let n = xs.len();
        debug_assert_eq!(n, ys.len());
        let kind = ring_kind_columns(xs, ys);
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
        Ok(Self {
            bounds,
            xs: coords.carried_xs(),
            ys: coords.carried_ys(),
            kind,
            edges: EdgeYIndex::build(ys)?,
        })
    }

    fn view(&self) -> RingView<'_> {
        RingView {
            xs: &self.xs,
            ys: &self.ys,
            kind: self.kind,
        }
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

    /// Winding number at a probe that avoids the ring, read from the edges
    /// stabbed at its height; `None` when the probe lies on an edge.
    fn winding_off_boundary(&self, x: f64, y: f64) -> Option<i64> {
        let query = XY::new(x, y);
        let mut winding = 0_i64;
        let touched = self.edges.for_each_edge(y, |edge| {
            let (ax, ay, bx, by) = self.edge_xy(edge);
            edge_winding(XY::new(ax, ay), XY::new(bx, by), query).map_or(
                ControlFlow::Break(()),
                |delta| {
                    winding += delta;
                    ControlFlow::Continue(())
                },
            )
        });
        touched.is_none().then_some(winding)
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
    /// Every ring carries areal winding; a collapsed ring is line support.
    winding_rings: bool,
}

impl PolygonPointIndex {
    pub(crate) fn build(polygon: &Polygon) -> Result<Self> {
        let mut index = Self::build_shell(&polygon.shell)?;
        let mut hole_rings = Vec::new();
        crate::try_reserve_vec(&mut hole_rings, polygon.holes.len())?;
        for hole in polygon.holes.iter() {
            let ring = RingPointIndex::build(hole.coords())?;
            index.winding_rings &= ring.kind == crate::geometry::RingKind::Winding;
            hole_rings.push(ring);
        }
        index.holes = YStabbingIndex::build(hole_rings, |ring| {
            (
                ring.bounds.minx(),
                ring.bounds.maxx(),
                ring.bounds.miny(),
                ring.bounds.maxy(),
            )
        })?;
        Ok(index)
    }

    pub(in crate::geometry) fn build_shell(shell: &Ring) -> Result<Self> {
        let shell_index = RingPointIndex::build(shell.coords())?;
        let winding_rings = shell_index.kind == crate::geometry::RingKind::Winding;
        Ok(Self {
            bounds: Some(shell_index.bounds),
            shell: shell_index,
            holes: YStabbingIndex::build(Vec::new(), |_| {
                (
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                )
            })?,
            winding_rings,
        })
    }

    /// Whether a probe that avoids every ring edge is interior; `None` when it
    /// touches an edge or a ring is collapsed, which the exact source scan
    /// resolves.
    fn occupied_off_boundary(&self, x: f64, y: f64) -> Option<bool> {
        if !self.in_bounds_xy(x, y) {
            return Some(false);
        }
        if !self.winding_rings {
            return None;
        }
        let shell = self.shell.winding_off_boundary(x, y)?;
        let mut hole_occupied = false;
        let touched = self.holes.for_each_at_point(x, y, |hole| {
            hole.winding_off_boundary(x, y)
                .map_or(ControlFlow::Break(()), |winding| {
                    hole_occupied |= winding != 0;
                    ControlFlow::Continue(())
                })
        });
        touched.is_none().then_some(shell != 0 && !hole_occupied)
    }

    fn in_bounds_xy(&self, x: f64, y: f64) -> bool {
        self.bounds.is_some_and(|bounds| {
            x >= bounds.minx() && x <= bounds.maxx() && y >= bounds.miny() && y <= bounds.maxy()
        })
    }

    /// Strict membership after the envelope candidate gate.
    #[cfg(test)]
    pub(crate) fn contains_xy(&self, x: f64, y: f64) -> bool {
        if !self.in_bounds_xy(x, y) {
            return false;
        }
        self.location(Point::new_raw_xy(x, y)) == RingClass::Interior
    }

    pub(crate) fn covers_point(&self, point: Point) -> bool {
        self.covers_xy(point.x, point.y)
    }

    fn covers_xy(&self, x: f64, y: f64) -> bool {
        if !self.in_bounds_xy(x, y) {
            return false;
        }
        self.location(Point::new_raw_xy(x, y)) != RingClass::Exterior
    }

    fn location(&self, point: Point) -> RingClass {
        if let Some(occupied) = self.occupied_off_boundary(point.x, point.y) {
            return if occupied {
                RingClass::Interior
            } else {
                RingClass::Exterior
            };
        }
        let mut hole_indices = Vec::new();
        let _: Option<()> = self
            .holes
            .for_each_at_point_index(point.x, point.y, |index| {
                hole_indices.push(index);
                ControlFlow::Continue(())
            });
        let rings =
            std::iter::once(self.shell.view()).chain(hole_indices.into_iter().map(|index| {
                self.holes
                    .get(index)
                    .expect("point index returned an invalid item index")
                    .view()
            }));
        let location = cartesian_polygon_views_location([rings], point);
        match location {
            crate::geometry::Loc::Interior => RingClass::Interior,
            crate::geometry::Loc::Boundary => RingClass::Boundary,
            crate::geometry::Loc::Exterior => RingClass::Exterior,
        }
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
pub(crate) struct ExactPointTester {
    shape: Shape,
}

impl ExactPointTester {
    #[cfg(test)]
    fn contains_point(&self, point: Point) -> bool {
        crate::geometry::cartesian_point_location(&self.shape, point)
            == crate::geometry::Loc::Interior
    }

    fn covers_point(&self, point: Point) -> bool {
        crate::geometry::cartesian_point_location(&self.shape, point)
            != crate::geometry::Loc::Exterior
    }
}

/// Union membership over the parts stabbed at `point`.
fn any_part_at(
    parts: &YStabbingIndex<PolygonPointIndex>,
    point: Point,
    accepts: impl Fn(&PolygonPointIndex) -> bool,
) -> bool {
    parts
        .for_each_at_point(point.x, point.y, |polygon| {
            if accepts(polygon) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_some()
}

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
    Generic(ExactPointTester),
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

    pub(crate) fn new(shape: &Shape) -> Result<Self> {
        match shape {
            Shape::Polygon(polygon) => Self::from_polygon(polygon),
            Shape::MultiPolygon(polygons) => Self::from_multipolygon(polygons),
            _ => Ok(Self::Generic(ExactPointTester {
                shape: shape.clone(),
            })),
        }
    }

    pub(crate) fn from_polygon(polygon: &Polygon) -> Result<Self> {
        Ok(Self::Polygon {
            index: PolygonPointIndex::build(polygon)?,
        })
    }

    pub(crate) fn from_multipolygon(polygons: &crate::geometry::MultiPolygon) -> Result<Self> {
        if polygons.len() == 1 {
            return Self::from_polygon(&polygons[0]);
        }
        let mut built = Vec::with_capacity(polygons.len());
        for polygon in polygons.iter() {
            built.push(PolygonPointIndex::build(polygon)?);
        }
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
        })?;
        Ok(Self::MultiPolygon { parts })
    }

    /// Location of a probe that avoids every ring edge, from indexed exact
    /// windings. `None` for a probe on an edge, a collapsed ring, or a shape
    /// that is not polygonal: the exact source scan owns those.
    pub(crate) fn location_off_boundary(&self, point: Point) -> Option<Loc> {
        let occupied = match self {
            Self::Polygon { index } => index.occupied_off_boundary(point.x, point.y)?,
            Self::MultiPolygon { parts } => {
                let mut occupied = false;
                let touched = parts.for_each_at_point(point.x, point.y, |part| {
                    part.occupied_off_boundary(point.x, point.y).map_or(
                        ControlFlow::Break(()),
                        |inside| {
                            occupied |= inside;
                            ControlFlow::Continue(())
                        },
                    )
                });
                if touched.is_some() {
                    return None;
                }
                occupied
            },
            Self::Generic(_) => return None,
        };
        Some(if occupied {
            Loc::Interior
        } else {
            Loc::Exterior
        })
    }

    /// Exact location of any probe. Off every edge the indexed windings
    /// answer; on an edge the exact local star decides from the stored ring
    /// views, so no ring is re-classified per probe. Only parts whose
    /// envelope holds the probe contribute: every other part has zero winding
    /// there and no incident edge.
    pub(crate) fn location(&self, point: Point) -> Loc {
        if let Some(location) = self.location_off_boundary(point) {
            return location;
        }
        match self {
            Self::Polygon { index } => match index.location(point) {
                RingClass::Interior => Loc::Interior,
                RingClass::Boundary => Loc::Boundary,
                RingClass::Exterior => Loc::Exterior,
            },
            Self::MultiPolygon { parts } => {
                let mut near = Vec::new();
                let _: Option<()> = parts.for_each_at_point_index(point.x, point.y, |index| {
                    near.push(index);
                    ControlFlow::Continue(())
                });
                cartesian_polygon_views_location(
                    near.into_iter().map(|index| {
                        let part = parts
                            .get(index)
                            .expect("point index returned an invalid item");
                        std::iter::once(part.shell.view())
                            .chain(part.holes.items().iter().map(RingPointIndex::view))
                    }),
                    point,
                )
            },
            Self::Generic(tester) => {
                crate::geometry::cartesian_point_location(&tester.shape, point)
            },
        }
    }

    /// Strict membership — [`Shape::contains_point`] semantics.
    #[cfg(test)]
    pub(crate) fn contains_point(&self, point: Point) -> bool {
        match self {
            Self::Polygon { index: polygon, .. } => polygon.contains_xy(point.x, point.y),
            Self::MultiPolygon { parts, .. } => any_part_at(parts, point, |polygon| {
                polygon.contains_xy(point.x, point.y)
            }),
            Self::Generic(tester) => tester.contains_point(point),
        }
    }

    /// Boundary-inclusive membership — [`Shape::covers_point`] semantics.
    pub(crate) fn covers_point(&self, point: Point) -> bool {
        match self {
            Self::Polygon { index: polygon, .. } => polygon.covers_xy(point.x, point.y),
            Self::MultiPolygon { parts, .. } => {
                any_part_at(parts, point, |polygon| polygon.covers_xy(point.x, point.y))
            },
            Self::Generic(tester) => tester.covers_point(point),
        }
    }
}

impl HeapSize for PointBatchTester {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Polygon { index: polygon } => polygon.heap_bytes(),
            Self::MultiPolygon { parts } => parts.heap_bytes(),
            Self::Generic(tester) => tester.shape.heap_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordWindow, LineSeq, MultiPolygon, ring_classify_point};

    fn winding_class(ring: &RingPointIndex, x: f64, y: f64) -> RingClass {
        match ring.winding_off_boundary(x, y) {
            None => RingClass::Boundary,
            Some(0) => RingClass::Exterior,
            Some(_) => RingClass::Interior,
        }
    }

    fn closed_ring(pts: &[(f64, f64)]) -> Ring {
        let xy: Vec<XY> = pts.iter().map(|&(x, y)| XY::new(x, y)).collect();
        Ring::from_closed(CoordSeq::try_from_xy(&xy).unwrap()).unwrap()
    }

    fn unit_square() -> Polygon {
        Polygon {
            shell: closed_ring(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
            holes: SharedSlice::empty(),
        }
    }

    #[test]
    fn point_probe_policy_indexes_polygon_hierarchy() {
        let mut polygon = unit_square();
        polygon.holes = SharedSlice::from_vec(
            std::iter::repeat_with(|| {
                closed_ring(&[(2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 3.0), (2.0, 2.0)])
            })
            .take(33)
            .collect::<Vec<_>>(),
        );
        let tester = PointBatchTester::new(&Shape::Polygon(polygon)).unwrap();
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
                    holes: SharedSlice::empty(),
                }
            })
            .collect::<Vec<_>>();
        let tester = PointBatchTester::new(&Shape::MultiPolygon(
            MultiPolygon::from_dense(parts).unwrap().unwrap(),
        ))
        .unwrap();
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
        let coords = CoordSeq::try_from_xy(&points).unwrap();
        assert_eq!(coords.xs().len() - 1, 36);
        let ring = RingPointIndex::build(&coords).unwrap();
        assert!(ring.edges.dense_band_edges(5.0).is_some());

        for (x, y) in [(5.0, 0.0), (5.0, 5.0), (9.0, 5.0), (-1.0, 5.0)] {
            assert_eq!(
                winding_class(&ring, x, y),
                ring_classify_point(&coords, XY::new(x, y).point()),
                "classification mismatch at ({x}, {y})"
            );
        }
    }

    #[test]
    fn full_window_ring_index_shares_xy_and_survives_source_drop() {
        let coords = CoordSeq::try_from_vecs(
            [0.0, 2.0, 2.0, 0.0, 0.0].into(),
            [0.0, 0.0, 2.0, 2.0, 0.0].into(),
            None,
            None,
        )
        .unwrap();
        let columns = coords.column_arcs();
        let source_xs = columns.xs.owner().unwrap();
        let source_ys = columns.ys.owner().unwrap();
        let ring = RingPointIndex::build(&coords).unwrap();

        assert!(Arc::ptr_eq(&ring.xs.owner().unwrap(), &source_xs));
        assert!(Arc::ptr_eq(&ring.ys.owner().unwrap(), &source_ys));

        drop(columns);
        drop(coords);
        drop(source_xs);
        drop(source_ys);

        assert_eq!(winding_class(&ring, 1.0, 1.0), RingClass::Interior);
        assert_eq!(winding_class(&ring, 0.0, 1.0), RingClass::Boundary);
        assert_eq!(winding_class(&ring, 3.0, 1.0), RingClass::Exterior);
    }

    #[test]
    fn windowed_ring_index_reads_visible_coordinates() {
        let source_xs = SharedSlice::from_vec(vec![1000.0, 0.0, 2.0, 2.0, 0.0, 0.0, -1000.0]);
        let source_ys = SharedSlice::from_vec(vec![1000.0, 0.0, 0.0, 2.0, 2.0, 0.0, -1000.0]);
        let source_xs_weak = Arc::downgrade(&source_xs.owner().unwrap());
        let source_ys_weak = Arc::downgrade(&source_ys.owner().unwrap());
        let coords =
            CoordSeq::from_shared_columns(source_xs.clone(), source_ys.clone(), None, None)
                .unwrap();
        let visible = coords.view(CoordWindow::from_range(1..6, coords.len()));
        let ring = RingPointIndex::build(&visible).unwrap();

        drop(visible);
        drop(coords);
        drop(source_xs);
        drop(source_ys);
        assert!(source_xs_weak.upgrade().is_none());
        assert!(source_ys_weak.upgrade().is_none());

        assert_eq!(winding_class(&ring, 1.0, 1.0), RingClass::Interior);
        assert_eq!(winding_class(&ring, 0.0, 1.0), RingClass::Boundary);
        assert_eq!(winding_class(&ring, 3.0, 1.0), RingClass::Exterior);
    }

    #[test]
    fn xyzm_ring_index_reads_xy_columns() {
        let coords = CoordSeq::try_from_vecs(
            [0.0, 2.0, 2.0, 0.0, 0.0].into(),
            [0.0, 0.0, 2.0, 2.0, 0.0].into(),
            Some([10.0, 11.0, 12.0, 13.0, 10.0].into()),
            Some([20.0, 21.0, 22.0, 23.0, 20.0].into()),
        )
        .unwrap();
        let columns = coords.column_arcs();
        let z_weak = Arc::downgrade(&columns.zs.as_ref().expect("Z column").owner().unwrap());
        let m = columns.m_parts().expect("M column").0;
        let m_weak = Arc::downgrade(&m.owner().unwrap());
        let ring = RingPointIndex::build(&coords).unwrap();

        assert!(Arc::ptr_eq(
            &ring.xs.owner().unwrap(),
            &columns.xs.owner().unwrap()
        ));
        assert!(Arc::ptr_eq(
            &ring.ys.owner().unwrap(),
            &columns.ys.owner().unwrap()
        ));
        drop(m);
        drop(columns);
        drop(coords);
        assert!(z_weak.upgrade().is_none());
        assert!(m_weak.upgrade().is_none());
        assert_eq!(winding_class(&ring, 1.0, 1.0), RingClass::Interior);
        assert_eq!(winding_class(&ring, 0.0, 1.0), RingClass::Boundary);
        assert_eq!(winding_class(&ring, 3.0, 1.0), RingClass::Exterior);
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
            holes: SharedSlice::from_vec(vec![closed_ring(&[
                (3.0, 3.0),
                (7.0, 3.0),
                (7.0, 7.0),
                (3.0, 7.0),
                (3.0, 3.0),
            ])]),
        }
    }

    #[test]
    fn contains_and_covers_match_shape_kernels() {
        let poly = square_with_hole();
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape).unwrap();
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
        }
    }

    #[test]
    fn multipolygon_is_union_not_xor() {
        let a = unit_square();
        let b = Polygon {
            shell: closed_ring(&[(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5), (0.5, 0.5)]),
            holes: SharedSlice::empty(),
        };
        let shape = Shape::MultiPolygon(vec![a, b].into());
        let tester = PointBatchTester::new(&shape).unwrap();
        let overlap = Point::new_raw_xy(0.75, 0.75);
        assert!(tester.contains_point(overlap));
        let out = Point::new_raw_xy(3.0, 3.0);
        assert!(!tester.contains_point(out));
    }

    #[test]
    fn hole_envelope_skips_non_covering_holes() {
        let poly = square_with_hole();
        let tester = PointBatchTester::new(&Shape::Polygon(poly)).unwrap();
        let p = Point::new_raw_xy(1.0, 9.0);
        assert!(tester.contains_point(p));
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
            shell: Ring::from_closed(CoordSeq::try_from_xy(&pts).unwrap()).unwrap(),
            holes: SharedSlice::empty(),
        };
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape).unwrap();
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
            holes: SharedSlice::from_vec(holes),
        };
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape).unwrap();
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
            holes: SharedSlice::empty(),
        });
        let tester = PointBatchTester::new(&shape).unwrap();
        for point in [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 1.0),
            Point::new_raw_xy(-1.0, 0.0),
        ] {
            assert_eq!(tester.contains_point(point), shape.contains_point(point));
            assert_eq!(tester.covers_point(point), shape.covers_point(point));
        }
    }

    #[test]
    fn generic_point_tester_uses_exact_source_incidence() {
        let line = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(2.0, 2.0)]).unwrap(),
        ));
        let tester = PointBatchTester::new(&line).unwrap();

        assert!(tester.covers_point(Point::new_raw_xy(1.0, 1.0)));
        assert!(tester.covers_point(Point::new_raw_xy(0.0, 0.0)));
        assert!(!tester.covers_point(Point::new_raw_xy(1.0, 1.1)));
        assert!(!tester.contains_point(Point::new_raw_xy(0.0, 0.0)));
        assert!(tester.contains_point(Point::new_raw_xy(1.0, 1.0)));
    }

    #[test]
    fn indexed_point_tester_uses_composed_source_support() {
        for (wkt, point, contains) in [
            (
                "POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))",
                Point::new_raw_xy(2.0, 2.0),
                true,
            ),
            (
                "POLYGON ((0 0,2 0,1 0,0 0))",
                Point::new_raw_xy(1.0, 0.0),
                true,
            ),
            (
                "POLYGON ((0 0,4 0,4 4,0 4,0 0),(8 8,9 8,9 9,8 9,8 8))",
                Point::new_raw_xy(2.0, 2.0),
                true,
            ),
        ] {
            let shape = crate::io::parse_wkt(wkt).unwrap();
            let tester = PointBatchTester::new(&shape).unwrap();
            assert_eq!(tester.contains_point(point), contains);
            assert_eq!(tester.covers_point(point), contains);
        }
    }

    /// The tester's location must agree with the exact source scan on every
    /// probe; its indexed off-boundary answer defers on every edge contact.
    #[test]
    fn indexed_locations_agree_with_the_exact_scan() {
        use crate::geometry::Coordinates as _;

        for wkt in [
            "POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))",
            "POLYGON ((0 0,4 0,4 4,0 4,0 0,-2 0,0 0))",
            "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))",
            "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,1 1,1 1,1 1))",
            "POLYGON ((0 0,4 4,4 0,0 4,0 0))",
            "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),((2 2,4 2,4 4,2 4,2 2)))",
            "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((1 1,3 1,3 3,1 3,1 1)))",
            "MULTIPOLYGON (((0 0,2 0,2 4,0 4,0 0)),((2 0,4 0,4 4,2 4,2 0)))",
            "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0),(0.5 0.5,1 0.5,1 1,0.5 1,0.5 0.5)),((2 0,4 0,4 4,2 4,2 0)))",
        ] {
            let shape = crate::io::parse_wkt(wkt).unwrap();
            let tester = PointBatchTester::new(&shape).unwrap();
            for step_x in -2..=18 {
                for step_y in -2..=18 {
                    let point = Point::new_raw_xy(f64::from(step_x) / 4.0, f64::from(step_y) / 4.0);
                    let exact = crate::geometry::cartesian_point_location(&shape, point);
                    assert_eq!(tester.location(point), exact, "{wkt} at {point:?}");
                    if let Some(location) = tester.location_off_boundary(point) {
                        assert_eq!(location, exact, "{wkt} at {point:?}");
                    } else {
                        let mut on_edge = false;
                        shape.for_each_segment_chain(|chain| {
                            on_edge |= chain.segment_pairs().any(|[start, end]| {
                                crate::geometry::point_on_segment(point, start, end)
                            });
                        });
                        assert!(
                            on_edge || wkt.contains("1 1,1 1"),
                            "{wkt} deferred an off-boundary probe at {point:?}"
                        );
                    }
                }
            }
        }
    }
}
