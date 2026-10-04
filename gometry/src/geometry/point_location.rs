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
use crate::geometry::segment_index::{EdgeId, EdgeYIndex, YStabbingIndex, checked_index_count};
use crate::geometry::source_support::{
    PointQuery, RingEdgeCandidates, SourcePointLocation, StreamingLocation,
    cartesian_point_location_and_area, exact_edge_winding, exact_offset_edge_winding,
    exact_offset_point_in_bounds, exact_point_in_bounds, exact_query_xy, exact_query_y,
};
use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::{
    Bounds, CoordSeq, Loc, Point, Polygon, Ring, RingClass, RingView, Shape, SharedSlice, XY,
    edge_winding, ring_kind_columns, xy_bounds_columns,
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
    fn build(ring: &Ring) -> Result<Self> {
        let coords = ring.coords();
        Self::build_with_kind(coords, ring_kind_columns(coords.xs(), coords.ys()))
    }

    fn build_winding(ring: &Ring) -> Result<Option<Self>> {
        let coords = ring.coords();
        let kind = ring_kind_columns(coords.xs(), coords.ys());
        if kind == crate::geometry::RingKind::Collapsed {
            return Ok(None);
        }
        Self::build_with_kind(coords, kind).map(Some)
    }

    fn build_with_kind(coords: &CoordSeq, kind: crate::geometry::RingKind) -> Result<Self> {
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
        let next = e.wrapping_add(1);
        debug_assert_eq!(self.xs.len(), self.ys.len());
        debug_assert!(next < self.xs.len());
        // SAFETY: Rings repeat their first coordinate at the end. EdgeYIndex::build
        // emits only adjacent-pair ordinals from these equally sized columns,
        // so both the start and its successor lie inside the visible window.
        unsafe {
            (
                *self.xs.get_unchecked(e),
                *self.ys.get_unchecked(e),
                *self.xs.get_unchecked(next),
                *self.ys.get_unchecked(next),
            )
        }
    }

    /// Winding number at a probe that avoids the ring, read from the edges
    /// stabbed at its height; `None` when the probe lies on an edge.
    fn winding_off_boundary(&self, x: f64, y: f64) -> Option<i64> {
        let query = XY::new(x, y);
        let mut winding = 0_i64;
        let mut visit = |edge| {
            let (ax, ay, bx, by) = self.edge_xy(edge);
            edge_winding(XY::new(ax, ay), XY::new(bx, by), query).map_or(
                ControlFlow::Break(()),
                |delta| {
                    winding = winding.wrapping_add(delta);
                    ControlFlow::Continue(())
                },
            )
        };
        let touched = if let Some(edges) = self.edges.dense_band_edges(y) {
            let mut touched = None;
            for edge in edges {
                if visit(edge).is_break() {
                    touched = Some(());
                    break;
                }
            }
            touched
        } else {
            self.edges.for_each_edge(y, visit)
        };
        touched.is_none().then_some(winding)
    }

    fn winding_off_boundary_exact(&self, point: &[ExactDyadic; 3]) -> Result<Option<i64>> {
        self.winding_off_boundary_exact_query(point, None)
    }

    fn winding_off_boundary_exact_offset(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<Option<i64>> {
        self.winding_off_boundary_exact_query(point, Some(direction))
    }

    fn winding_off_boundary_exact_query(
        &self,
        point: &[ExactDyadic; 3],
        direction: Option<&[ExactDyadic; 2]>,
    ) -> Result<Option<i64>> {
        let in_bounds = match direction {
            Some(direction) => exact_offset_point_in_bounds(self.bounds, point, direction)?,
            None => exact_point_in_bounds(self.bounds, point)?,
        };
        if !in_bounds {
            return Ok(Some(0));
        }
        let direction_y = direction.map(|direction| {
            let [_, y] = direction;
            y
        });
        let y = exact_query_y(point, direction_y)?;
        let mut winding = 0_i64;
        let mut boundary = false;
        let mut error = None;
        let mut visit = |edge| {
            let (ax, ay, bx, by) = self.edge_xy(edge);
            let start = XY::new(ax, ay);
            let end = XY::new(bx, by);
            let contact = direction.map_or_else(
                || exact_edge_winding(start, end, point),
                |direction| exact_offset_edge_winding(start, end, point, direction),
            );
            match contact {
                Ok(Some(delta)) => {
                    winding = winding.wrapping_add(delta);
                    ControlFlow::Continue(())
                },
                Ok(None) => {
                    boundary = true;
                    ControlFlow::Break(())
                },
                Err(failure) => {
                    error = Some(failure);
                    ControlFlow::Break(())
                },
            }
        };
        if let Some(edges) = self.edges.dense_band_edges(y) {
            for edge in edges {
                if visit(edge).is_break() {
                    break;
                }
            }
        } else {
            self.edges.for_each_edge(y, visit);
        }
        if let Some(error) = error {
            return Err(error);
        }
        Ok((!boundary).then_some(winding))
    }
}

impl RingEdgeCandidates for RingPointIndex {
    fn ring_view(&self) -> RingView<'_> {
        self.view()
    }

    fn visit_stabbed_edges(
        &self,
        y: f64,
        visit: &mut dyn FnMut(XY, XY) -> Result<()>,
    ) -> Result<()> {
        let failure = self.edges.for_each_edge(y, |edge| {
            let (ax, ay, bx, by) = self.edge_xy(edge);
            match visit(XY::new(ax, ay), XY::new(bx, by)) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            }
        });
        failure.map_or(Ok(()), Err)
    }
}

impl HeapSize for RingPointIndex {
    fn heap_bytes(&self) -> usize {
        self.xs
            .heap_bytes()
            .saturating_add(self.ys.heap_bytes())
            .saturating_add(self.edges.heap_bytes())
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
        let hole_count = checked_index_count(polygon.holes.len())?;
        let shell = RingPointIndex::build(&polygon.shell)?;
        let mut winding_rings = shell.kind == crate::geometry::RingKind::Winding;
        let mut hole_rings = Vec::with_capacity(hole_count as usize);
        for hole in polygon.holes.iter() {
            let ring = RingPointIndex::build(hole)?;
            winding_rings &= ring.kind == crate::geometry::RingKind::Winding;
            hole_rings.push(ring);
        }
        Self::from_rings(shell, hole_rings, winding_rings)
    }

    pub(crate) fn build_areal(polygon: &Polygon) -> Result<Option<Self>> {
        let Some(shell) = RingPointIndex::build_winding(&polygon.shell)? else {
            return Ok(None);
        };
        let hole_count = checked_index_count(polygon.holes.len())?;
        let mut holes = Vec::with_capacity(hole_count as usize);
        for hole in polygon.holes.iter() {
            let Some(ring) = RingPointIndex::build_winding(hole)? else {
                return Ok(None);
            };
            holes.push(ring);
        }
        Self::from_rings(shell, holes, true).map(Some)
    }

    fn from_rings(
        shell: RingPointIndex,
        hole_rings: Vec<RingPointIndex>,
        winding_rings: bool,
    ) -> Result<Self> {
        let holes = YStabbingIndex::build(hole_rings, |ring| {
            (
                ring.bounds.minx(),
                ring.bounds.maxx(),
                ring.bounds.miny(),
                ring.bounds.maxy(),
            )
        })?;
        Ok(Self {
            bounds: Some(shell.bounds),
            shell,
            holes,
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

    fn occupied_off_boundary_exact(&self, point: &[ExactDyadic; 3]) -> Result<Option<bool>> {
        if let Some(bounds) = self.bounds
            && !exact_point_in_bounds(bounds, point)?
        {
            return Ok(Some(false));
        }
        if !self.winding_rings {
            return Ok(None);
        }
        let Some(shell) = self.shell.winding_off_boundary_exact(point)? else {
            return Ok(None);
        };
        if shell == 0 {
            return Ok(Some(false));
        }
        let query = exact_query_xy(point, None)?;
        let mut hole_occupied = false;
        let mut error = None;
        let touched = self.holes.for_each_at_point(query.x, query.y, |hole| {
            match hole.winding_off_boundary_exact(point) {
                Ok(Some(winding)) => {
                    hole_occupied |= winding != 0;
                    ControlFlow::Continue(())
                },
                Ok(None) => ControlFlow::Break(()),
                Err(failure) => {
                    error = Some(failure);
                    ControlFlow::Break(())
                },
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
        Ok(touched.is_none().then_some(!hole_occupied))
    }

    fn occupied_off_boundary_exact_offset(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<Option<bool>> {
        if let Some(bounds) = self.bounds
            && !exact_offset_point_in_bounds(bounds, point, direction)?
        {
            return Ok(Some(false));
        }
        if !self.winding_rings {
            return Ok(None);
        }
        let Some(shell) = self
            .shell
            .winding_off_boundary_exact_offset(point, direction)?
        else {
            return Ok(None);
        };
        if shell == 0 {
            return Ok(Some(false));
        }
        let query = exact_query_xy(point, Some(direction))?;
        let mut hole_occupied = false;
        let mut error = None;
        let touched = self.holes.for_each_at_point(query.x, query.y, |hole| {
            match hole.winding_off_boundary_exact_offset(point, direction) {
                Ok(Some(winding)) => {
                    hole_occupied |= winding != 0;
                    ControlFlow::Continue(())
                },
                Ok(None) => ControlFlow::Break(()),
                Err(failure) => {
                    error = Some(failure);
                    ControlFlow::Break(())
                },
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
        Ok(touched.is_none().then_some(!hole_occupied))
    }

    fn visit_indexed_into<Q: PointQuery>(&self, location: &mut StreamingLocation<Q>) -> Result<()> {
        let rings = std::iter::once(&self.shell).chain(self.holes.items().iter());
        location.visit_indexed_polygon_views(rings)
    }

    pub(in crate::geometry) fn location_and_area(&self, point: Point) -> SourcePointLocation {
        if let Some(occupied) = self.occupied_off_boundary(point.x, point.y) {
            return SourcePointLocation {
                location: if occupied {
                    Loc::Interior
                } else {
                    Loc::Exterior
                },
                area: occupied,
            };
        }
        self.location_from_indexed_edges(point)
    }

    fn location_from_indexed_edges(&self, point: Point) -> SourcePointLocation {
        let mut location = StreamingLocation::for_stored_point(point);
        self.visit_indexed_into(&mut location)
            .expect("stored point location does not construct exact values");
        location
            .finish()
            .expect("stored point location does not construct exact values")
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
        match self.location_and_area(point).location {
            Loc::Interior => RingClass::Interior,
            Loc::Boundary => RingClass::Boundary,
            Loc::Exterior => RingClass::Exterior,
        }
    }
}

impl HeapSize for PolygonPointIndex {
    fn heap_bytes(&self) -> usize {
        self.shell
            .heap_bytes()
            .saturating_add(self.holes.heap_bytes())
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
        crate::geometry::cartesian_point_covers(&self.shape, point)
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

fn location_from_indexed_parts<Q: PointQuery>(
    parts: &YStabbingIndex<PolygonPointIndex>,
    x: f64,
    y: f64,
    mut location: StreamingLocation<Q>,
) -> Result<SourcePointLocation> {
    let mut error = None;
    let _ = parts.for_each_at_point(x, y, |part| match part.visit_indexed_into(&mut location) {
        Ok(()) => ControlFlow::Continue(()),
        Err(failure) => {
            error = Some(failure);
            ControlFlow::Break(())
        },
    });
    if let Some(error) = error {
        return Err(error);
    }
    location.finish()
}

fn indexed_parts_occupied_exact(
    parts: &YStabbingIndex<PolygonPointIndex>,
    point: &[ExactDyadic; 3],
    direction: Option<&[ExactDyadic; 2]>,
) -> Result<Option<bool>> {
    let query = exact_query_xy(point, direction)?;
    let mut occupied = false;
    let mut unresolved = false;
    let mut error = None;
    let _ = parts.for_each_at_point(query.x, query.y, |part| {
        let result = direction.map_or_else(
            || part.occupied_off_boundary_exact(point),
            |direction| part.occupied_off_boundary_exact_offset(point, direction),
        );
        match result {
            Ok(Some(inside)) => {
                occupied |= inside;
                ControlFlow::Continue(())
            },
            Ok(None) => {
                unresolved = true;
                ControlFlow::Break(())
            },
            Err(failure) => {
                error = Some(failure);
                ControlFlow::Break(())
            },
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok((!unresolved).then_some(occupied))
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
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => Ok(Self::Generic(ExactPointTester {
                shape: shape.clone(),
            })),
        }
    }

    pub(crate) fn from_polygon(polygon: &Polygon) -> Result<Self> {
        if let Some(index) = PolygonPointIndex::build_areal(polygon)? {
            return Ok(Self::Polygon { index });
        }
        Ok(Self::Generic(ExactPointTester {
            shape: Shape::Polygon(polygon.clone()),
        }))
    }

    pub(crate) fn from_multipolygon(polygons: &crate::geometry::MultiPolygon) -> Result<Self> {
        if let [polygon] = polygons.as_slice() {
            return Self::from_polygon(polygon);
        }
        let part_count = checked_index_count(polygons.len())?;
        let mut built = Vec::with_capacity(part_count as usize);
        for polygon in polygons.iter() {
            let Some(index) = PolygonPointIndex::build_areal(polygon)? else {
                return Ok(Self::Generic(ExactPointTester {
                    shape: Shape::MultiPolygon(polygons.clone()),
                }));
            };
            built.push(index);
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

    pub(crate) fn location(&self, point: Point) -> Loc {
        self.location_and_area(point).location
    }

    pub(in crate::geometry) fn location_and_area(&self, point: Point) -> SourcePointLocation {
        if let Some(location) = self.location_off_boundary(point) {
            return SourcePointLocation {
                location,
                area: location == Loc::Interior,
            };
        }
        match self {
            Self::Polygon { index } => index.location_from_indexed_edges(point),
            Self::MultiPolygon { parts } => {
                let location = StreamingLocation::for_stored_point(point);
                location_from_indexed_parts(parts, point.x, point.y, location)
                    .expect("stored point location does not construct exact values")
            },
            Self::Generic(tester) => cartesian_point_location_and_area(&tester.shape, point),
        }
    }

    pub(in crate::geometry) fn location_and_area_exact(
        &self,
        point: &[ExactDyadic; 3],
    ) -> Result<Option<SourcePointLocation>> {
        let occupied = match self {
            Self::Polygon { index } => index.occupied_off_boundary_exact(point)?,
            Self::MultiPolygon { parts } => indexed_parts_occupied_exact(parts, point, None)?,
            Self::Generic(_) => return Ok(None),
        };
        if let Some(occupied) = occupied {
            return Ok(Some(SourcePointLocation {
                location: if occupied {
                    Loc::Interior
                } else {
                    Loc::Exterior
                },
                area: occupied,
            }));
        }

        let location = StreamingLocation::for_exact_point(point)?;
        match self {
            Self::Polygon { index } => {
                let mut location = location;
                index.visit_indexed_into(&mut location)?;
                Ok(Some(location.finish()?))
            },
            Self::MultiPolygon { parts } => {
                let query = exact_query_xy(point, None)?;
                Ok(Some(location_from_indexed_parts(
                    parts, query.x, query.y, location,
                )?))
            },
            Self::Generic(_) => Ok(None),
        }
    }

    pub(in crate::geometry) fn location_and_area_exact_offset(
        &self,
        point: &[ExactDyadic; 3],
        direction: &[ExactDyadic; 2],
    ) -> Result<Option<SourcePointLocation>> {
        let occupied = match self {
            Self::Polygon { index } => {
                index.occupied_off_boundary_exact_offset(point, direction)?
            },
            Self::MultiPolygon { parts } => {
                indexed_parts_occupied_exact(parts, point, Some(direction))?
            },
            Self::Generic(_) => return Ok(None),
        };
        if let Some(occupied) = occupied {
            return Ok(Some(SourcePointLocation {
                location: if occupied {
                    Loc::Interior
                } else {
                    Loc::Exterior
                },
                area: occupied,
            }));
        }

        let location = StreamingLocation::for_exact_offset(point, direction)?;
        match self {
            Self::Polygon { index } => {
                let mut location = location;
                index.visit_indexed_into(&mut location)?;
                Ok(Some(location.finish()?))
            },
            Self::MultiPolygon { parts } => {
                let query = exact_query_xy(point, Some(direction))?;
                Ok(Some(location_from_indexed_parts(
                    parts, query.x, query.y, location,
                )?))
            },
            Self::Generic(_) => Ok(None),
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
    use crate::geometry::source_support::{
        OccupiedToward, cartesian_exact_offset_location, cartesian_exact_offset_location_and_area,
        cartesian_exact_point_location, cartesian_exact_point_location_and_area,
    };
    use crate::geometry::{CoordWindow, LineSeq, MultiPolygon, ring_classify_point, same_point};

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

    fn dense_square_points() -> Vec<(f64, f64)> {
        let mut points = Vec::with_capacity(37);
        points.extend((0..=9).map(|x| (f64::from(x), 0.0)));
        points.extend((1..=9).map(|y| (10.0, f64::from(y))));
        points.extend((0..=8).rev().map(|x| (f64::from(x), 10.0)));
        points.extend((1..=8).rev().map(|y| (0.0, f64::from(y))));
        points.push((0.0, 0.0));
        points
    }

    fn dense_square() -> Shape {
        Shape::Polygon(Polygon {
            shell: closed_ring(&dense_square_points()),
            holes: SharedSlice::empty(),
        })
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
            MultiPolygon::from_dense(parts).unwrap(),
        ))
        .unwrap();
        assert!(tester.uses_indexed_plan());
    }

    #[test]
    fn explicitly_closed_ring_indexes_only_its_adjacent_edges() {
        let ring = closed_ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let index = RingPointIndex::build(&ring).unwrap();
        let mut edges = 0;
        index.edges.for_each_edge(0.0, |edge| {
            let (ax, ay, bx, by) = index.edge_xy(edge);
            assert!(!same_point(XY::new(ax, ay), XY::new(bx, by)));
            edges += 1;
            ControlFlow::<()>::Continue(())
        });
        assert_eq!(edges, 4);
        for (point, expected) in [
            (Point::new_raw_xy(0.0, 0.0), Loc::Boundary),
            (Point::new_raw_xy(0.0, 2.0), Loc::Boundary),
            (Point::new_raw_xy(2.0, 2.0), Loc::Interior),
            (Point::new_raw_xy(-1.0, 2.0), Loc::Exterior),
        ] {
            let polygon = Polygon::new(ring.clone(), Vec::new());
            assert_eq!(
                PolygonPointIndex::build(&polygon)
                    .unwrap()
                    .location_and_area(point)
                    .location,
                expected,
            );
        }
    }

    #[test]
    fn dense_ring_classification_matches_shape_kernel() {
        let points: Vec<_> = dense_square_points()
            .into_iter()
            .map(|(x, y)| XY::new(x, y))
            .collect();
        let coords = CoordSeq::try_from_xy(&points).unwrap();
        assert_eq!(coords.xs().len() - 1, 36);
        let ring = RingPointIndex::build(&Ring::from_closed(coords.clone()).unwrap()).unwrap();
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
        let ring = RingPointIndex::build(&Ring::from_closed(coords.clone()).unwrap()).unwrap();

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
        let ring = RingPointIndex::build(&Ring::from_closed(visible.clone()).unwrap()).unwrap();

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
        let ring = RingPointIndex::build(&Ring::from_closed(coords.clone()).unwrap()).unwrap();

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
        assert!(!tester.uses_indexed_plan());
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
                    let exact = cartesian_point_location_and_area(&shape, point);
                    assert_eq!(tester.location(point), exact.location, "{wkt} at {point:?}");
                    assert_eq!(
                        tester.location_and_area(point),
                        exact,
                        "area for {wkt} at {point:?}"
                    );
                    if let Some(location) = tester.location_off_boundary(point) {
                        assert_eq!(location, exact.location, "{wkt} at {point:?}");
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

    #[test]
    fn indexed_rational_locations_match_streaming_labels_and_area() {
        let shape = dense_square();
        let tester = PointBatchTester::new(&shape).unwrap();
        assert!(tester.uses_indexed_plan());

        for point in [
            [
                ExactDyadic::from_f64(1.0),
                ExactDyadic::from_f64(1.0),
                ExactDyadic::from_f64(3.0),
            ],
            [
                ExactDyadic::from_f64(1.0),
                ExactDyadic::from_f64(0.0),
                ExactDyadic::from_f64(3.0),
            ],
            [
                ExactDyadic::from_f64(20.0),
                ExactDyadic::from_f64(10.0),
                ExactDyadic::from_f64(2.0),
            ],
            [
                ExactDyadic::from_f64(22.0),
                ExactDyadic::from_f64(10.0),
                ExactDyadic::from_f64(2.0),
            ],
        ] {
            let expected = cartesian_exact_point_location_and_area(&shape, &point).unwrap();
            assert_eq!(
                tester.location_and_area_exact(&point).unwrap(),
                Some(expected)
            );
            assert_eq!(
                cartesian_exact_point_location(&shape, &point).unwrap(),
                expected.location
            );
        }

        let point = [
            ExactDyadic::from_f64(1.0),
            ExactDyadic::from_f64(1.0),
            ExactDyadic::from_f64(3.0),
        ];
        let expected = cartesian_exact_point_location_and_area(&shape, &point).unwrap();
        let plan = crate::geometry::PreparedPlan::transient(&shape);
        for _ in 0..4 {
            assert_eq!(plan.locate_exact(&point).unwrap(), expected);
        }
        assert!(
            plan.point_tester()
                .unwrap()
                .expect("large prepared polygon gets a point tester")
                .uses_indexed_plan()
        );
    }

    #[test]
    fn exact_offsets_use_indexed_edges_and_preserve_tangency() {
        let shape = dense_square();
        let point = [
            ExactDyadic::from_f64(1.0),
            ExactDyadic::from_f64(0.0),
            ExactDyadic::from_f64(3.0),
        ];
        let plan = crate::geometry::PreparedPlan::transient(&shape);
        for _ in 0..4 {
            let _ = plan.locate_exact(&point).unwrap();
        }

        for (direction, expected_toward) in [
            (
                [ExactDyadic::from_f64(0.0), ExactDyadic::from_f64(1.0)],
                OccupiedToward::Yes,
            ),
            (
                [ExactDyadic::from_f64(0.0), ExactDyadic::from_f64(-1.0)],
                OccupiedToward::No,
            ),
            (
                [ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(0.0)],
                OccupiedToward::Tangent,
            ),
        ] {
            let indexed = plan.locate_exact_offset(&point, &direction).unwrap();
            let streaming =
                cartesian_exact_offset_location_and_area(&shape, &point, &direction).unwrap();
            assert_eq!(indexed, streaming);
            assert_eq!(
                cartesian_exact_offset_location(&shape, &point, &direction).unwrap(),
                streaming.location
            );
            assert_eq!(
                plan.occupied_toward_exact(&point, &direction).unwrap(),
                expected_toward
            );
        }
    }

    #[test]
    fn prepared_location_schedule_keeps_small_polygons_streaming() {
        let shape = Shape::Polygon(unit_square());
        let point = [
            ExactDyadic::from_f64(0.5),
            ExactDyadic::from_f64(0.5),
            ExactDyadic::one(),
        ];
        let plan = crate::geometry::PreparedPlan::transient(&shape);
        for _ in 0..4 {
            assert_eq!(plan.locate_exact(&point).unwrap().location, Loc::Interior);
        }
        assert!(plan.point_tester().unwrap().is_none());
    }
}
