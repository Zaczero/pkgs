use super::{
    Bounds, ControlFlow, HeapSize, Loc, Point, PointQuery, Polygon, Result, RingClass,
    RingPointIndex, Shape, SourcePointLocation, StreamingLocation, YStabbingIndex,
    checked_index_count, point_in_bounds,
};

/// One polygon: shell minus holes, with hierarchical Y-stabbing on both levels.
pub(crate) struct PolygonPointIndex {
    pub(super) bounds: Option<Bounds>,
    pub(super) shell: RingPointIndex,
    pub(super) holes: YStabbingIndex<RingPointIndex>,
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
    pub(super) fn occupied_off_boundary(&self, x: f64, y: f64) -> Option<bool> {
        if !self.in_bounds_xy(x, y) {
            return Some(false);
        }
        if !self.winding_rings {
            return None;
        }
        let shell = self.shell.winding_off_boundary(x, y)?;
        if self.holes.items().is_empty() {
            return Some(shell != 0);
        }
        self.holes_occupied_off_boundary(x, y)
            .map(|hole_occupied| shell != 0 && !hole_occupied)
    }

    fn holes_occupied_off_boundary(&self, x: f64, y: f64) -> Option<bool> {
        let mut hole_occupied = false;
        let touched = self.holes.for_each_at_point(x, y, |hole| {
            hole.winding_off_boundary(x, y)
                .map_or(ControlFlow::Break(()), |winding| {
                    hole_occupied |= winding != 0;
                    ControlFlow::Continue(())
                })
        });
        touched.is_none().then_some(hole_occupied)
    }

    pub(super) fn occupied_off_boundary_query(
        &self,
        query: impl PointQuery,
    ) -> Result<Option<bool>> {
        if let Some(bounds) = self.bounds
            && !point_in_bounds(query, bounds)?
        {
            return Ok(Some(false));
        }
        if !self.winding_rings {
            return Ok(None);
        }
        let Some(shell) = self.shell.winding_off_boundary_query(query)? else {
            return Ok(None);
        };
        if shell == 0 {
            return Ok(Some(false));
        }
        let xy = query.index_xy();
        let mut hole_occupied = false;
        let mut error = None;
        let touched = self.holes.for_each_at_point(xy.x, xy.y, |hole| {
            match hole.winding_off_boundary_query(query) {
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

    pub(super) fn visit_indexed_into<Q: PointQuery>(
        &self,
        location: &mut StreamingLocation<Q>,
    ) -> Result<()> {
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

    pub(super) fn location_from_indexed_edges(&self, point: Point) -> SourcePointLocation {
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

    pub(crate) fn covers_point(&self, point: Point) -> bool {
        self.covers_xy(point.x, point.y)
    }

    pub(super) fn covers_xy(&self, x: f64, y: f64) -> bool {
        if !self.in_bounds_xy(x, y) {
            return false;
        }
        self.location(Point::new_raw_xy(x, y)) != RingClass::Exterior
    }

    pub(super) fn location(&self, point: Point) -> RingClass {
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

/// Source fallback for shapes without an indexed winding-ring plan.
pub(crate) struct ExactPointTester {
    pub(super) shape: Shape,
}

impl ExactPointTester {
    pub(super) fn covers_point(&self, point: Point) -> bool {
        crate::geometry::cartesian_point_covers(&self.shape, point)
    }
}

pub(super) fn location_from_indexed_parts<Q: PointQuery>(
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

pub(super) fn indexed_parts_occupied_query(
    parts: &YStabbingIndex<PolygonPointIndex>,
    query: impl PointQuery,
) -> Result<Option<bool>> {
    let xy = query.index_xy();
    let mut occupied = false;
    let mut unresolved = false;
    let mut error = None;
    let _ = parts.for_each_at_point(xy.x, xy.y, |part| {
        let result = part.occupied_off_boundary_query(query);
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
