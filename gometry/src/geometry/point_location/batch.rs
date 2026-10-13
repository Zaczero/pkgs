use super::{
    ControlFlow, ExactDyadic, ExactPointTester, Loc, Point, PointMembership, Polygon,
    PolygonPointIndex, Result, Shape, SourcePointLocation, StreamingLocation, YStabbingIndex,
    cartesian_point_location_and_area, checked_index_count, exact_query_xy,
    indexed_parts_occupied_exact, location_from_indexed_parts,
};
use crate::HeapSize;

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
    /// Includes the complete contact path, which visits every hole of each
    /// selected part and can build at most two rays per candidate edge.
    pub(in crate::geometry) fn query_work_bound(&self) -> Option<usize> {
        fn polygon_work(polygon: &PolygonPointIndex) -> usize {
            std::iter::once(&polygon.shell)
                .chain(polygon.holes.items())
                .fold(1_usize, |work, ring| {
                    work.saturating_add(1)
                        .saturating_add(ring.edges.candidate_bound())
                })
        }
        match self {
            Self::Polygon { index } => Some(polygon_work(index)),
            Self::MultiPolygon { parts } => Some(
                parts
                    .items()
                    .iter()
                    .fold(parts.items().len(), |work, part| {
                        work.saturating_add(polygon_work(part))
                    }),
            ),
            Self::Generic(_) => None,
        }
    }

    #[cfg(test)]
    pub(super) const fn uses_indexed_plan(&self) -> bool {
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
    pub(crate) fn location_off_boundary(&self, x: f64, y: f64) -> Option<Loc> {
        let occupied = match self {
            Self::Polygon { index } => index.occupied_off_boundary(x, y)?,
            Self::MultiPolygon { parts } => {
                let mut occupied = false;
                let touched = parts.for_each_at_point(x, y, |part| {
                    part.occupied_off_boundary(x, y)
                        .map_or(ControlFlow::Break(()), |inside| {
                            occupied |= inside;
                            ControlFlow::Continue(())
                        })
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

    #[cfg(test)]
    pub(crate) fn location(&self, point: Point) -> Loc {
        self.location_and_area(point).location
    }

    pub(in crate::geometry) fn location_and_area(&self, point: Point) -> SourcePointLocation {
        if let Some(location) = self.location_off_boundary(point.x, point.y) {
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

    pub(crate) fn membership_xy(&self, x: f64, y: f64, membership: PointMembership) -> bool {
        if let Some(location) = self.location_off_boundary(x, y) {
            return membership.accepts(location);
        }
        let point = Point::new_raw_xy(x, y);
        match self {
            Self::Generic(tester) => match membership {
                PointMembership::Strict => membership.accepts(
                    crate::geometry::cartesian_point_location(&tester.shape, point),
                ),
                PointMembership::BoundaryInclusive => tester.covers_point(point),
            },
            Self::Polygon { index } => {
                membership.accepts(index.location_from_indexed_edges(point).location)
            },
            Self::MultiPolygon { parts } => {
                let location = StreamingLocation::for_stored_point(point);
                membership.accepts(
                    location_from_indexed_parts(parts, x, y, location)
                        .expect("stored point location does not construct exact values")
                        .location,
                )
            },
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
        self.membership_xy(point.x, point.y, PointMembership::Strict)
    }

    /// Boundary-inclusive membership — [`Shape::covers_point`] semantics.
    pub(crate) fn covers_point(&self, point: Point) -> bool {
        self.membership_xy(point.x, point.y, PointMembership::BoundaryInclusive)
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
