use super::{
    ExpansionBudget, GeometryKind, MultiPoint, MultiPolygon, NonNegative, Polygon, Result,
    SegmentPlacement, Shape, remove_repeated_points, remove_repeated_ring_points, split,
};

impl Shape {
    /// Split the geometry into parts of at most `max_vertices` coordinates
    /// by selecting representable source-coordinate cuts across the longer
    /// axis and emitting both closed children from one exact partition (the
    /// `PostGIS` ``ST_Subdivide`` shape). Parts cover the input up to
    /// binary64 rounding at constructed cut vertices; simple inputs return as
    /// a single part. Linework with no representable rectangular cut is
    /// divided between source vertices. If exact partitioning cannot produce
    /// bounded parts, subdivision returns an unrepresentable-output error
    /// instead of emitting an oversized part.
    pub fn subdivide(&self, max_vertices: usize, drop: bool) -> Result<Vec<Self>> {
        self.subdivide_with_context(
            max_vertices,
            drop,
            &crate::geometry::SourceArcContext::cartesian(),
        )
    }

    pub(crate) fn subdivide_with_context(
        &self,
        max_vertices: usize,
        drop: bool,
        context: &crate::geometry::SourceArcContext<'_, '_>,
    ) -> Result<Vec<Self>> {
        let mut parts = Vec::new();
        let mut budget = ExpansionBudget::new("subdivide", "max_vertices");
        split(
            self.clone(),
            max_vertices,
            drop,
            context,
            &mut parts,
            &mut budget,
        )?;
        Ok(parts)
    }
}

impl Shape {
    pub fn remove_repeated_points(&self, tolerance: f64) -> Result<Self> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let remove_repeated_polygon = |polygon: &Polygon| -> Result<Option<Polygon>> {
            polygon.map_rings_collapsing(|ring| remove_repeated_ring_points(ring, tolerance))
        };
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => {
                if ![
                    Some(points.xs()),
                    Some(points.ys()),
                    points.zs(),
                    points.ms(),
                ]
                .into_iter()
                .flatten()
                .all(crate::geometry::column_all_finite)
                {
                    return Err(crate::geometry::GeometryErrorKind::NonFiniteCoordinate.into());
                }
                let cleaned = remove_repeated_points(points, tolerance)?;
                let mut cleaned = cleaned.iter();
                let members = points
                    .members()
                    .filter_map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Some(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(_) => {
                            cleaned.next().map(crate::geometry::MultiMember::Value)
                        },
                    })
                    .collect::<Vec<_>>();
                if members.is_empty() {
                    Self::typed_empty(GeometryKind::MultiPoint, points.axes())
                } else {
                    Self::MultiPoint(MultiPoint::try_new(points.axes(), members)?)
                }
            },
            Self::LineString(points) => Self::LineString(points.remove_repeated_points(tolerance)?),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .try_map_values(lines.axes(), |line| line.remove_repeated_points(tolerance))?,
            ),
            Self::Polygon(polygon) => remove_repeated_polygon(polygon)?.map_or_else(
                || Self::typed_empty(GeometryKind::Polygon, polygon.axes()),
                Self::Polygon,
            ),
            Self::MultiPolygon(polygons) => {
                let members = polygons
                    .members_with_values()
                    .map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Ok(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(polygon) => {
                            Ok(remove_repeated_polygon(polygon)?.map_or(
                                crate::geometry::MultiMember::Empty,
                                crate::geometry::MultiMember::Value,
                            ))
                        },
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self::MultiPolygon(MultiPolygon::try_new(polygons.axes(), members)?)
            },
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .try_map_members(|geometry| geometry.remove_repeated_points(tolerance))?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }

    /// Densify every segment chain by `fraction` (see `densify_points`) —
    /// the re-segmentation behind `segmentize(fraction=)` and the vertex
    /// refinement behind discrete Fréchet. Caller validates `fraction` in
    /// `(0, 1]` and names the budget after its public parameter.
    pub(crate) fn densified_budgeted(
        &self,
        fraction: f64,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        self.densified_budgeted_with_placement(fraction, &SegmentPlacement::Planar, budget)
    }

    pub(crate) fn densified_budgeted_with_placement(
        &self,
        fraction: f64,
        placement: &SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        Ok(match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => self.clone(),
            Self::LineString(points) => {
                Self::LineString(points.densify(fraction, placement, budget)?)
            },
            Self::MultiLineString(lines) => {
                Self::MultiLineString(lines.try_map_values(lines.axes(), |line| {
                    line.densify(fraction, placement, budget)
                })?)
            },
            Self::Polygon(polygon) => Self::Polygon(
                polygon.densified_budgeted_with_placement(fraction, placement, budget)?,
            ),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.try_map_values(polygons.axes(), |polygon| {
                    polygon.densified_budgeted_with_placement(fraction, placement, budget)
                })?)
            },
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.try_map_members(|geometry| {
                    geometry.densified_budgeted_with_placement(fraction, placement, budget)
                })?)
            },
        })
    }

    /// Planar `segmentize` — the CRS-free reading. CRS-aware callers resolve a
    /// [`SegmentPlacement`] first and use [`Self::segmentize_budgeted`].
    #[cfg(test)]
    pub fn segmentize(&self, max_segment_length: f64) -> Result<Self> {
        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        self.segmentize_budgeted(max_segment_length, &SegmentPlacement::Planar, &mut budget)
    }

    pub(crate) fn segmentize_budgeted(
        &self,
        max_segment_length: f64,
        placement: &SegmentPlacement<'_>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => Self::MultiPoint(points.clone()),
            Self::LineString(points) => {
                Self::LineString(points.segmentize(max_segment_length, placement, budget)?)
            },
            Self::MultiLineString(lines) => {
                Self::MultiLineString(lines.try_map_values(lines.axes(), |line| {
                    line.segmentize(max_segment_length, placement, budget)
                })?)
            },
            Self::Polygon(polygon) => {
                Self::Polygon(polygon.segmentize_budgeted(max_segment_length, placement, budget)?)
            },
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.try_map_values(polygons.axes(), |polygon| {
                    polygon.segmentize_budgeted(max_segment_length, placement, budget)
                })?)
            },
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.try_map_members(|geometry| {
                    geometry.segmentize_budgeted(max_segment_length, placement, budget)
                })?)
            },
            Self::Empty(..) => self.clone(),
        })
    }
}

#[cfg(test)]
mod tests;
