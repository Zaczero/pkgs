use crate::error::Result;
use crate::geometry::predicates::{line_contains_point, multiline_contains_point};
use crate::geometry::{
    CompiledPattern, Coordinates as _, GeometryErrorKind, Point, PreparedPlan, Shape,
    graph_relate_shapes, point_equals_exact, point_on_segment, points_equal_exact, same_point,
};
use crate::predicates::engine::{PreparedTopologyPair, topology_scalar_pattern};

impl Shape {
    /// Strict point membership used by coordinate operations and geometry
    /// algorithms. Topological predicates use the complete relation matrix.
    pub fn contains_point(&self, point: Point) -> Result<bool> {
        self.point_membership::<false>(point)
    }

    pub fn covers_point(&self, point: Point) -> Result<bool> {
        self.point_membership::<true>(point)
    }

    /// One point-membership skeleton for both spellings: `BOUNDARY`
    /// selects boundary-inclusive `covers` semantics over interior-only
    /// `contains` in the three arms where they differ — the const flag
    /// keeps a single exhaustive match, so a new `Shape` variant cannot
    /// update one predicate and silently miss its twin.
    fn point_membership<const BOUNDARY: bool>(&self, point: Point) -> Result<bool> {
        Ok(match self {
            Self::Point(value) => same_point(*value, point),
            Self::MultiPoint(points) => points.iter().any(|value| same_point(value, point)),
            Self::LineString(points) => {
                if BOUNDARY {
                    points
                        .segment_pairs()
                        .any(|[start, end]| point_on_segment(point, start, end))
                } else {
                    line_contains_point(points, point)
                }
            },
            Self::MultiLineString(lines) => {
                if BOUNDARY {
                    lines.iter().any(|line| {
                        line.segment_pairs()
                            .any(|[start, end]| point_on_segment(point, start, end))
                    })
                } else {
                    multiline_contains_point(lines, point)
                }
            },
            Self::Polygon(polygon) => {
                if BOUNDARY {
                    polygon.covers_point(point)
                } else {
                    polygon.contains_point(point)
                }
            },
            Self::MultiPolygon(polygons) => polygons.iter().any(|polygon| {
                if BOUNDARY {
                    polygon.covers_point(point)
                } else {
                    polygon.contains_point(point)
                }
            }),
            Self::GeometryCollection(_) => {
                let matrix = graph_relate_shapes(self, &Self::Point(point.to_xy()))?;
                if BOUNDARY {
                    matrix.is_covers()
                } else {
                    matrix.is_contains()
                }
            },
            Self::Empty(..) => false,
        })
    }

    pub fn contains(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.is_contains())
    }

    /// `contains` with no boundary contact: *other* lies in the interior of
    /// *self* and never touches its boundary (DE-9IM `T**FF*FF*`).
    pub fn contains_properly(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.is_contains_properly())
    }

    pub fn within(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.transpose().is_contains())
    }

    pub fn covers(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.is_covers())
    }

    pub fn covered_by(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.transpose().is_covers())
    }

    pub fn intersects(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.is_intersects())
    }

    /// Visit one representative vertex per connected 1D/2D component (first
    /// vertex of each line/shell), short-circuiting on the first `true`.
    /// With no boundary contact between two geometries, each component is
    /// uniformly inside or outside the other, so its representative decides
    /// area containment for the whole component.
    pub(crate) fn any_component_representative(
        &self,
        predicate: &mut impl FnMut(Point) -> bool,
    ) -> bool {
        match self {
            Self::Point(point) => predicate(*point),
            Self::MultiPoint(points) => points.iter().any(&mut *predicate),
            Self::LineString(points) => points.first().is_some_and(&mut *predicate),
            Self::MultiLineString(lines) => lines
                .iter()
                .any(|line| line.first().is_some_and(&mut *predicate)),
            Self::Polygon(polygon) => polygon.shell.first().is_some_and(&mut *predicate),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .any(|polygon| polygon.shell.first().is_some_and(&mut *predicate)),
            Self::GeometryCollection(geometries) => geometries
                .iter()
                .any(|geometry| geometry.any_component_representative(predicate)),
            Self::Empty(..) => false,
        }
    }

    /// Whether any part carries area (polygon parts, recursively through
    /// collections) — gates the representative-containment phase.
    pub(crate) fn has_area_parts(&self) -> bool {
        match self {
            Self::Polygon(_) | Self::MultiPolygon(_) => true,
            Self::GeometryCollection(geometries) => geometries.iter().any(Self::has_area_parts),
            _ => false,
        }
    }

    /// Boundary-inclusive point-in-AREA test: `covers_point` restricted to
    /// the polygon parts (1D/0D parts cannot contain a disjoint component).
    pub(crate) fn area_covers_point(&self, point: Point) -> bool {
        match self {
            Self::Polygon(polygon) => polygon.covers_point(point),
            Self::MultiPolygon(polygons) => {
                polygons.iter().any(|polygon| polygon.covers_point(point))
            },
            Self::GeometryCollection(geometries) => geometries
                .iter()
                .any(|geometry| geometry.area_covers_point(point)),
            _ => false,
        }
    }

    pub fn disjoint(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.is_disjoint())
    }

    pub fn touches(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.is_touches())
    }

    pub fn crosses(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.is_crosses_by_dimension())
    }

    pub fn overlaps(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.is_overlaps_by_dimension())
    }

    pub fn relate(&self, other: &Self) -> Result<String> {
        Ok(graph_relate_shapes(self, other)?.text())
    }
}

impl Shape {
    pub fn relate_pattern(&self, other: &Self, pattern: &str) -> Result<bool> {
        let compiled =
            CompiledPattern::new(pattern).ok_or_else(|| Self::invalid_de9im_pattern(pattern))?;
        let left = PreparedPlan::transient(self)?;
        let right = PreparedPlan::transient(other)?;
        let pair = PreparedTopologyPair::Cartesian {
            left: &left,
            right: &right,
        };
        topology_scalar_pattern(compiled, &pair)
    }

    fn invalid_de9im_pattern(pattern: &str) -> crate::error::Error {
        GeometryErrorKind::invalid_de9im_pattern(format!(
            "invalid DE-9IM pattern {pattern:?}; expected 9 characters of \
             'T', 'F', '0', '1', '2', or '*'"
        ))
    }

    pub fn equals(&self, other: &Self) -> Result<bool> {
        Ok(graph_relate_shapes(self, other)?.is_equal_topo())
    }

    /// Coordinate equality within `tolerance` (per-ordinate `|Δ| ≤ tolerance`,
    /// JTS/Shapely `equals_exact` semantics): the structure must match and
    /// every paired ordinate must agree to `tolerance`. `tolerance == 0.0`
    /// is exact. CRS/epoch are ignored here — that is the job of `==`.
    pub fn equals_exact(
        &self,
        other: &Self,
        tolerance: f64,
        include_z: bool,
        include_m: bool,
    ) -> bool {
        match (include_z, include_m) {
            (false, false) => self.equals_exact_impl::<false, false>(other, tolerance),
            (true, false) => self.equals_exact_impl::<true, false>(other, tolerance),
            (false, true) => self.equals_exact_impl::<false, true>(other, tolerance),
            (true, true) => self.equals_exact_impl::<true, true>(other, tolerance),
        }
    }

    pub(crate) fn equals_exact_impl<const Z: bool, const M: bool>(
        &self,
        other: &Self,
        tolerance: f64,
    ) -> bool {
        match (self, other) {
            (Self::Point(left), Self::Point(right)) => {
                point_equals_exact::<Z, M>(*left, *right, tolerance)
            },
            (Self::MultiPoint(multipoint_left), Self::MultiPoint(multipoint_right)) => {
                (!Z || multipoint_left.axes().has_z() == multipoint_right.axes().has_z())
                    && (!M || multipoint_left.axes().has_m() == multipoint_right.axes().has_m())
                    && multipoint_left.len() == multipoint_right.len()
                    && multipoint_left
                        .members()
                        .zip(multipoint_right.members())
                        .all(
                            |(left_member, right_member)| match (left_member, right_member) {
                                (
                                    crate::geometry::MultiMember::Empty,
                                    crate::geometry::MultiMember::Empty,
                                ) => true,
                                (
                                    crate::geometry::MultiMember::Value(left_index),
                                    crate::geometry::MultiMember::Value(right_index),
                                ) => point_equals_exact::<Z, M>(
                                    multipoint_left.point_at(left_index),
                                    multipoint_right.point_at(right_index),
                                    tolerance,
                                ),
                                _ => false,
                            },
                        )
            },
            (Self::LineString(left), Self::LineString(right)) => {
                points_equal_exact::<Z, M, _, _>(left, right, tolerance)
            },
            (Self::MultiLineString(multiline_left), Self::MultiLineString(multiline_right)) => {
                (!Z || multiline_left.axes().has_z() == multiline_right.axes().has_z())
                    && (!M || multiline_left.axes().has_m() == multiline_right.axes().has_m())
                    && multiline_left.len() == multiline_right.len()
                    && multiline_left.members().zip(multiline_right.members()).all(
                        |(left_member, right_member)| match (left_member, right_member) {
                            (
                                crate::geometry::MultiMember::Empty,
                                crate::geometry::MultiMember::Empty,
                            ) => true,
                            (
                                crate::geometry::MultiMember::Value(left_index),
                                crate::geometry::MultiMember::Value(right_index),
                            ) => points_equal_exact::<Z, M, _, _>(
                                &multiline_left.as_slice()[left_index],
                                &multiline_right.as_slice()[right_index],
                                tolerance,
                            ),
                            _ => false,
                        },
                    )
            },
            (Self::Polygon(left), Self::Polygon(right)) => {
                left.equals_exact_impl::<Z, M>(right, tolerance)
            },
            (Self::MultiPolygon(multipolygon_left), Self::MultiPolygon(multipolygon_right)) => {
                (!Z || multipolygon_left.axes().has_z() == multipolygon_right.axes().has_z())
                    && (!M || multipolygon_left.axes().has_m() == multipolygon_right.axes().has_m())
                    && multipolygon_left.len() == multipolygon_right.len()
                    && multipolygon_left
                        .members()
                        .zip(multipolygon_right.members())
                        .all(
                            |(left_member, right_member)| match (left_member, right_member) {
                                (
                                    crate::geometry::MultiMember::Empty,
                                    crate::geometry::MultiMember::Empty,
                                ) => true,
                                (
                                    crate::geometry::MultiMember::Value(left_index),
                                    crate::geometry::MultiMember::Value(right_index),
                                ) => multipolygon_left.as_slice()[left_index]
                                    .equals_exact_impl::<Z, M>(
                                        &multipolygon_right.as_slice()[right_index],
                                        tolerance,
                                    ),
                                _ => false,
                            },
                        )
            },
            (Self::GeometryCollection(left), Self::GeometryCollection(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| left.equals_exact_impl::<Z, M>(right, tolerance))
            },
            // Structural equality: a typed empty matches only the same-kind,
            // same-declared-axes empty (`POINT EMPTY` ≠ `POLYGON EMPTY`,
            // `POINT EMPTY` ≠ `POINT Z EMPTY`), unlike topological `equals`.
            // The Z/M axes comparisons honor `include_z`/`include_m`, exactly
            // like the column-presence comparison for empty sequences.
            (Self::Empty(left, left_axes), Self::Empty(right, right_axes)) => {
                left == right
                    && (!Z || left_axes.has_z() == right_axes.has_z())
                    && (!M || left_axes.has_m() == right_axes.has_m())
            },
            _ => false,
        }
    }
}
