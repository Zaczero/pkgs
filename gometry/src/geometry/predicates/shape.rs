use crate::error::Result;
#[cfg(test)]
use crate::geometry::{CompiledPattern, GeometryErrorKind, PreparedPlan};
use crate::geometry::{
    Loc, Point, RunIdentity, Shape, SharedShapeOwners, SupportRunKind, cartesian_point_covers,
    cartesian_point_location, graph_relate_shapes, point_equals_exact, points_equal_exact,
};
use crate::predicates::engine::{Predicate, topology_shapes_predicate};
#[cfg(test)]
use crate::predicates::engine::{PreparedTopologyPair, topology_scalar_pattern};

impl Shape {
    /// Strict point membership used by coordinate operations and geometry
    /// algorithms. Topological predicates use the shared DE-9IM evidence model.
    pub fn contains_point(&self, point: Point) -> bool {
        self.point_membership::<false>(point)
    }

    pub fn covers_point(&self, point: Point) -> bool {
        self.point_membership::<true>(point)
    }

    /// One point-membership skeleton for both spellings: `BOUNDARY`
    /// selects boundary-inclusive `covers` semantics over interior-only
    /// `contains` in the three arms where they differ — the const flag
    /// keeps a single exhaustive match, so a new `Shape` variant cannot
    /// update one predicate and silently miss its twin.
    fn point_membership<const BOUNDARY: bool>(&self, point: Point) -> bool {
        if BOUNDARY {
            return cartesian_point_covers(self, point);
        }
        let location = cartesian_point_location(self, point);
        location == Loc::Interior
    }

    pub fn contains(&self, other: &Self) -> Result<bool> {
        topology_shapes_predicate(Predicate::Contains, self, other)
    }

    /// `contains` with no boundary contact: *other* lies in the interior of
    /// *self* and never touches its boundary (DE-9IM `T**FF*FF*`).
    #[cfg(test)]
    pub fn contains_properly(&self, other: &Self) -> Result<bool> {
        topology_shapes_predicate(Predicate::ContainsProperly, self, other)
    }

    #[cfg(test)]
    pub fn within(&self, other: &Self) -> Result<bool> {
        topology_shapes_predicate(Predicate::Within, self, other)
    }

    pub fn covers(&self, other: &Self) -> Result<bool> {
        topology_shapes_predicate(Predicate::Covers, self, other)
    }

    #[cfg(test)]
    pub fn covered_by(&self, other: &Self) -> Result<bool> {
        topology_shapes_predicate(Predicate::CoveredBy, self, other)
    }

    #[cfg(test)]
    pub fn intersects(&self, other: &Self) -> Result<bool> {
        topology_shapes_predicate(Predicate::Intersects, self, other)
    }

    /// Visit one representative vertex per connected 1D/2D component (first
    /// vertex of each line/shell), short-circuiting on the first `true`.
    /// With no boundary contact between two geometries, each component is
    /// uniformly inside or outside the other, so its representative decides
    /// area containment for the whole component. Shared immutable owners keep
    /// their first representative; later identical occurrences cannot change
    /// the existential result.
    pub(crate) fn any_component_representative(
        &self,
        predicate: &mut impl FnMut(Point) -> bool,
    ) -> bool {
        let mut owners = SharedShapeOwners::for_shape(self);
        self.any_component_representative_inner(&mut owners, predicate)
    }

    fn any_component_representative_inner(
        &self,
        owners: &mut Option<SharedShapeOwners>,
        predicate: &mut impl FnMut(Point) -> bool,
    ) -> bool {
        match self {
            Self::Point(point) => predicate(*point),
            Self::MultiPoint(points) => {
                owners.as_mut().is_none_or(|owners| {
                    owners.first_support_run(points, RunIdentity::Full, SupportRunKind::PointSet)
                }) && points.iter().any(&mut *predicate)
            },
            Self::LineString(line) => {
                owners.as_mut().is_none_or(|owners| {
                    owners.first_support_run(
                        line.as_coords(),
                        RunIdentity::Full,
                        SupportRunKind::SegmentChain,
                    )
                }) && line.first().is_some_and(&mut *predicate)
            },
            Self::MultiLineString(lines) => lines.iter().any(|line| {
                owners.as_mut().is_none_or(|owners| {
                    owners.first_support_run(
                        line.as_coords(),
                        RunIdentity::Full,
                        SupportRunKind::SegmentChain,
                    )
                }) && line.first().is_some_and(&mut *predicate)
            }),
            Self::Polygon(polygon) => {
                owners.as_mut().is_none_or(|owners| {
                    owners.first_support_run(
                        polygon.shell.coords(),
                        RunIdentity::Full,
                        SupportRunKind::SegmentChain,
                    )
                }) && polygon.shell.first().is_some_and(&mut *predicate)
            },
            Self::MultiPolygon(polygons) => polygons.iter().any(|polygon| {
                owners.as_mut().is_none_or(|owners| {
                    owners.first_support_run(
                        polygon.shell.coords(),
                        RunIdentity::Full,
                        SupportRunKind::SegmentChain,
                    )
                }) && polygon.shell.first().is_some_and(&mut *predicate)
            }),
            Self::GeometryCollection(geometries) => {
                owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                }) && geometries
                    .iter()
                    .any(|geometry| geometry.any_component_representative_inner(owners, predicate))
            },
            Self::Empty(..) => false,
        }
    }

    /// Whether any part is structurally polygonal, recursively through
    /// collections. Ring winding is deliberately not consulted: a
    /// self-crossing shell can have zero signed area while enclosing filled
    /// lobes.
    pub(crate) fn has_area_parts(&self) -> bool {
        self.has_area_parts_with_owners(&mut SharedShapeOwners::for_collections())
    }

    fn has_area_parts_with_owners(&self, owners: &mut SharedShapeOwners) -> bool {
        match self {
            Self::Polygon(_) | Self::MultiPolygon(_) => true,
            Self::GeometryCollection(geometries) => {
                owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                    && geometries
                        .iter()
                        .any(|geometry| geometry.has_area_parts_with_owners(owners))
            },
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Empty(..) => false,
        }
    }

    /// Boundary-inclusive point-in-AREA test: `covers_point` restricted to
    /// the polygon parts (1D/0D parts cannot contain a disjoint component).
    pub(crate) fn area_covers_point(&self, point: Point) -> bool {
        crate::geometry::cartesian_area_covers_point(self, point)
    }

    #[cfg(test)]
    pub fn disjoint(&self, other: &Self) -> Result<bool> {
        topology_shapes_predicate(Predicate::Disjoint, self, other)
    }

    #[cfg(test)]
    pub fn touches(&self, other: &Self) -> Result<bool> {
        topology_shapes_predicate(Predicate::Touches, self, other)
    }

    #[cfg(test)]
    pub fn overlaps(&self, other: &Self) -> Result<bool> {
        topology_shapes_predicate(Predicate::Overlaps, self, other)
    }

    pub fn relate(&self, other: &Self) -> Result<String> {
        Ok(graph_relate_shapes(self, other)?.text())
    }
}

impl Shape {
    #[cfg(test)]
    pub fn relate_pattern(&self, other: &Self, pattern: &str) -> Result<bool> {
        let compiled =
            CompiledPattern::new(pattern).ok_or_else(|| Self::invalid_de9im_pattern(pattern))?;
        let left = PreparedPlan::transient(self);
        let right = PreparedPlan::transient(other);
        let pair = PreparedTopologyPair::Cartesian {
            left: &left,
            right: &right,
        };
        topology_scalar_pattern(compiled, &pair)
    }

    #[cfg(test)]
    fn invalid_de9im_pattern(pattern: &str) -> crate::error::Error {
        GeometryErrorKind::invalid_de9im_pattern(format!(
            "invalid DE-9IM pattern {pattern:?}; expected 9 characters of \
             'T', 'F', '0', '1', '2', or '*'"
        ))
    }

    pub fn equals(&self, other: &Self) -> Result<bool> {
        topology_shapes_predicate(Predicate::Equals, self, other)
    }

    /// Coordinate equality within `tolerance` (per-ordinate `|Δ| ≤ tolerance`):
    /// the structure must match and every paired ordinate must agree to
    /// `tolerance`. `tolerance == 0.0` is exact. CRS/epoch are ignored here —
    /// that is the job of `==`.
    #[cfg(test)]
    pub fn equals_exact(
        &self,
        other: &Self,
        tolerance: f64,
        include_z: bool,
        include_m: bool,
    ) -> bool {
        crate::geometry::dispatch_axes!((include_z, include_m), HAS_Z, HAS_M, {
            self.equals_exact_impl::<HAS_Z, HAS_M>(other, tolerance)
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "MultiMember::Value indices are validated by their owning multipart geometry; preserve O(1) member access"
    )]
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
                    && multipoint_left.member_count() == multipoint_right.member_count()
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
                    && multiline_left.member_count() == multiline_right.member_count()
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
                    && multipolygon_left.member_count() == multipolygon_right.member_count()
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
