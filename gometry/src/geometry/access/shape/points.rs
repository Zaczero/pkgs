use super::owners::shape_may_repeat_coord_run;
use super::*;

#[cfg(test)]
mod tests;

fn reserve_unique_points_capacity(
    capacity: usize,
    seen: &mut HashSet<Point>,
    unique: &mut Vec<Point>,
) {
    // Distinct-owner estimates avoid expanding shared DAG occurrences. Failed
    // reservations only lose this hint; growth follows the points inserted.
    let _seen_reservation = seen.try_reserve(capacity);
    let _unique_reservation = unique.try_reserve(capacity);
}

#[derive(Clone, Copy)]
enum PointOnlyRun<'a> {
    Point(Point),
    MultiPoint(&'a MultiPoint),
}

/// A point-set operand, empties included (see [`Lineal`]).
pub(crate) enum PointSet<'a> {
    Point(Point),
    MultiPoint(&'a MultiPoint),
    Empty,
}

fn point_set_only_with_owners(shape: &Shape, owners: &mut SharedShapeOwners) -> bool {
    match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => true,
        Shape::LineString(line) => line.as_coords().is_empty(),
        Shape::MultiLineString(lines) => lines.iter().all(|line| line.as_coords().is_empty()),
        Shape::Polygon(polygon) => polygon.rings().all(|ring| ring.coords().is_empty()),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .all(|polygon| polygon.rings().all(|ring| ring.coords().is_empty())),
        Shape::GeometryCollection(collection) => {
            if owners.first_collection(collection.as_slice(), collection.members_are_shared()) {
                collection
                    .iter()
                    .all(|member| point_set_only_with_owners(member, owners))
            } else {
                true
            }
        },
    }
}

impl Shape {
    /// Whether every nonempty component is puntal. Empty geometries keep
    /// their declared kind but contribute no site to a point-set selection.
    pub(crate) fn point_set_only(&self) -> bool {
        match self {
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::Empty(
                GeometryKind::Point | GeometryKind::MultiPoint | GeometryKind::GeometryCollection,
                _,
            ) => true,
            Self::GeometryCollection(_) => {
                point_set_only_with_owners(self, &mut SharedShapeOwners::for_collections())
            },
            Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::Empty(..) => false,
        }
    }

    pub const fn geometry_type(&self) -> &'static str {
        GeometryKind::of(self).geometry_type()
    }

    pub const fn topological_dimension(&self) -> Dimension {
        match self {
            Self::GeometryCollection(geometries) => geometries.topological_dimension(),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::Empty(..) => GeometryKind::of(self).topological_dimension(),
        }
    }

    /// The point an operation reads from this geometry: `None` for POINT
    /// EMPTY, which such operations answer rather than reject.
    pub(crate) fn point_operand(&self, operation: &'static str) -> Result<Option<Point>> {
        match self {
            Self::Point(point) => Ok(Some(*point)),
            Self::Empty(GeometryKind::Point, _) => Ok(None),
            Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(
                GeometryKind::MultiPoint
                | GeometryKind::LineString
                | GeometryKind::MultiLineString
                | GeometryKind::Polygon
                | GeometryKind::MultiPolygon
                | GeometryKind::GeometryCollection,
                _,
            ) => Err(GeometryErrorKind::PointRequired { operation }.into()),
        }
    }

    /// The shape as a point-set operand; `None` for any other kind.
    pub(crate) const fn point_set(&self) -> Option<PointSet<'_>> {
        match self {
            Self::Point(point) => Some(PointSet::Point(*point)),
            Self::MultiPoint(points) => Some(PointSet::MultiPoint(points)),
            Self::Empty(GeometryKind::Point | GeometryKind::MultiPoint, _) => Some(PointSet::Empty),
            Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(
                GeometryKind::LineString
                | GeometryKind::MultiLineString
                | GeometryKind::Polygon
                | GeometryKind::MultiPolygon
                | GeometryKind::GeometryCollection,
                _,
            ) => None,
        }
    }

    /// Visit every vertex without allocating an iterator. Prefer it for folds
    /// and scans on hot per-element paths (encode, axis detection); collect
    /// with [`points_vec`](Self::points_vec) when a slice is needed.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "visitor type is intentionally opaque at this one-pass traversal boundary"
    )]
    pub fn for_each_point(&self, mut visit: impl FnMut(Point)) {
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.try_for_each_point_inner(&mut |point| {
                visit(point);
                ControlFlow::Continue(())
            });
    }

    /// Whether any vertex satisfies `predicate`, short-circuiting on the first
    /// match. The non-boxing counterpart to `points().any(...)`.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "predicate type is intentionally opaque at this one-pass traversal boundary"
    )]
    pub fn any_point(&self, mut predicate: impl FnMut(Point) -> bool) -> bool {
        matches!(
            self.try_for_each_point_inner(&mut |point| {
                if predicate(point) {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            }),
            ControlFlow::Break(())
        )
    }

    /// Visit every vertex until `visit` returns `Err`, short-circuiting — the
    /// fallible counterpart to [`for_each_point`](Self::for_each_point), for
    /// hot scans that validate each vertex without allocating a boxed
    /// iterator.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "visitor type is intentionally opaque at this one-pass traversal boundary"
    )]
    pub fn try_for_each_point<E>(
        &self,
        mut visit: impl FnMut(Point) -> Result<(), E>,
    ) -> Result<(), E> {
        match self.try_for_each_point_inner(&mut |point| match visit(point) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }

    /// Visit the first occurrence of every full structural point. Shared
    /// immutable collection and coordinate owners are scanned once; distinct
    /// owners with equal points are still compared by Point identity.
    pub(crate) fn try_for_each_distinct_point<B>(
        &self,
        seen: &mut HashSet<Point>,
        mut visit: impl FnMut(Point) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        let mut publish = |point| {
            if seen.insert(point) {
                visit(point)?;
            }
            ControlFlow::Continue(())
        };
        let Some(mut owners) = SharedShapeOwners::for_shape(self) else {
            return self.try_for_each_point_inner(&mut publish);
        };
        let mut owner_set = Some(&mut owners);
        self.for_each_coord_run_inner(&mut owner_set, RunIdentity::Full, &mut |run| {
            match run {
                CoordRun::Point(point) => publish(point)?,
                CoordRun::Seq(coords) => {
                    for point in coords {
                        publish(point)?;
                    }
                },
            }
            ControlFlow::Continue(())
        })
    }

    /// Fallible-hash XY set traversal. The first PointKey occurrence retains
    /// the source Point's active ordinates and signed-zero spelling.
    pub(crate) fn try_for_each_distinct_xy_point<B>(
        &self,
        mut visit: impl FnMut(Point) -> ControlFlow<B>,
    ) -> Result<ControlFlow<B>> {
        let mut seen = crate::collections::try_hash_set()?;
        if let Some(capacity) = self.point_set_capacity_hint(RunIdentity::XY) {
            let _reservation = seen.try_reserve(capacity);
        }
        let mut publish = |point| {
            if seen.insert(PointKey::new(point)) {
                visit(point)
            } else {
                ControlFlow::Continue(())
            }
        };
        let outcome = if let Some(mut owners) = SharedShapeOwners::for_shape(self) {
            let mut owner_set = Some(&mut owners);
            self.for_each_coord_run_inner(&mut owner_set, RunIdentity::XY, &mut |run| {
                match run {
                    CoordRun::Point(point) => publish(point)?,
                    CoordRun::Seq(coords) => {
                        for point in coords {
                            publish(point)?;
                        }
                    },
                }
                ControlFlow::Continue(())
            })
        } else {
            self.try_for_each_point_inner(&mut publish)
        };
        Ok(outcome)
    }

    /// Visit full-ordinate point runs once by visible shared owner, without
    /// deduplicating Point values across distinct coordinate payloads.
    pub(crate) fn for_each_first_point_run(&self, mut visit: impl FnMut(Point)) {
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.try_for_each_first_point_run(|point| {
                visit(point);
                ControlFlow::Continue(())
            });
    }

    pub(crate) fn try_for_each_first_point_run<B>(
        &self,
        mut visit: impl FnMut(Point) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        let Some(mut owners) = SharedShapeOwners::for_shape(self) else {
            return self.try_for_each_point_inner(&mut visit);
        };
        let mut owner_set = Some(&mut owners);
        self.for_each_coord_run_inner(&mut owner_set, RunIdentity::Full, &mut |run| {
            match run {
                CoordRun::Point(point) => visit(point)?,
                CoordRun::Seq(coords) => {
                    for point in coords {
                        visit(point)?;
                    }
                },
            }
            ControlFlow::Continue(())
        })
    }

    /// Visit isolated point support once per shared owner, preserving the
    /// first occurrence order.
    pub(crate) fn for_each_first_point_only(
        &self,
        identity: RunIdentity,
        mut visit: impl FnMut(Point),
    ) {
        let ControlFlow::Continue(()): ControlFlow<Infallible> = self
            .try_for_each_first_point_only(identity, |point| {
                visit(point);
                ControlFlow::Continue(())
            });
    }

    /// Append the first occurrence of each selected point-only run.
    pub(crate) fn append_first_point_only(&self, identity: RunIdentity, out: &mut Vec<Point>) {
        let ControlFlow::Continue(()): ControlFlow<Infallible> = self
            .for_each_first_point_only_run(identity, |run| {
                match run {
                    PointOnlyRun::Point(point) => out.push(point),
                    PointOnlyRun::MultiPoint(points) => {
                        out.reserve(points.coord_count());
                        out.extend(points);
                    },
                }
                ControlFlow::Continue(())
            });
    }

    pub(crate) fn try_for_each_first_point_only<B>(
        &self,
        identity: RunIdentity,
        mut visit: impl FnMut(Point) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.for_each_first_point_only_run(identity, |run| {
            match run {
                PointOnlyRun::Point(point) => visit(point)?,
                PointOnlyRun::MultiPoint(points) => {
                    for point in points {
                        visit(point)?;
                    }
                },
            }
            ControlFlow::Continue(())
        })
    }

    fn for_each_first_point_only_run<B>(
        &self,
        identity: RunIdentity,
        mut visit: impl FnMut(PointOnlyRun<'_>) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        let Some(mut owners) = SharedShapeOwners::for_shape(self) else {
            return self.for_each_point_only_inner(&mut None, identity, &mut visit);
        };
        let mut owner_set = Some(&mut owners);
        self.for_each_point_only_inner(&mut owner_set, identity, &mut visit)
    }

    fn for_each_point_only_inner<B>(
        &self,
        owners: &mut Option<&mut SharedShapeOwners>,
        identity: RunIdentity,
        visit: &mut impl FnMut(PointOnlyRun<'_>) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            Self::Point(point) => visit(PointOnlyRun::Point(*point))?,
            Self::MultiPoint(points) => {
                if owners
                    .as_mut()
                    .is_none_or(|owners| owners.first_coord_seq(points, identity))
                {
                    visit(PointOnlyRun::MultiPoint(points))?;
                }
            },
            Self::GeometryCollection(geometries) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                }) {
                    for geometry in geometries {
                        geometry.for_each_point_only_inner(owners, identity, visit)?;
                    }
                }
            },
            Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::Empty(..) => {},
        }
        ControlFlow::Continue(())
    }

    /// Visit points in first occurrences of XY-identical shared runs. Semantic
    /// XY duplicates from distinct owners remain the caller's set policy.
    pub(crate) fn for_each_first_xy_point_run(&self, mut visit: impl FnMut(Point)) {
        let ControlFlow::Continue(()): ControlFlow<Infallible> = self
            .try_for_each_first_xy_point_run(|point| {
                visit(point);
                ControlFlow::Continue(())
            });
    }

    pub(crate) fn try_for_each_first_xy_point_run<B>(
        &self,
        mut visit: impl FnMut(Point) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        let Some(mut owners) = SharedShapeOwners::for_shape(self) else {
            return self.try_for_each_point_inner(&mut visit);
        };
        let mut owner_set = Some(&mut owners);
        self.for_each_coord_run_inner(&mut owner_set, RunIdentity::XY, &mut |run| {
            match run {
                CoordRun::Point(point) => visit(point)?,
                CoordRun::Seq(coords) => {
                    for point in coords {
                        visit(point)?;
                    }
                },
            }
            ControlFlow::Continue(())
        })
    }

    fn try_for_each_point_inner<B, F: FnMut(Point) -> ControlFlow<B>>(
        &self,
        visit: &mut F,
    ) -> ControlFlow<B> {
        match self {
            Self::Point(point) => visit(*point)?,
            Self::MultiPoint(points) => {
                for point in points {
                    visit(point)?;
                }
            },
            Self::LineString(points) => {
                for point in points {
                    visit(point)?;
                }
            },
            Self::MultiLineString(lines) => {
                for point in lines.iter().flatten() {
                    visit(point)?;
                }
            },
            Self::Polygon(polygon) => {
                for point in polygon.points() {
                    visit(point)?;
                }
            },
            Self::MultiPolygon(polygons) => {
                for point in polygons.iter().flat_map(Polygon::points) {
                    visit(point)?;
                }
            },
            Self::GeometryCollection(geometries) => {
                for geometry in geometries {
                    geometry.try_for_each_point_inner(visit)?;
                }
            },
            Self::Empty(..) => {},
        }
        ControlFlow::Continue(())
    }

    /// Append every vertex to `out` without allocating a boxed iterator — the
    /// non-boxing counterpart to `points().collect()`. Reserve in the caller
    /// when the count is known.
    pub fn collect_points_into(&self, out: &mut Vec<Point>) {
        self.for_each_point(|point| out.push(point));
    }

    /// Materialize every vertex into a pre-sized `Vec`, avoiding the
    /// boxed-`dyn` iterator allocation of the historical `points()`. Prefer
    /// it for hot paths (distance worksets, geodesic/Hausdorff distance,
    /// min-clearance) that need an owned vertex buffer. Returns
    /// `SizeOverflow` if the logical point count is not representable.
    pub fn points_vec(&self) -> crate::error::Result<Vec<Point>> {
        let count = ShapeCountQuery::default().coordinates(self)?;
        let mut points = Vec::with_capacity(count);
        self.collect_points_into(&mut points);
        Ok(points)
    }

    /// Distinct vertices in first-occurrence order.
    /// The dedup key is full structural identity (axes + every active
    /// ordinate by bit pattern, the `equals_identical` notion) — unlike
    /// `remove_repeated_points`, which collapses only consecutive runs within
    /// a tolerance. Homogeneous survivors use a `MultiPoint`; heterogeneous
    /// survivors remain ordered `Point` members of a `GeometryCollection`.
    pub fn unique_points(&self) -> Result<Self> {
        // A point atom is already its own unique set — no hashing.
        if let Self::Point(point) = self {
            return Ok(Self::multi_point(
                point.axes,
                CoordSeq::try_from_points(&[*point])?,
            ));
        }
        let unique = self.distinct_point_values();
        let Some(first) = unique.first().copied() else {
            return Ok(Self::multi_point(
                self.axes(),
                CoordSeq::try_from_points(&unique)?,
            ));
        };
        if unique.iter().all(|point| point.axes() == first.axes()) {
            return Ok(Self::multi_point(
                first.axes(),
                CoordSeq::try_from_points(&unique)?,
            ));
        }
        let mut members = Vec::with_capacity(unique.len());
        members.extend(unique.into_iter().map(Self::Point));
        Self::geometry_collection(members)
    }

    /// Full-structural point set in first occurrence order.
    pub(crate) fn distinct_point_values(&self) -> Vec<Point> {
        if let Self::Point(point) = self {
            return vec![*point];
        }
        let mut seen = HashSet::new();
        let mut unique = Vec::new();
        if let Some(capacity) = self.point_set_capacity_hint(RunIdentity::Full) {
            reserve_unique_points_capacity(capacity, &mut seen, &mut unique);
        }
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.try_for_each_distinct_point(&mut seen, |point| {
                unique.push(point);
                ControlFlow::Continue(())
            });
        unique
    }

    pub(crate) fn point_set_capacity_hint(&self, identity: RunIdentity) -> Option<usize> {
        if matches!(self, Self::GeometryCollection(_)) {
            Some(self.distinct_point_capacity_hint(identity))
        } else {
            ShapeCountQuery::default().coordinate_count(self)
        }
    }

    /// Reservation estimate for segment-set consumers. Multiplicity in a
    /// shared collection or coordinate owner does not enlarge the set.
    pub(crate) fn segment_count_capacity_hint(&self, identity: RunIdentity) -> Option<usize> {
        if !shape_may_repeat_coord_run(self) {
            return ShapeCountQuery::default().segments(self).ok();
        }
        let mut capacity = 0_usize;
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.for_each_first_segment_chain(identity, |chain| {
                capacity = capacity.wrapping_add(chain.len().saturating_sub(1));
                ControlFlow::Continue(())
            });
        Some(capacity)
    }

    /// Reservation estimate for 3D point-and-segment support records.
    pub(crate) fn metric_support_capacity_hint(&self, identity: RunIdentity) -> Option<usize> {
        if !shape_may_repeat_coord_run(self) {
            return ShapeCountQuery::default().coordinate_count(self);
        }
        let mut capacity = 0_usize;
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.for_each_first_support_run(identity, |run| {
                let count = match run {
                    MetricSupportRun::Point(_) => 1,
                    MetricSupportRun::MultiPoint(points) => points.coord_count(),
                    MetricSupportRun::SegmentChain(coords) if coords.len() == 1 => 1,
                    MetricSupportRun::SegmentChain(coords) => coords.len().saturating_sub(1),
                };
                capacity = capacity.wrapping_add(count);
                ControlFlow::Continue(())
            });
        Some(capacity)
    }

    /// Capacity estimate in the requested identity domain. Collection hints
    /// count shared owners once; other shapes retain their exact coordinate
    /// count.
    fn distinct_point_capacity_hint(&self, identity: RunIdentity) -> usize {
        let mut owners = SharedShapeOwners::for_shape(self);
        let mut owner_set = owners.as_mut();
        let mut capacity = 0_usize;
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.for_each_coord_run_inner(&mut owner_set, identity, &mut |run| {
                let count = match run {
                    CoordRun::Point(_) => 1,
                    CoordRun::Seq(coords) => coords.len(),
                };
                capacity = capacity.wrapping_add(count);
                ControlFlow::Continue(())
            });
        capacity
    }

    /// Whether this tree repeats a shared immutable collection or coordinate
    /// window. The scan visits each repeated subtree only once.
    pub(crate) fn has_repeated_shared_owner(&self) -> bool {
        let Some(mut owners) = SharedShapeOwners::for_shape(self) else {
            return false;
        };
        let mut owner_set = Some(&mut owners);
        let outcome = self.for_each_coord_run_inner(&mut owner_set, RunIdentity::Full, &mut |_| {
            ControlFlow::<()>::Continue(())
        });
        debug_assert!(outcome.is_continue());
        owners.saw_repeated_owner()
    }

    /// A compact full-Point set in first occurrence order, retaining mixed
    /// axes as Point members and homogeneous points in column storage.
    pub(crate) fn distinct_point_cloud(&self) -> Result<Self> {
        let points = self.distinct_point_values();
        let Some(first) = points.first().copied() else {
            return Self::geometry_collection(Vec::new());
        };
        if points.iter().all(|point| point.axes() == first.axes())
            && i32::try_from(points.len()).is_ok()
        {
            return Ok(Self::multi_point(
                first.axes(),
                CoordSeq::try_from_points(&points)?,
            ));
        }
        Self::geometry_collection(points.into_iter().map(Self::Point).collect())
    }
}
