use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::hash::Hash;
use std::ops::ControlFlow;

use crate::error::Result;
use crate::geometry::access::{bounds_from_iter, canonicalize_zero, column_minmax, line_length};
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, Coordinates as _, Dimension, GeometryErrorKind, GeometryKind,
    HasM, HasZ, LineSeq, MultiLineString, MultiMember, MultiPoint, MultiPolygon, Point, PointKey,
    Polygon, Shape, ShapeCountQuery, ShapeOwnerKey, VisiblePointRunKey, VisibleXYKey,
    admit_shape_vector_capacity, line_length_3d,
};

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

/// One atomic part of a shape: a point, a line, or a polygon.
///
/// Multiparts and collections are syntax over these, so an operation that
/// treats a one-member `MultiPolygon` and a bare `Polygon` differently is
/// wrong by construction. Polygon rings stay together: shell and holes are one
/// part, not three.
#[derive(Clone, Copy)]
pub(crate) enum Component<'a> {
    Point(Point),
    LineString(&'a LineSeq),
    Polygon(&'a Polygon),
}

impl Component<'_> {
    pub(crate) fn to_shape(self) -> Shape {
        match self {
            Self::Point(point) => Shape::Point(point),
            Self::LineString(line) => Shape::LineString(line.clone()),
            Self::Polygon(polygon) => Shape::Polygon(polygon.clone()),
        }
    }
}

/// One contiguous coordinate run of a shape, in flatten order.
///
/// A `MultiPoint` is a single run rather than one run per member, because
/// that is how flattening, reconstruction and the coordinate views all treat
/// it. A walk that has to tell puntal members from linework wants
/// [`Component`] instead.
#[derive(Clone, Copy)]
pub(crate) enum CoordRun<'a> {
    Point(Point),
    Seq(&'a CoordSeq),
}

/// One isolated atom or coordinate run in a metric support set.
#[derive(Clone, Copy)]
pub(crate) enum MetricSupportRun<'a> {
    Point(Point),
    MultiPoint(&'a MultiPoint),
    SegmentChain(&'a CoordSeq),
}

#[derive(Clone, Copy)]
enum PointOnlyRun<'a> {
    Point(Point),
    MultiPoint(&'a MultiPoint),
}

#[derive(Clone, Copy)]
pub(crate) enum RunIdentity {
    Full,
    XY,
}

/// The same CoordSeq owner can represent point support and linework support.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) enum SupportRunKind {
    PointSet,
    SegmentChain,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum SupportRunOwner {
    FullPointSet(VisiblePointRunKey),
    FullSegmentChain(VisiblePointRunKey),
    XyPointSet(VisibleXYKey),
    XySegmentChain(VisibleXYKey),
}

/// Query-local aliases for immutable collection and coordinate owners.
///
/// A single coordinate run cannot recur within a simple Point, MultiPoint,
/// or LineString query. Avoid a per-query owner table on those ordinary paths.
pub(crate) struct SharedShapeOwners {
    collections: HashSet<ShapeOwnerKey>,
    xy_runs: HashSet<VisibleXYKey>,
    full_runs: HashSet<VisiblePointRunKey>,
    support_runs: HashSet<SupportRunOwner>,
    collection_capacity_hint: usize,
    track_runs: bool,
    repeated_owner: bool,
}

impl SharedShapeOwners {
    pub(crate) fn for_collections() -> Self {
        Self {
            collections: HashSet::new(),
            xy_runs: HashSet::new(),
            full_runs: HashSet::new(),
            support_runs: HashSet::new(),
            collection_capacity_hint: 0,
            track_runs: false,
            repeated_owner: false,
        }
    }

    pub(crate) fn for_shape(shape: &Shape) -> Option<Self> {
        let track_runs = shape_may_repeat_coord_run(shape);
        track_runs.then(|| Self {
            collections: HashSet::new(),
            xy_runs: HashSet::new(),
            full_runs: HashSet::new(),
            support_runs: HashSet::new(),
            collection_capacity_hint: shape.nesting_depth(),
            track_runs,
            repeated_owner: false,
        })
    }

    pub(crate) fn first_collection(&mut self, members: &[Shape], shared: bool) -> bool {
        if !shared {
            return true;
        }
        if self.collections.is_empty() && self.collection_capacity_hint > 0 {
            self.collections.reserve(self.collection_capacity_hint);
            self.collection_capacity_hint = 0;
        }
        let owner = ShapeOwnerKey::new(members);
        // Unique member vectors cannot recur in an immutable Shape tree.
        // Avoid hashing the ordinary collection owner that is visited once.
        let first = self.collections.insert(owner);
        self.repeated_owner |= !first;
        first
    }

    pub(crate) fn first_coord_seq(&mut self, coords: &CoordSeq, identity: RunIdentity) -> bool {
        if !self.track_runs {
            return true;
        }
        let first = match identity {
            RunIdentity::Full => coords
                .shared_point_run_key()
                .is_none_or(|key| self.full_runs.insert(key)),
            RunIdentity::XY => coords
                .shared_xy_key()
                .is_none_or(|key| self.xy_runs.insert(key)),
        };
        self.repeated_owner |= !first;
        first
    }

    pub(crate) fn first_support_run(
        &mut self,
        coords: &CoordSeq,
        identity: RunIdentity,
        kind: SupportRunKind,
    ) -> bool {
        if !self.track_runs {
            return true;
        }
        let key = match (identity, kind) {
            (RunIdentity::Full, SupportRunKind::PointSet) => coords
                .shared_point_run_key()
                .map(SupportRunOwner::FullPointSet),
            (RunIdentity::Full, SupportRunKind::SegmentChain) => coords
                .shared_point_run_key()
                .map(SupportRunOwner::FullSegmentChain),
            (RunIdentity::XY, SupportRunKind::PointSet) => {
                coords.shared_xy_key().map(SupportRunOwner::XyPointSet)
            },
            (RunIdentity::XY, SupportRunKind::SegmentChain) => {
                coords.shared_xy_key().map(SupportRunOwner::XySegmentChain)
            },
        };
        let first = key.is_none_or(|key| self.support_runs.insert(key));
        self.repeated_owner |= !first;
        first
    }

    pub(crate) const fn saw_repeated_owner(&self) -> bool {
        self.repeated_owner
    }
}

fn shape_may_repeat_coord_run(shape: &Shape) -> bool {
    match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::LineString(_) | Shape::Empty(..) => false,
        Shape::MultiLineString(lines) => lines.as_slice().len() > 1,
        Shape::Polygon(polygon) => !polygon.holes.is_empty(),
        Shape::MultiPolygon(polygons) => {
            polygons.as_slice().len() > 1
                || polygons.iter().any(|polygon| !polygon.holes.is_empty())
        },
        Shape::GeometryCollection(members) => {
            members.len() > 1 || members.iter().any(shape_may_repeat_coord_run)
        },
    }
}

fn reserve_owner_map<K: Hash + Eq, V>(owners: &mut HashMap<K, V>, capacity_hint: &mut usize) {
    if owners.is_empty() && *capacity_hint > 0 {
        owners.reserve(*capacity_hint);
        *capacity_hint = 0;
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum BoundsOwner {
    Collection(ShapeOwnerKey),
    XyRun(VisibleXYKey),
}

#[derive(Default)]
struct ShapeBoundsMemo {
    owners: HashMap<BoundsOwner, Option<Bounds>>,
    owner_capacity_hint: usize,
    track_runs: bool,
}

impl ShapeBoundsMemo {
    fn for_shape(shape: &Shape) -> Self {
        Self {
            owner_capacity_hint: shape.nesting_depth(),
            track_runs: true,
            ..Self::default()
        }
    }

    fn shape_bounds(&mut self, shape: &Shape) -> Option<Bounds> {
        if let Shape::GeometryCollection(geometries) = shape {
            let owner = geometries
                .members_are_shared()
                .then(|| BoundsOwner::Collection(ShapeOwnerKey::new(geometries.as_slice())));
            if let Some(&bounds) = owner.as_ref().and_then(|owner| self.owners.get(owner)) {
                return bounds;
            }
            let bounds = bounds_from_iter(
                geometries
                    .iter()
                    .filter_map(|geometry| self.shape_bounds(geometry)),
            );
            if let Some(owner) = owner {
                reserve_owner_map(&mut self.owners, &mut self.owner_capacity_hint);
                self.owners.insert(owner, bounds);
            }
            bounds
        } else {
            shape_bounds_with_coord_bounds(shape, &mut |coords| self.coord_bounds(coords))
        }
    }

    fn coord_bounds(&mut self, coords: &CoordSeq) -> Option<Bounds> {
        if self.track_runs
            && let Some(key) = coords.shared_xy_key().map(BoundsOwner::XyRun)
        {
            if let Some(&bounds) = self.owners.get(&key) {
                return bounds;
            }
            let bounds = Bounds::from_coords(coords);
            reserve_owner_map(&mut self.owners, &mut self.owner_capacity_hint);
            self.owners.insert(key, bounds);
            return bounds;
        }
        Bounds::from_coords(coords)
    }
}

fn shape_bounds_with_coord_bounds(
    shape: &Shape,
    coord_bounds: &mut impl FnMut(&CoordSeq) -> Option<Bounds>,
) -> Option<Bounds> {
    match shape {
        Shape::Point(point) => Some(Bounds::from_point(*point)),
        Shape::MultiPoint(coords) => coord_bounds(coords),
        Shape::LineString(line) => coord_bounds(line.as_coords()),
        Shape::MultiLineString(lines) => {
            bounds_from_iter(lines.iter().filter_map(|line| coord_bounds(line)))
        },
        Shape::Polygon(polygon) => bounds_from_iter(
            polygon
                .rings()
                .filter_map(|ring| coord_bounds(ring.coords())),
        ),
        Shape::MultiPolygon(polygons) => bounds_from_iter(
            polygons
                .iter()
                .flat_map(Polygon::rings)
                .filter_map(|ring| coord_bounds(ring.coords())),
        ),
        Shape::GeometryCollection(geometries) => bounds_from_iter(
            geometries
                .iter()
                .filter_map(|geometry| shape_bounds_with_coord_bounds(geometry, coord_bounds)),
        ),
        Shape::Empty(..) => None,
    }
}

#[derive(Clone, Copy)]
enum Ordinate {
    Z,
    M,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum OrdinateExtremaOwner {
    Collection(ShapeOwnerKey),
    ZRun(usize, usize),
    MRun(usize, usize),
}

#[derive(Default)]
struct OrdinateExtremaMemo {
    owners: HashMap<OrdinateExtremaOwner, Option<(f64, f64)>>,
    owner_capacity_hint: usize,
    track_runs: bool,
}

impl OrdinateExtremaMemo {
    fn for_shape(shape: &Shape) -> Self {
        Self {
            owner_capacity_hint: shape.nesting_depth(),
            track_runs: shape_may_repeat_coord_run(shape),
            ..Self::default()
        }
    }

    fn run_extrema(&mut self, coords: &CoordSeq, ordinate: Ordinate) -> Option<(f64, f64)> {
        let values = match ordinate {
            Ordinate::Z => coords.zs(),
            Ordinate::M => coords.ms(),
        };
        let key = if self.track_runs {
            match ordinate {
                Ordinate::Z => coords
                    .shared_z_key()
                    .map(|(owner, len)| OrdinateExtremaOwner::ZRun(owner, len)),
                Ordinate::M => coords
                    .shared_m_key()
                    .map(|(owner, len)| OrdinateExtremaOwner::MRun(owner, len)),
            }
        } else {
            None
        };
        if let Some(key) = key
            && let Some(&extrema) = self.owners.get(&key)
        {
            return extrema;
        }
        let extrema = values.and_then(column_minmax);
        if let Some(key) = key {
            reserve_owner_map(&mut self.owners, &mut self.owner_capacity_hint);
            self.owners.insert(key, extrema);
        }
        extrema
    }

    fn shape_extrema(&mut self, shape: &Shape, ordinate: Ordinate) -> Option<(f64, f64)> {
        if let Shape::GeometryCollection(collection) = shape {
            let owner = collection.members_are_shared().then(|| {
                OrdinateExtremaOwner::Collection(ShapeOwnerKey::new(collection.as_slice()))
            });
            if let Some(&extrema) = owner.as_ref().and_then(|owner| self.owners.get(owner)) {
                return extrema;
            }
            let extrema = merge_ordinate_extrema(
                collection
                    .iter()
                    .filter_map(|member| self.shape_extrema(member, ordinate)),
            );
            if let Some(owner) = owner {
                reserve_owner_map(&mut self.owners, &mut self.owner_capacity_hint);
                self.owners.insert(owner, extrema);
            }
            return extrema;
        }
        match shape {
            Shape::Point(point) => ordinate_value(*point, ordinate).map(|value| (value, value)),
            Shape::MultiPoint(coords) => self.run_extrema(coords, ordinate),
            Shape::LineString(coords) => self.run_extrema(coords, ordinate),
            Shape::MultiLineString(lines) => merge_ordinate_extrema(
                lines
                    .iter()
                    .filter_map(|coords| self.run_extrema(coords, ordinate)),
            ),
            Shape::Polygon(polygon) => merge_ordinate_extrema(
                polygon
                    .rings()
                    .filter_map(|ring| self.run_extrema(ring.coords(), ordinate)),
            ),
            Shape::MultiPolygon(polygons) => merge_ordinate_extrema(
                polygons
                    .iter()
                    .flat_map(|polygon| polygon.rings())
                    .filter_map(|ring| self.run_extrema(ring.coords(), ordinate)),
            ),
            Shape::GeometryCollection(_) => unreachable!("handled above"),
            Shape::Empty(..) => None,
        }
    }
}

fn ordinate_value(point: Point, ordinate: Ordinate) -> Option<f64> {
    match ordinate {
        Ordinate::Z => point.z(),
        Ordinate::M => point.m(),
    }
}

fn merge_ordinate_extrema(extrema: impl IntoIterator<Item = (f64, f64)>) -> Option<(f64, f64)> {
    let mut result = None;
    for item in extrema {
        merge_ord_extremes(&mut result, Some(item));
    }
    result
}

/// A lineal operand, empties included. Operations over lines match it
/// exhaustively, so none can route a typed empty into its kind error.
pub(crate) enum Lineal<'a> {
    LineString(&'a LineSeq),
    MultiLineString(&'a MultiLineString),
    Empty,
}

/// A polygonal operand, empties included (see [`Lineal`]).
pub(crate) enum Polygonal<'a> {
    Polygon(&'a Polygon),
    MultiPolygon(&'a MultiPolygon),
    Empty,
}

/// A point-set operand, empties included (see [`Lineal`]).
pub(crate) enum PointSet<'a> {
    Point(Point),
    MultiPoint(&'a MultiPoint),
    Empty,
}

impl Shape {
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

    /// The axes carried by this geometry's coordinates (the union over all
    /// vertices). The single source of truth for dimensionality.
    pub fn axes(&self) -> CoordinateAxes {
        // Coordinate sequences store ordinate presence in column flags, and
        // multipart payloads retain their validated common axes. Polygon rings
        // and geometry collections still use the structural scans below.
        let mut has_z = false;
        let mut has_m = false;
        self.fold_axes(
            &mut has_z,
            &mut has_m,
            &mut SharedShapeOwners::for_collections(),
        );
        CoordinateAxes::new(HasZ(has_z), HasM(has_m))
    }

    fn fold_axes(&self, has_z: &mut bool, has_m: &mut bool, owners: &mut SharedShapeOwners) {
        if *has_z && *has_m {
            return;
        }
        match self {
            Self::Point(point) => {
                *has_z |= point.z().is_some();
                *has_m |= point.m().is_some();
            },
            Self::MultiPoint(points) => {
                *has_z |= points.zs().is_some();
                *has_m |= points.ms().is_some();
            },
            Self::LineString(points) => {
                *has_z |= points.zs().is_some();
                *has_m |= points.ms().is_some();
            },
            Self::MultiLineString(lines) => {
                *has_z |= lines.axes().has_z();
                *has_m |= lines.axes().has_m();
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    *has_z |= ring.zs().is_some();
                    *has_m |= ring.ms().is_some();
                }
            },
            Self::MultiPolygon(polygons) => {
                *has_z |= polygons.axes().has_z();
                *has_m |= polygons.axes().has_m();
            },
            Self::GeometryCollection(geometries) => {
                if !owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                {
                    return;
                }
                for geometry in geometries {
                    geometry.fold_axes(has_z, has_m, owners);
                }
            },
            Self::Empty(_, axes) => {
                *has_z |= axes.has_z();
                *has_m |= axes.has_m();
            },
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

    /// The shape as a lineal operand; `None` for any other kind.
    pub(crate) const fn lineal(&self) -> Option<Lineal<'_>> {
        match self {
            Self::LineString(line) => Some(Lineal::LineString(line)),
            Self::MultiLineString(lines) => Some(Lineal::MultiLineString(lines)),
            Self::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => {
                Some(Lineal::Empty)
            },
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(
                GeometryKind::Point
                | GeometryKind::MultiPoint
                | GeometryKind::Polygon
                | GeometryKind::MultiPolygon
                | GeometryKind::GeometryCollection,
                _,
            ) => None,
        }
    }

    /// The shape as a polygonal operand; `None` for any other kind.
    pub(crate) const fn polygonal(&self) -> Option<Polygonal<'_>> {
        match self {
            Self::Polygon(polygon) => Some(Polygonal::Polygon(polygon)),
            Self::MultiPolygon(polygons) => Some(Polygonal::MultiPolygon(polygons)),
            Self::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, _) => {
                Some(Polygonal::Empty)
            },
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::GeometryCollection(_)
            | Self::Empty(
                GeometryKind::Point
                | GeometryKind::MultiPoint
                | GeometryKind::LineString
                | GeometryKind::MultiLineString
                | GeometryKind::GeometryCollection,
                _,
            ) => None,
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

    pub fn coordinate_axes(&self) -> &'static str {
        self.axes().as_str()
    }

    pub fn has_z(&self) -> bool {
        let (mut has_z, mut has_m) = (false, true);
        self.fold_axes(
            &mut has_z,
            &mut has_m,
            &mut SharedShapeOwners::for_collections(),
        );
        has_z
    }

    /// Whether every non-empty component carries a Z ordinate (stricter than
    /// [`has_z`](Self::has_z), which is a union-of-axes flag). Empty shapes
    /// answer `true` vacuously.
    pub(crate) fn axes_all_z(&self) -> bool {
        shape_axes_all_z(self)
    }

    pub fn has_m(&self) -> bool {
        let (mut has_z, mut has_m) = (true, false);
        self.fold_axes(
            &mut has_z,
            &mut has_m,
            &mut SharedShapeOwners::for_collections(),
        );
        has_m
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

    /// Visit each first-occurrence atom in the metric support of the shape.
    pub(crate) fn for_each_first_support_run<B>(
        &self,
        identity: RunIdentity,
        mut visit: impl FnMut(MetricSupportRun<'_>) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        let Some(mut owners) = SharedShapeOwners::for_shape(self) else {
            return self.for_each_first_support_run_inner(&mut None, identity, &mut visit);
        };
        let mut owner_set = Some(&mut owners);
        self.for_each_first_support_run_inner(&mut owner_set, identity, &mut visit)
    }

    fn for_each_first_support_run_inner<B>(
        &self,
        owners: &mut Option<&mut SharedShapeOwners>,
        identity: RunIdentity,
        visit: &mut impl FnMut(MetricSupportRun<'_>) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            Self::Point(point) => visit(MetricSupportRun::Point(*point))?,
            Self::MultiPoint(points) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_support_run(points, identity, SupportRunKind::PointSet)
                }) {
                    visit(MetricSupportRun::MultiPoint(points))?;
                }
            },
            Self::LineString(line) => {
                let coords = line.as_coords();
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_support_run(coords, identity, SupportRunKind::SegmentChain)
                }) {
                    visit(MetricSupportRun::SegmentChain(coords))?;
                }
            },
            Self::MultiLineString(lines) => {
                for line in lines {
                    let coords = line.as_coords();
                    if owners.as_mut().is_none_or(|owners| {
                        owners.first_support_run(coords, identity, SupportRunKind::SegmentChain)
                    }) {
                        visit(MetricSupportRun::SegmentChain(coords))?;
                    }
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    let coords = ring.coords();
                    if owners.as_mut().is_none_or(|owners| {
                        owners.first_support_run(coords, identity, SupportRunKind::SegmentChain)
                    }) {
                        visit(MetricSupportRun::SegmentChain(coords))?;
                    }
                }
            },
            Self::MultiPolygon(polygons) => {
                for ring in polygons.iter().flat_map(Polygon::rings) {
                    let coords = ring.coords();
                    if owners.as_mut().is_none_or(|owners| {
                        owners.first_support_run(coords, identity, SupportRunKind::SegmentChain)
                    }) {
                        visit(MetricSupportRun::SegmentChain(coords))?;
                    }
                }
            },
            Self::GeometryCollection(geometries) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                }) {
                    for geometry in geometries {
                        geometry.for_each_first_support_run_inner(owners, identity, visit)?;
                    }
                }
            },
            Self::Empty(..) => {},
        }
        ControlFlow::Continue(())
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
    /// `SizeOverflow` if the logical point count or vector layout is not
    /// representable.
    pub fn points_vec(&self) -> crate::error::Result<Vec<Point>> {
        let count = ShapeCountQuery::default().coordinates(self)?;
        let capacity = admit_shape_vector_capacity::<Point>(count)?;
        let mut points = Vec::with_capacity(capacity);
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
        let capacity = admit_shape_vector_capacity::<Self>(unique.len())?;
        let mut members = Vec::with_capacity(capacity);
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

    /// Visit every segment-bearing coordinate chain — linestrings,
    /// multi-linestring members, and polygon rings, recursively through
    /// collections — as borrowed columns. The chain enumeration behind
    /// segment-level prepared state (facet linework).
    pub(crate) fn for_each_segment_chain(&self, mut visit: impl FnMut(&CoordSeq)) {
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.for_each_segment_chain_inner(&mut None, RunIdentity::XY, &mut |chain| {
                visit(chain);
                ControlFlow::Continue(())
            });
    }

    /// Visit every segment-bearing chain until `visit` returns `Err`.
    pub(crate) fn try_for_each_segment_chain<E>(
        &self,
        mut visit: impl FnMut(&CoordSeq) -> Result<(), E>,
    ) -> Result<(), E> {
        match self.for_each_segment_chain_inner(
            &mut None,
            RunIdentity::XY,
            &mut |chain| match visit(chain) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            },
        ) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }

    pub(crate) fn try_for_each_first_segment_chain<E>(
        &self,
        identity: RunIdentity,
        mut visit: impl FnMut(&CoordSeq) -> Result<(), E>,
    ) -> Result<(), E> {
        match self.for_each_first_segment_chain(identity, |chain| match visit(chain) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }

    pub(crate) fn for_each_first_segment_chain<B>(
        &self,
        identity: RunIdentity,
        mut visit: impl FnMut(&CoordSeq) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        let Some(mut owners) = SharedShapeOwners::for_shape(self) else {
            return self.for_each_segment_chain_inner(&mut None, identity, &mut visit);
        };
        self.for_each_segment_chain_inner(&mut Some(&mut owners), identity, &mut visit)
    }

    /// Visit distinct shared XY chains in first-occurrence order.
    pub(crate) fn for_each_first_xy_segment_chain(
        &self,
        visit: impl FnMut(&CoordSeq) -> ControlFlow<()>,
    ) -> ControlFlow<()> {
        self.for_each_first_segment_chain(RunIdentity::XY, visit)
    }

    fn for_each_segment_chain_inner<B>(
        &self,
        owners: &mut Option<&mut SharedShapeOwners>,
        identity: RunIdentity,
        visit: &mut impl FnMut(&CoordSeq) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => {},
            Self::LineString(points) => {
                if owners
                    .as_mut()
                    .is_none_or(|owners| owners.first_coord_seq(points.as_coords(), identity))
                {
                    visit(points.as_coords())?;
                }
            },
            Self::MultiLineString(lines) => {
                for line in lines {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(line.as_coords(), identity))
                    {
                        visit(line.as_coords())?;
                    }
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(ring.coords(), identity))
                    {
                        visit(ring.coords())?;
                    }
                }
            },
            Self::MultiPolygon(polygons) => {
                for ring in polygons.iter().flat_map(Polygon::rings) {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(ring.coords(), identity))
                    {
                        visit(ring.coords())?;
                    }
                }
            },
            Self::GeometryCollection(geometries) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(geometries.as_slice(), geometries.members_are_shared())
                }) {
                    for geometry in geometries {
                        geometry.for_each_segment_chain_inner(owners, identity, visit)?;
                    }
                }
            },
        }
        ControlFlow::Continue(())
    }

    pub fn parts(&self) -> ShapePartsIter<'_> {
        match self {
            Self::Point(_) | Self::LineString(_) | Self::Polygon(_) => {
                ShapePartsIter::Single(Some(self))
            },
            Self::MultiPoint(points) => {
                ShapePartsIter::MultiPoint(points, 0..points.member_count())
            },
            Self::MultiLineString(lines) => {
                ShapePartsIter::MultiLineString(lines, 0..lines.member_count())
            },
            Self::MultiPolygon(polygons) => {
                ShapePartsIter::MultiPolygon(polygons, 0..polygons.member_count())
            },
            Self::GeometryCollection(geometries) => {
                ShapePartsIter::GeometryCollection(geometries.iter())
            },
            Self::Empty(
                GeometryKind::Point | GeometryKind::LineString | GeometryKind::Polygon,
                _,
            ) => ShapePartsIter::Single(Some(self)),
            Self::Empty(..) => ShapePartsIter::Empty,
        }
    }

    /// Number of top-level parts, without materializing them — `1` for a single
    /// geometry, the member count for a multi/collection. The `O(1)`
    /// counterpart to `parts().count()`.
    #[expect(
        clippy::match_same_arms,
        reason = "the arms bind different member types; they cannot share a pattern"
    )]
    pub fn part_count(&self) -> usize {
        match self {
            Self::Point(_) | Self::LineString(_) | Self::Polygon(_) => 1,
            Self::MultiPoint(points) => points.member_count(),
            Self::MultiLineString(lines) => lines.member_count(),
            Self::MultiPolygon(polygons) => polygons.member_count(),
            Self::GeometryCollection(geometries) => geometries.len(),
            Self::Empty(
                GeometryKind::Point | GeometryKind::LineString | GeometryKind::Polygon,
                _,
            ) => 1,
            Self::Empty(..) => 0,
        }
    }

    /// The `index`-th top-level part, cloning only that part. `None` when out
    /// of range. Avoids the `parts().collect()` that clones every part to
    /// reach one.
    #[expect(
        clippy::indexing_slicing,
        reason = "member_at returns stored-value indices admitted by the multipart layout"
    )]
    pub fn part_at(&self, index: usize) -> Option<Self> {
        match self {
            Self::Point(_) | Self::LineString(_) | Self::Polygon(_) => {
                (index == 0).then(|| self.clone())
            },
            Self::MultiPoint(points) => points.member_at(index).map(|member| match member {
                crate::geometry::MultiMember::Empty => {
                    Self::typed_empty(GeometryKind::Point, points.axes())
                },
                crate::geometry::MultiMember::Value(value_index) => {
                    Self::Point(points.point_at(value_index))
                },
            }),
            Self::MultiLineString(lines) => lines.member_at(index).map(|member| match member {
                crate::geometry::MultiMember::Empty => {
                    Self::typed_empty(GeometryKind::LineString, lines.axes())
                },
                crate::geometry::MultiMember::Value(value_index) => {
                    Self::LineString(lines.as_slice()[value_index].clone())
                },
            }),
            Self::MultiPolygon(polygons) => polygons.member_at(index).map(|member| match member {
                crate::geometry::MultiMember::Empty => {
                    Self::typed_empty(GeometryKind::Polygon, polygons.axes())
                },
                crate::geometry::MultiMember::Value(value_index) => {
                    Self::Polygon(polygons.as_slice()[value_index].clone())
                },
            }),
            Self::GeometryCollection(geometries) => geometries.get(index).cloned(),
            Self::Empty(
                GeometryKind::Point | GeometryKind::LineString | GeometryKind::Polygon,
                _,
            ) => (index == 0).then(|| self.clone()),
            Self::Empty(..) => None,
        }
    }

    /// Exterior ring of a `Polygon` as a closed `LineString`. `None` for any
    /// other geometry kind (callers gate on `Shape::Polygon`).
    pub fn exterior(&self) -> Option<Self> {
        match self {
            Self::Polygon(polygon) => Some(Self::LineString(polygon.shell.as_line())),
            // `POLYGON EMPTY` has an empty shell; the shell carries the empty
            // polygon's declared axes.
            Self::Empty(GeometryKind::Polygon, axes) => {
                Some(Self::typed_empty(GeometryKind::LineString, *axes))
            },
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(
                GeometryKind::Point
                | GeometryKind::MultiPoint
                | GeometryKind::LineString
                | GeometryKind::MultiLineString
                | GeometryKind::MultiPolygon
                | GeometryKind::GeometryCollection,
                _,
            ) => None,
        }
    }

    /// Interior rings (holes) of a `Polygon`, each as a closed `LineString`.
    /// Empty for any other geometry kind.
    pub fn interiors(&self) -> Vec<Self> {
        match self {
            Self::Polygon(polygon) => polygon
                .holes
                .iter()
                .map(|hole| Self::LineString(hole.as_line()))
                .collect(),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => Vec::new(),
        }
    }

    pub fn bounds(&self) -> Option<Bounds> {
        if matches!(self, Self::GeometryCollection(_)) && shape_may_repeat_coord_run(self) {
            return ShapeBoundsMemo::for_shape(self).shape_bounds(self);
        }
        shape_bounds_with_coord_bounds(self, &mut |coords| Bounds::from_coords(coords))
    }

    pub fn area(&self) -> f64 {
        canonicalize_zero(match self {
            Self::Polygon(polygon) => polygon.area(),
            Self::MultiPolygon(polygons) => polygons.iter().map(Polygon::area).sum(),
            Self::GeometryCollection(geometries) => geometries.iter().map(Self::area).sum(),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Empty(..) => 0.0,
        })
    }

    pub fn length(&self) -> f64 {
        canonicalize_zero(match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => 0.0,
            Self::LineString(points) => line_length(points),
            Self::MultiLineString(lines) => lines.iter().map(|line| line_length(line)).sum(),
            Self::Polygon(polygon) => polygon.length(),
            Self::MultiPolygon(polygons) => polygons.iter().map(Polygon::length).sum(),
            Self::GeometryCollection(geometries) => geometries.iter().map(Self::length).sum(),
        })
    }

    /// Total 3D (Euclidean) length, folding `sqrt(dx² + dy² + dz²)` over every
    /// segment. Every vertex that participates in a segment must carry a Z
    /// ordinate; otherwise [`GeometryErrorKind::MissingZ`] is returned rather than
    /// silently treating the missing elevation as 0.
    pub fn length_3d(&self) -> Result<f64> {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => Ok(0.0),
            Self::LineString(points) => line_length_3d(points),
            Self::MultiLineString(lines) => lines.iter().map(line_length_3d).sum(),
            Self::Polygon(polygon) => polygon.rings().map(|ring| line_length_3d(&ring)).sum(),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .flat_map(Polygon::rings)
                .map(|ring| line_length_3d(&ring))
                .sum(),
            Self::GeometryCollection(geometries) => geometries.iter().map(Self::length_3d).sum(),
        }
        .map(canonicalize_zero)
    }

    /// Smallest and largest Z ordinates present in ONE traversal, or
    /// `None` if no vertex carries Z — the kernel behind `min_z`/`max_z`.
    pub fn z_extremes(&self) -> Option<(f64, f64)> {
        let mut extremes = None;
        self.fold_ord_extremes(&mut extremes, CoordSeq::zs, Point::z, Ordinate::Z);
        extremes
    }

    /// Smallest Z ordinate present, or `None` if no vertex carries Z.
    pub fn min_z(&self) -> Option<f64> {
        self.z_extremes().map(|(low, _)| low)
    }

    /// Largest Z ordinate present, or `None` if no vertex carries Z.
    pub fn max_z(&self) -> Option<f64> {
        self.z_extremes().map(|(_, high)| high)
    }

    /// Smallest and largest M ordinates present in ONE traversal, or
    /// `None` if no vertex carries M — the kernel behind `min_m`/`max_m`.
    pub fn m_extremes(&self) -> Option<(f64, f64)> {
        let mut extremes = None;
        self.fold_ord_extremes(&mut extremes, CoordSeq::ms, Point::m, Ordinate::M);
        extremes
    }

    /// Smallest M ordinate present, or `None` if no vertex carries M.
    pub fn min_m(&self) -> Option<f64> {
        self.m_extremes().map(|(low, _)| low)
    }

    /// Largest M ordinate present, or `None` if no vertex carries M.
    pub fn max_m(&self) -> Option<f64> {
        self.m_extremes().map(|(_, high)| high)
    }

    fn fold_ord_extremes<SeqOrd, PointOrd>(
        &self,
        extremes: &mut Option<(f64, f64)>,
        seq_ord: SeqOrd,
        point_ord: PointOrd,
        ordinate: Ordinate,
    ) where
        SeqOrd: for<'a> Fn(&'a CoordSeq) -> Option<&'a [f64]> + Copy,
        PointOrd: Fn(Point) -> Option<f64> + Copy,
    {
        match self {
            Self::Point(point) => {
                if let Some(value) = point_ord(*point) {
                    merge_ord_extremes(extremes, Some((value, value)));
                }
            },
            Self::MultiPoint(seq) => {
                merge_ord_extremes(extremes, seq_ord(seq).and_then(column_minmax));
            },
            Self::LineString(seq) => {
                merge_ord_extremes(extremes, seq_ord(seq).and_then(column_minmax));
            },
            Self::MultiLineString(lines) => {
                for line in lines {
                    merge_ord_extremes(extremes, seq_ord(line).and_then(column_minmax));
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    merge_ord_extremes(extremes, seq_ord(ring.coords()).and_then(column_minmax));
                }
            },
            Self::MultiPolygon(polygons) => {
                for polygon in polygons {
                    for ring in polygon.rings() {
                        merge_ord_extremes(
                            extremes,
                            seq_ord(ring.coords()).and_then(column_minmax),
                        );
                    }
                }
            },
            Self::GeometryCollection(_) => {
                merge_ord_extremes(
                    extremes,
                    OrdinateExtremaMemo::for_shape(self).shape_extrema(self, ordinate),
                );
            },
            Self::Empty(..) => {},
        }
    }
}

/// Exact-size top-level part iterator specialized once for the shape kind.
pub enum ShapePartsIter<'a> {
    Single(Option<&'a Shape>),
    MultiPoint(&'a MultiPoint, std::ops::Range<usize>),
    MultiLineString(&'a MultiLineString, std::ops::Range<usize>),
    MultiPolygon(&'a MultiPolygon, std::ops::Range<usize>),
    GeometryCollection(std::slice::Iter<'a, Shape>),
    Empty,
}

impl Iterator for ShapePartsIter<'_> {
    type Item = Shape;

    #[expect(
        clippy::indexing_slicing,
        reason = "member_at returns stored-value indices admitted by the multipart layout"
    )]
    fn next(&mut self) -> Option<Shape> {
        match self {
            Self::Single(shape) => shape.take().cloned(),
            Self::MultiPoint(points, indices) => indices.next().map(|index| {
                match points.member_at(index).expect("part index in range") {
                    MultiMember::Empty => Shape::typed_empty(GeometryKind::Point, points.axes()),
                    MultiMember::Value(value_index) => Shape::Point(points.point_at(value_index)),
                }
            }),
            Self::MultiLineString(lines, indices) => indices.next().map(|index| {
                match lines.member_at(index).expect("part index in range") {
                    MultiMember::Empty => {
                        Shape::typed_empty(GeometryKind::LineString, lines.axes())
                    },
                    MultiMember::Value(value_index) => {
                        Shape::LineString(lines.as_slice()[value_index].clone())
                    },
                }
            }),
            Self::MultiPolygon(polygons, indices) => indices.next().map(|index| {
                match polygons.member_at(index).expect("part index in range") {
                    MultiMember::Empty => {
                        Shape::typed_empty(GeometryKind::Polygon, polygons.axes())
                    },
                    MultiMember::Value(value_index) => {
                        Shape::Polygon(polygons.as_slice()[value_index].clone())
                    },
                }
            }),
            Self::GeometryCollection(geometries) => geometries.next().cloned(),
            Self::Empty => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
}

impl DoubleEndedIterator for ShapePartsIter<'_> {
    #[expect(
        clippy::indexing_slicing,
        reason = "member_at returns stored-value indices admitted by the multipart layout"
    )]
    fn next_back(&mut self) -> Option<Shape> {
        match self {
            Self::Single(shape) => shape.take().cloned(),
            Self::MultiPoint(points, indices) => indices.next_back().map(|index| {
                match points.member_at(index).expect("part index in range") {
                    MultiMember::Empty => Shape::typed_empty(GeometryKind::Point, points.axes()),
                    MultiMember::Value(value_index) => Shape::Point(points.point_at(value_index)),
                }
            }),
            Self::MultiLineString(lines, indices) => indices.next_back().map(|index| {
                match lines.member_at(index).expect("part index in range") {
                    MultiMember::Empty => {
                        Shape::typed_empty(GeometryKind::LineString, lines.axes())
                    },
                    MultiMember::Value(value_index) => {
                        Shape::LineString(lines.as_slice()[value_index].clone())
                    },
                }
            }),
            Self::MultiPolygon(polygons, indices) => {
                indices.next_back().map(|index| {
                    match polygons.member_at(index).expect("part index in range") {
                        MultiMember::Empty => {
                            Shape::typed_empty(GeometryKind::Polygon, polygons.axes())
                        },
                        MultiMember::Value(value_index) => {
                            Shape::Polygon(polygons.as_slice()[value_index].clone())
                        },
                    }
                })
            },
            Self::GeometryCollection(geometries) => geometries.next_back().cloned(),
            Self::Empty => None,
        }
    }
}

impl ExactSizeIterator for ShapePartsIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Single(shape) => usize::from(shape.is_some()),
            Self::MultiPoint(_, indices)
            | Self::MultiLineString(_, indices)
            | Self::MultiPolygon(_, indices) => indices.len(),
            Self::GeometryCollection(geometries) => geometries.len(),
            Self::Empty => 0,
        }
    }
}

/// Every non-empty vertex carries Z: empty shapes are vacuously `true`;
/// points require `z()`; `CoordSeq` requires a Z column; collections require
/// all children. Unlike [`Shape::has_z`], a mixed-axis collection is `false`.
pub(crate) fn shape_axes_all_z(shape: &Shape) -> bool {
    shape_axes_all_ord(
        shape,
        CoordSeq::zs,
        Point::z,
        &mut SharedShapeOwners::for_collections(),
    )
}

fn shape_axes_all_ord<SeqOrd, PointOrd>(
    shape: &Shape,
    seq_ord: SeqOrd,
    point_ord: PointOrd,
    owners: &mut SharedShapeOwners,
) -> bool
where
    SeqOrd: for<'a> Fn(&'a CoordSeq) -> Option<&'a [f64]> + Copy,
    PointOrd: Fn(Point) -> Option<f64> + Copy,
{
    match shape {
        Shape::Empty(..) => true,
        Shape::Point(point) => point_ord(*point).is_some(),
        Shape::MultiPoint(points) => seq_ord(points).is_some(),
        Shape::LineString(points) => seq_ord(points).is_some(),
        Shape::MultiLineString(lines) => lines.iter().all(|line| seq_ord(line).is_some()),
        Shape::Polygon(polygon) => polygon.rings().all(|ring| seq_ord(ring).is_some()),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .all(|polygon| polygon.rings().all(|ring| seq_ord(ring).is_some())),
        Shape::GeometryCollection(parts) => {
            !owners.first_collection(parts.as_slice(), parts.members_are_shared())
                || parts
                    .iter()
                    .all(|part| shape_axes_all_ord(part, seq_ord, point_ord, owners))
        },
    }
}

const fn merge_ord_extremes(acc: &mut Option<(f64, f64)>, chunk: Option<(f64, f64)>) {
    if let Some((lo, hi)) = chunk {
        *acc = Some(match *acc {
            Some((low, high)) => (low.min(lo), high.max(hi)),
            None => (lo, hi),
        });
    }
}

/// First vertex of the first non-empty line piece, if any.
pub(crate) fn linework_first_point(lines: &[&CoordSeq]) -> Option<Point> {
    lines.iter().find_map(|line| line.first())
}

impl Shape {
    /// Visit every atomic [`Component`], recursing through collections and
    /// skipping empties.
    /// Visit every [`CoordRun`] of this shape in flatten order, recursing
    /// through collections and skipping the empty variants.
    ///
    /// The visitor's return type carries the break: `Result` stops at the
    /// first error, `ControlFlow` at the first `Break`.
    pub(crate) fn for_each_coord_run<'a, R: std::ops::Try<Output = ()>>(
        &'a self,
        visit: &mut impl FnMut(CoordRun<'a>) -> R,
    ) -> R {
        self.for_each_coord_run_inner(&mut None, RunIdentity::Full, visit)
    }

    fn for_each_coord_run_inner<'a, R: std::ops::Try<Output = ()>>(
        &'a self,
        owners: &mut Option<&mut SharedShapeOwners>,
        identity: RunIdentity,
        visit: &mut impl FnMut(CoordRun<'a>) -> R,
    ) -> R {
        match self {
            Self::Point(point) => visit(CoordRun::Point(*point))?,
            Self::MultiPoint(points) => {
                if owners
                    .as_mut()
                    .is_none_or(|owners| owners.first_coord_seq(points, identity))
                {
                    visit(CoordRun::Seq(points))?;
                }
            },
            Self::LineString(line) => {
                if owners
                    .as_mut()
                    .is_none_or(|owners| owners.first_coord_seq(line.as_coords(), identity))
                {
                    visit(CoordRun::Seq(line))?;
                }
            },
            Self::MultiLineString(lines) => {
                for line in lines {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(line.as_coords(), identity))
                    {
                        visit(CoordRun::Seq(line))?;
                    }
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(ring.coords(), identity))
                    {
                        visit(CoordRun::Seq(ring.coords()))?;
                    }
                }
            },
            Self::MultiPolygon(polygons) => {
                for ring in polygons.iter().flat_map(Polygon::rings) {
                    if owners
                        .as_mut()
                        .is_none_or(|owners| owners.first_coord_seq(ring.coords(), identity))
                    {
                        visit(CoordRun::Seq(ring.coords()))?;
                    }
                }
            },
            Self::GeometryCollection(parts) => {
                if owners.as_mut().is_none_or(|owners| {
                    owners.first_collection(parts.as_slice(), parts.members_are_shared())
                }) {
                    for part in parts {
                        part.for_each_coord_run_inner(owners, identity, visit)?;
                    }
                }
            },
            Self::Empty(..) => {},
        }
        R::from_output(())
    }

    /// [`Self::try_for_each_component`] for a visitor that cannot fail.
    pub(crate) fn for_each_component<'a>(&'a self, visit: &mut impl FnMut(Component<'a>)) {
        match self.try_for_each_component(
            &mut |component| -> Result<(), std::convert::Infallible> {
                visit(component);
                Ok(())
            },
        ) {
            Ok(()) => {},
            Err(never) => match never {},
        }
    }

    pub(crate) fn try_for_each_component<'a, E>(
        &'a self,
        visit: &mut impl FnMut(Component<'a>) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Point(point) => visit(Component::Point(*point))?,
            Self::LineString(line) => visit(Component::LineString(line))?,
            Self::Polygon(polygon) => visit(Component::Polygon(polygon))?,
            Self::MultiPoint(points) => {
                for point in points {
                    visit(Component::Point(point))?;
                }
            },
            Self::MultiLineString(lines) => {
                for line in lines {
                    visit(Component::LineString(line))?;
                }
            },
            Self::MultiPolygon(polygons) => {
                for polygon in polygons {
                    visit(Component::Polygon(polygon))?;
                }
            },
            Self::GeometryCollection(parts) => {
                for part in parts {
                    part.try_for_each_component(visit)?;
                }
            },
            Self::Empty(..) => {},
        }
        Ok(())
    }
}

#[cfg(test)]
mod parts_iter_tests {
    use super::*;

    #[test]
    fn parts_iterator_is_exact_and_exhausts_from_both_ends() {
        let points = [
            Point::new_raw_xy(1.0, 2.0),
            Point::new_raw_xy(3.0, 4.0),
            Point::new_raw_xy(5.0, 6.0),
        ];
        let shape = Shape::MultiPoint(points.to_vec().into());
        let mut parts = shape.parts();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts.next(), Some(Shape::Point(points[0])));
        assert_eq!(parts.len(), 2);
        assert_eq!(parts.next_back(), Some(Shape::Point(points[2])));
        assert_eq!(parts.next(), Some(Shape::Point(points[1])));
        assert_eq!(parts.len(), 0);
        assert_eq!(parts.next(), None);
        assert_eq!(parts.next_back(), None);

        assert_eq!(
            Shape::Empty(GeometryKind::Point, CoordinateAxes::XY)
                .parts()
                .len(),
            1
        );
    }
}

#[cfg(test)]
mod unique_points_capacity_tests {
    use super::*;

    #[test]
    fn oversized_unique_point_capacity_is_only_a_hint() {
        let capacity = 1_usize << 57;
        // The vector layout fits, but the hash table cannot represent this
        // many points on the supported 64-bit target.
        let _layout = std::alloc::Layout::array::<Point>(capacity)
            .expect("point vector layout fits on the supported 64-bit target");

        let mut seen = HashSet::new();
        let mut unique = Vec::new();
        reserve_unique_points_capacity(capacity, &mut seen, &mut unique);

        assert!(seen.is_empty());
        assert!(unique.is_empty());
    }
}
