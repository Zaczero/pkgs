use std::collections::HashSet;
use std::convert::Infallible;
use std::ops::ControlFlow;

use crate::error::Result;
use crate::geometry::access::{bounds_from_iter, canonicalize_zero, column_minmax, line_length};
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, Dimension, GeometryErrorKind, GeometryKind, HasM, HasZ,
    LineSeq, MultiLineString, MultiMember, MultiPoint, MultiPolygon, Point, Polygon, Shape,
    ShapeCountQuery, admit_shape_vector_capacity, line_length_3d,
};

fn reserve_unique_points_capacity(
    capacity: usize,
    seen: &mut HashSet<Point>,
    unique: &mut Vec<Point>,
) {
    // The coordinate count bounds both containers but can greatly exceed the
    // number of distinct points. A failed reservation only loses this hint;
    // normal growth still follows the values actually inserted.
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
        self.fold_axes(&mut has_z, &mut has_m);
        CoordinateAxes::new(HasZ(has_z), HasM(has_m))
    }

    fn fold_axes(&self, has_z: &mut bool, has_m: &mut bool) {
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
                for geometry in geometries {
                    geometry.fold_axes(has_z, has_m);
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
        self.fold_axes(&mut has_z, &mut has_m);
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
        self.fold_axes(&mut has_z, &mut has_m);
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
        let capacity = ShapeCountQuery::default().coordinates(self).ok();
        let mut seen = HashSet::<Point>::new();
        let mut unique = Vec::new();
        if let Some(capacity) = capacity {
            reserve_unique_points_capacity(capacity, &mut seen, &mut unique);
        }
        self.for_each_point(|point| {
            if seen.insert(point) {
                unique.push(point);
            }
        });
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

    /// Visit every segment-bearing coordinate chain — linestrings,
    /// multi-linestring members, and polygon rings, recursively through
    /// collections — as borrowed columns. The chain enumeration behind
    /// segment-level prepared state (facet linework).
    pub(crate) fn for_each_segment_chain(&self, mut visit: impl FnMut(&CoordSeq)) {
        let ControlFlow::Continue(()): ControlFlow<Infallible> =
            self.for_each_segment_chain_inner(&mut |chain| {
                visit(chain);
                ControlFlow::Continue(())
            });
    }

    /// Visit every segment-bearing chain until `visit` returns `Err`.
    pub(crate) fn try_for_each_segment_chain<E>(
        &self,
        mut visit: impl FnMut(&CoordSeq) -> Result<(), E>,
    ) -> Result<(), E> {
        match self.for_each_segment_chain_inner(&mut |chain| match visit(chain) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }

    fn for_each_segment_chain_inner<B, F: FnMut(&CoordSeq) -> ControlFlow<B>>(
        &self,
        visit: &mut F,
    ) -> ControlFlow<B> {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => {},
            Self::LineString(points) => visit(points)?,
            Self::MultiLineString(lines) => {
                for line in lines {
                    visit(line)?;
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    visit(ring)?;
                }
            },
            Self::MultiPolygon(polygons) => {
                for ring in polygons.iter().flat_map(Polygon::rings) {
                    visit(ring)?;
                }
            },
            Self::GeometryCollection(geometries) => {
                for geometry in geometries {
                    geometry.for_each_segment_chain_inner(visit)?;
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
        match self {
            Self::Point(point) => Some(Bounds::from_point(*point)),
            Self::MultiPoint(points) => Bounds::from_coords(points),
            Self::LineString(points) => Bounds::from_coords(points),
            Self::MultiLineString(lines) => {
                bounds_from_iter(lines.iter().filter_map(|line| Bounds::from_coords(line)))
            },
            Self::Polygon(polygon) => polygon.bounds(),
            Self::MultiPolygon(polygons) => {
                bounds_from_iter(polygons.iter().filter_map(Polygon::bounds))
            },
            Self::GeometryCollection(geometries) => {
                bounds_from_iter(geometries.iter().filter_map(Self::bounds))
            },
            Self::Empty(..) => None,
        }
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
        self.fold_ord_extremes(&mut extremes, CoordSeq::zs, Point::z);
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
        self.fold_ord_extremes(&mut extremes, CoordSeq::ms, Point::m);
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
                    merge_ord_extremes(extremes, seq_ord(ring).and_then(column_minmax));
                }
            },
            Self::MultiPolygon(polygons) => {
                for polygon in polygons {
                    for ring in polygon.rings() {
                        merge_ord_extremes(extremes, seq_ord(ring).and_then(column_minmax));
                    }
                }
            },
            Self::GeometryCollection(geometries) => {
                for geometry in geometries {
                    geometry.fold_ord_extremes(extremes, seq_ord, point_ord);
                }
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
    shape_axes_all_ord(shape, CoordSeq::zs, Point::z)
}

fn shape_axes_all_ord<SeqOrd, PointOrd>(shape: &Shape, seq_ord: SeqOrd, point_ord: PointOrd) -> bool
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
        Shape::GeometryCollection(parts) => parts
            .iter()
            .all(|part| shape_axes_all_ord(part, seq_ord, point_ord)),
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
        match self {
            Self::Point(point) => visit(CoordRun::Point(*point))?,
            Self::MultiPoint(points) => visit(CoordRun::Seq(points))?,
            Self::LineString(line) => visit(CoordRun::Seq(line))?,
            Self::MultiLineString(lines) => {
                for line in lines {
                    visit(CoordRun::Seq(line))?;
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    visit(CoordRun::Seq(ring.coords()))?;
                }
            },
            Self::MultiPolygon(polygons) => {
                for ring in polygons.iter().flat_map(Polygon::rings) {
                    visit(CoordRun::Seq(ring.coords()))?;
                }
            },
            Self::GeometryCollection(parts) => {
                for part in parts {
                    part.for_each_coord_run(visit)?;
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
        assert!(std::alloc::Layout::array::<Point>(capacity).is_ok());

        let mut seen = HashSet::new();
        let mut unique = Vec::new();
        reserve_unique_points_capacity(capacity, &mut seen, &mut unique);

        assert!(seen.is_empty());
        assert!(unique.is_empty());
    }
}
