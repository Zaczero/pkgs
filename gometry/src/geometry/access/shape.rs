#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::error::Result;
use crate::geometry::*;
use crate::py::support::Bounds3D;

impl Shape {
    pub const fn geometry_type(&self) -> &'static str {
        match self {
            Self::Point(_) => "Point",
            Self::MultiPoint(_) => "MultiPoint",
            Self::LineString(_) => "LineString",
            Self::MultiLineString(_) => "MultiLineString",
            Self::Polygon(_) => "Polygon",
            Self::MultiPolygon(_) => "MultiPolygon",
            Self::GeometryCollection(_) => "GeometryCollection",
            Self::Empty(kind, _) => kind.geometry_type(),
        }
    }

    pub fn topological_dimension(&self) -> Dimension {
        match self {
            Self::Point(_) | Self::MultiPoint(_) => Dimension::Point,
            Self::LineString(_) | Self::MultiLineString(_) => Dimension::Curve,
            Self::Polygon(_) | Self::MultiPolygon(_) => Dimension::Surface,
            Self::Empty(kind, _) => kind.topological_dimension(),
            Self::GeometryCollection(geometries) => geometries
                .iter()
                .map(Self::topological_dimension)
                .max()
                .unwrap_or(Dimension::Point),
        }
    }

    /// The axes carried by this geometry's coordinates (the union over all
    /// vertices). The single source of truth for dimensionality.
    pub fn axes(&self) -> CoordinateAxes {
        // Ordinate presence is a per-chain COLUMN flag in the SoA storage
        // (`CoordSeq.zs/ms: Option<Box<[f64]>>`), so this folds O(parts)
        // flags — never a per-vertex scan.
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
                for line in lines {
                    *has_z |= line.zs().is_some();
                    *has_m |= line.ms().is_some();
                }
            },
            Self::Polygon(polygon) => {
                for ring in polygon.rings() {
                    *has_z |= ring.zs().is_some();
                    *has_m |= ring.ms().is_some();
                }
            },
            Self::MultiPolygon(polygons) => {
                for polygon in polygons {
                    for ring in polygon.rings() {
                        *has_z |= ring.zs().is_some();
                        *has_m |= ring.ms().is_some();
                    }
                }
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
    pub fn for_each_point(&self, mut visit: impl FnMut(Point)) {
        self.for_each_point_inner(&mut visit);
    }

    fn for_each_point_inner<F: FnMut(Point)>(&self, visit: &mut F) {
        match self {
            Self::Point(point) => visit(*point),
            Self::MultiPoint(points) => {
                for point in points {
                    visit(point);
                }
            },
            Self::LineString(points) => {
                for point in points {
                    visit(point);
                }
            },
            Self::MultiLineString(lines) => {
                for point in lines.iter().flatten() {
                    visit(point);
                }
            },
            Self::Polygon(polygon) => {
                for point in polygon.points() {
                    visit(point);
                }
            },
            Self::MultiPolygon(polygons) => {
                for point in polygons.iter().flat_map(Polygon::points) {
                    visit(point);
                }
            },
            Self::GeometryCollection(geometries) => {
                for geometry in geometries {
                    geometry.for_each_point_inner(visit);
                }
            },
            Self::Empty(..) => {},
        }
    }

    /// Whether any vertex satisfies `predicate`, short-circuiting on the first
    /// match. The non-boxing counterpart to `points().any(...)`.
    pub fn any_point(&self, mut predicate: impl FnMut(Point) -> bool) -> bool {
        self.any_point_inner(&mut predicate)
    }

    fn any_point_inner<F: FnMut(Point) -> bool>(&self, predicate: &mut F) -> bool {
        match self {
            Self::Point(point) => predicate(*point),
            Self::MultiPoint(points) => points.iter().any(&mut *predicate),
            Self::LineString(points) => points.iter().any(&mut *predicate),
            Self::MultiLineString(lines) => lines.iter().flatten().any(&mut *predicate),
            Self::Polygon(polygon) => polygon.points().any(&mut *predicate),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .any(|polygon| polygon.points().any(&mut *predicate)),
            Self::GeometryCollection(geometries) => geometries
                .iter()
                .any(|geometry| geometry.any_point_inner(predicate)),
            Self::Empty(..) => false,
        }
    }

    /// Visit every vertex until `visit` returns `Err`, short-circuiting — the
    /// fallible counterpart to [`for_each_point`](Self::for_each_point), for
    /// hot scans that validate each vertex without allocating a boxed
    /// iterator.
    pub fn try_for_each_point<E>(
        &self,
        mut visit: impl FnMut(Point) -> Result<(), E>,
    ) -> Result<(), E> {
        self.try_for_each_point_inner(&mut visit)
    }

    fn try_for_each_point_inner<E, F: FnMut(Point) -> Result<(), E>>(
        &self,
        visit: &mut F,
    ) -> Result<(), E> {
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
        Ok(())
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
    /// min-clearance) that need an owned vertex buffer.
    pub fn points_vec(&self) -> Vec<Point> {
        let mut points = Vec::with_capacity(self.coord_count());
        self.collect_points_into(&mut points);
        points
    }

    /// Distinct vertices in first-occurrence order, as a `MultiPoint`.
    /// The dedup key is full structural identity (axes + every active
    /// ordinate by bit pattern, the `equals_identical` notion) — unlike
    /// `remove_repeated_points`, which collapses only consecutive runs
    /// within a tolerance.
    pub fn unique_points(&self) -> Self {
        // A point atom is already its own unique set — no hashing.
        if let Self::Point(point) = self {
            return Self::MultiPoint(vec![*point].into());
        }
        let capacity = self.coord_count();
        let mut seen = HashSet::with_capacity(capacity);
        let mut unique: Vec<Point> = Vec::with_capacity(capacity);
        self.for_each_point(|point| {
            if seen.insert(point) {
                unique.push(point);
            }
        });
        Self::MultiPoint(unique.into())
    }

    /// Visit every segment-bearing coordinate chain — linestrings,
    /// multi-linestring members, and polygon rings, recursively through
    /// collections — as borrowed columns. The chain enumeration behind
    /// segment-level prepared state (facet linework).
    pub(crate) fn for_each_segment_chain(&self, mut visit: impl FnMut(&CoordSeq)) {
        self.for_each_segment_chain_inner(&mut visit);
    }

    fn for_each_segment_chain_inner<F: FnMut(&CoordSeq)>(&self, visit: &mut F) {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => {},
            Self::LineString(points) => visit(points),
            Self::MultiLineString(lines) => lines.iter().for_each(|line| visit(line)),
            Self::Polygon(polygon) => polygon.rings().for_each(&mut *visit),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .flat_map(Polygon::rings)
                .for_each(&mut *visit),
            Self::GeometryCollection(geometries) => {
                for geometry in geometries {
                    geometry.for_each_segment_chain_inner(visit);
                }
            },
        }
    }

    /// Borrowed counterpart of [`parts`](Self::parts): test the top-level
    /// parts without cloning coordinate storage. Collection members that are
    /// themselves multi/collection shapes are visited as
    /// [`ShapePart::Nested`] (matching `parts()`, which does not recurse).
    pub(crate) fn any_part(&self, mut predicate: impl FnMut(ShapePart<'_>) -> bool) -> bool {
        match self {
            Self::Point(point) => predicate(ShapePart::Point(*point)),
            Self::LineString(points) => predicate(ShapePart::LineString(points)),
            Self::Polygon(polygon) => predicate(ShapePart::Polygon(polygon)),
            Self::MultiPoint(points) => points
                .iter()
                .any(|point| predicate(ShapePart::Point(point))),
            Self::MultiLineString(lines) => lines
                .iter()
                .any(|line| predicate(ShapePart::LineString(line))),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .any(|polygon| predicate(ShapePart::Polygon(polygon))),
            Self::GeometryCollection(geometries) => geometries.iter().any(|geometry| {
                predicate(match geometry {
                    Self::Point(point) => ShapePart::Point(*point),
                    Self::LineString(points) => ShapePart::LineString(points),
                    Self::Polygon(polygon) => ShapePart::Polygon(polygon),
                    nested => ShapePart::Nested(nested),
                })
            }),
            Self::Empty(..) => false,
        }
    }

    pub fn parts(&self) -> Box<dyn Iterator<Item = Self> + '_> {
        match self {
            Self::Point(_) | Self::LineString(_) | Self::Polygon(_) => {
                Box::new(std::iter::once(self.clone()))
            },
            Self::MultiPoint(points) => Box::new(points.iter().map(Self::Point)),
            Self::MultiLineString(lines) => {
                Box::new(lines.iter().map(|line| Self::LineString(line.clone())))
            },
            Self::MultiPolygon(polygons) => Box::new(polygons.iter().cloned().map(Self::Polygon)),
            Self::GeometryCollection(geometries) => Box::new(geometries.iter().cloned()),
            Self::Empty(..) => Box::new(std::iter::empty()),
        }
    }

    /// Total vertex count across every part — `O(parts)`, for pre-sizing
    /// vertex collectors on hot paths instead of growing a `Vec` from empty.
    pub fn coord_count(&self) -> usize {
        match self {
            Self::Point(_) => 1,
            Self::MultiPoint(coords) => coords.coord_count(),
            Self::LineString(coords) => coords.coord_count(),
            Self::MultiLineString(lines) => lines.iter().map(Coordinates::coord_count).sum(),
            Self::Polygon(polygon) => polygon.coord_count(),
            Self::MultiPolygon(polygons) => polygons.iter().map(Polygon::coord_count).sum(),
            Self::GeometryCollection(geometries) => geometries.iter().map(Self::coord_count).sum(),
            Self::Empty(..) => 0,
        }
    }

    /// Number of top-level parts, without materializing them — `1` for a single
    /// geometry, the member count for a multi/collection. The `O(1)`
    /// counterpart to `parts().count()`.
    pub fn part_count(&self) -> usize {
        match self {
            Self::Point(_) | Self::LineString(_) | Self::Polygon(_) => 1,
            Self::MultiPoint(points) => points.len(),
            Self::MultiLineString(lines) => lines.len(),
            Self::MultiPolygon(polygons) => polygons.len(),
            Self::GeometryCollection(geometries) => geometries.len(),
            Self::Empty(..) => 0,
        }
    }

    /// The `index`-th top-level part, cloning only that part. `None` when out
    /// of range. Avoids the `parts().collect()` that clones every part to
    /// reach one.
    pub fn part_at(&self, index: usize) -> Option<Self> {
        match self {
            Self::Point(_) | Self::LineString(_) | Self::Polygon(_) => {
                (index == 0).then(|| self.clone())
            },
            Self::MultiPoint(points) => points.get(index).map(Self::Point),
            Self::MultiLineString(lines) => {
                lines.get(index).map(|line| Self::LineString(line.clone()))
            },
            Self::MultiPolygon(polygons) => polygons
                .get(index)
                .map(|polygon| Self::Polygon(polygon.clone())),
            Self::GeometryCollection(geometries) => geometries.get(index).cloned(),
            Self::Empty(..) => None,
        }
    }

    /// Visit every polygon ring (shells then holes, per polygon) as
    /// borrowed columns; non-polygonal shapes have none.
    pub fn for_each_ring(&self, mut visit: impl FnMut(&CoordSeq)) {
        match self {
            Self::Polygon(polygon) => polygon.rings().for_each(&mut visit),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .flat_map(Polygon::rings)
                .for_each(&mut visit),
            _ => {},
        }
    }

    /// Exterior ring of a `Polygon` as a closed `LineString`. `None` for any
    /// other geometry kind (callers gate on `Shape::Polygon`).
    pub fn exterior(&self) -> Option<Self> {
        match self {
            Self::Polygon(polygon) => Some(Self::LineString(
                LineSeq::try_new(polygon.shell.coords().clone())
                    .expect("polygon shell is a valid line"),
            )),
            // `POLYGON EMPTY` has an (empty) shell, mirroring Shapely's
            // `Polygon().exterior == LINEARRING EMPTY`; the shell carries the
            // empty polygon's declared axes.
            Self::Empty(EmptyKind::Polygon, axes) => Some(Self::LineString(LineSeq::empty(*axes))),
            _ => None,
        }
    }

    /// Interior rings (holes) of a `Polygon`, each as a closed `LineString`.
    /// Empty for any other geometry kind.
    pub fn interiors(&self) -> Vec<Self> {
        match self {
            Self::Polygon(polygon) => polygon
                .holes
                .iter()
                .map(|hole| {
                    Self::LineString(
                        LineSeq::try_new(hole.coords().clone())
                            .expect("polygon hole is a valid line"),
                    )
                })
                .collect(),
            _ => Vec::new(),
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
    /// `None` if no vertex carries Z — the kernel behind `min_z`/`max_z`
    /// and the single-pass `z_range`.
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
    /// `None` if no vertex carries M — the kernel behind `min_m`/`max_m`
    /// and the single-pass `m_range`.
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

    /// 3D bounding box `(minx, miny, minz, maxx, maxy, maxz)`, or `None` when
    /// the geometry is empty or carries no Z ordinate.
    pub fn bounds_3d(&self) -> Option<Bounds3D> {
        // XY from the column-native `bounds()` reducer; Z from the Z-column
        // `column_minmax` fold. `None` unless there is at least one vertex
        // and at least one Z, matching `bounds()` + `min_z()`/`max_z()`.
        let bounds = self.bounds()?;
        let (minz, maxz) = self.z_extremes()?;
        Some(Bounds3D {
            minx: bounds.minx(),
            miny: bounds.miny(),
            minz,
            maxx: bounds.maxx(),
            maxy: bounds.maxy(),
            maxz,
        })
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

/// Borrowed view of one top-level part — the clone-free currency of
/// `Shape::any_part`.
///
/// Single geometries borrow their coordinate storage; a collection member
/// that is itself a multi/collection arrives as [`Nested`](Self::Nested).
pub(crate) enum ShapePart<'a> {
    Point(Point),
    LineString(&'a CoordSeq),
    Polygon(&'a Polygon),
    Nested(&'a Shape),
}

impl ShapePart<'_> {
    /// Planar bounds of this part (`None` for an empty nested member).
    pub(crate) fn bounds(&self) -> Option<Bounds> {
        match self {
            Self::Point(point) => Some(Bounds::from_point(*point)),
            Self::LineString(points) => Bounds::from_coords(points),
            Self::Polygon(polygon) => polygon.bounds(),
            Self::Nested(shape) => shape.bounds(),
        }
    }
}

/// First vertex of the first non-empty line piece, if any.
pub(crate) fn linework_first_point(lines: &[&CoordSeq]) -> Option<Point> {
    lines.iter().find_map(|line| line.first())
}
