use super::owners::{reserve_owner_map, shape_may_repeat_coord_run};
use super::{
    CoordSeq, CoordinateAxes, HasM, HasZ, Hash, HashMap, Point, Shape, ShapeOwnerKey,
    SharedShapeOwners, column_minmax,
};

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
                    .flat_map(crate::geometry::Polygon::rings)
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

impl Shape {
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
