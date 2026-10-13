use super::*;

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

impl Shape {
    pub fn bounds(&self) -> Option<Bounds> {
        if matches!(self, Self::GeometryCollection(_)) && shape_may_repeat_coord_run(self) {
            return ShapeBoundsMemo::for_shape(self).shape_bounds(self);
        }
        shape_bounds_with_coord_bounds(self, &mut |coords| Bounds::from_coords(coords))
    }
}
