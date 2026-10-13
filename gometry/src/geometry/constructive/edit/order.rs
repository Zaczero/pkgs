use super::{
    HashMap, Polygon, Result, Shape, ShapeOwnerKey, compare_point_slices, compare_points,
    compare_polygons, compare_shapes,
};

impl Shape {
    /// Swap the X and Y ordinates of every coordinate (Z/M untouched) —
    /// the axis-order repair for latitude/longitude-ordered data.
    pub fn swap_xy(&self) -> Result<Self> {
        // O(1) column swap per sequence (Arc-pointer swap, no per-vertex copy);
        // same coordinates swapped, bit-identical to a `(x, y) → (y, x)` map.
        self.try_map_coordseqs(
            |seq| Ok(seq.swap_xy()),
            |point| Ok(point.with_xy_raw(point.y, point.x)),
        )
    }

    pub fn reverse(&self) -> Result<Self> {
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => Self::MultiPoint(points.clone()),
            // Columnar reverse-copies — no AoS `Vec<Point>` round-trip.
            Self::LineString(points) => Self::LineString(points.reversed()?),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines.try_map_values(lines.axes(), crate::geometry::LineSeq::reversed)?,
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.reverse()?),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.try_map_values(polygons.axes(), Polygon::reverse)?)
            },
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.try_map_members(Self::reverse)?)
            },
            Self::Empty(..) => self.clone(),
        })
    }

    pub fn orient_polygons(&self, exterior_cw: bool) -> Result<Self> {
        self.orient_polygons_with_owners(exterior_cw, &mut None)
    }

    fn orient_polygons_with_owners(
        &self,
        exterior_cw: bool,
        owners: &mut Option<HashMap<ShapeOwnerKey, Self>>,
    ) -> Result<Self> {
        Ok(match self {
            Self::Polygon(polygon) => Self::Polygon(polygon.orient(exterior_cw)?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons.try_map_values(polygons.axes(), |polygon| polygon.orient(exterior_cw))?,
            ),
            Self::GeometryCollection(geometries) => {
                let owner = geometries
                    .members_are_shared()
                    .then(|| ShapeOwnerKey::new(geometries.as_slice()));
                if let Some(oriented) = owner
                    .and_then(|owner| owners.as_ref()?.get(&owner))
                    .cloned()
                {
                    return Ok(oriented);
                }
                let oriented =
                    Self::GeometryCollection(geometries.try_map_members(|geometry| {
                        geometry.orient_polygons_with_owners(exterior_cw, owners)
                    })?);
                if let Some(owner) = owner {
                    owners
                        .get_or_insert_with(HashMap::new)
                        .insert(owner, oriented.clone());
                }
                oriented
            },
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Empty(..) => self.clone(),
        })
    }

    pub fn normalize(&self) -> Result<Self> {
        Ok(match self {
            Self::Point(point) => Self::Point(*point),
            Self::MultiPoint(points) => {
                let mut values = points.to_vec();
                values.sort_unstable_by(compare_points);
                Self::MultiPoint(points.with_values(&values)?)
            },
            Self::LineString(points) => Self::LineString(points.normalized()?),
            Self::MultiLineString(lines) => {
                let mut values = Vec::with_capacity(lines.len());
                for line in lines.iter() {
                    values.push(line.normalized()?);
                }
                values.sort_unstable_by(compare_point_slices);
                Self::MultiLineString(lines.with_values(values))
            },
            Self::Polygon(polygon) => Self::Polygon(polygon.normalize()?),
            Self::MultiPolygon(polygons) => {
                let mut values = Vec::with_capacity(polygons.len());
                for polygon in polygons.iter() {
                    values.push(polygon.normalize()?);
                }
                values.sort_unstable_by(compare_polygons);
                Self::MultiPolygon(polygons.with_values(values))
            },
            Self::GeometryCollection(geometries) => {
                let mut members = Vec::with_capacity(geometries.len());
                for geometry in geometries {
                    members.push(geometry.normalize()?);
                }
                members.sort_unstable_by(compare_shapes);
                Self::GeometryCollection(geometries.try_rebuild_members(members)?)
            },
            Self::Empty(..) => self.clone(),
        })
    }
}
