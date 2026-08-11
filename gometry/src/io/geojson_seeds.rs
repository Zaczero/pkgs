//! `DeserializeSeed` machinery that parses GeoJSON coordinate arrays
//! straight into SoA columns — no `serde_json::Value` tree, no AoS
//! staging. Child module of [`super`] (the GeoJSON reader/writer).

use crate::io::geojson::{
    CoordSeq, CoordSeqBuilder, CoordinateAxes, CoordinateNumberSeed, DeserializeSeed, Deserializer,
    IgnoredAny, IoGeometryKind, LineSeq, MOrdinate, Point, Polygon, Ring, SeqAccess, Shape,
    Visitor, ZOrdinate, de, fmt, geojson_domain_violation, geojson_mixed_axes_message,
};

pub(super) fn read_position<'de, A>(
    seq: &mut A,
    allow_empty: bool,
) -> std::result::Result<Option<Point>, A::Error>
where
    A: SeqAccess<'de>,
{
    let Some(x) = seq.next_element_seed(CoordinateNumberSeed("x coordinate must be numeric"))?
    else {
        return if allow_empty {
            Ok(None)
        } else {
            Err(de::Error::custom("coordinate requires x and y"))
        };
    };
    let y = seq
        .next_element_seed(CoordinateNumberSeed("y coordinate must be numeric"))?
        .ok_or_else(|| de::Error::custom("coordinate requires x and y"))?;
    let z = seq.next_element_seed(CoordinateNumberSeed("z coordinate must be numeric"))?;
    if seq.next_element::<IgnoredAny>()?.is_some() {
        return Err(de::Error::custom(
            "GeoJSON coordinates support XY or XYZ only; M coordinates are not supported",
        ));
    }
    if let Some((label, value)) = geojson_domain_violation(x, y) {
        return Err(de::Error::custom(format!(
            "GeoJSON {label} {value} is outside the WGS84 domain (longitude [-180, 180], latitude [-90, 90])"
        )));
    }
    Point::new_axes(x, y, ZOrdinate(z), MOrdinate(None))
        .map(Some)
        .map_err(de::Error::custom)
}

pub(super) struct PositionSeed;

impl<'de> DeserializeSeed<'de> for PositionSeed {
    type Value = Point;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(PositionVisitor)
    }
}

pub(super) struct PositionVisitor;

impl<'de> Visitor<'de> for PositionVisitor {
    type Value = Point;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("coordinate must be an array")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        read_position(&mut seq, false).map(|point| point.expect("non-empty position required"))
    }
}

pub(super) struct PointCoordinatesSeed;

impl<'de> DeserializeSeed<'de> for PointCoordinatesSeed {
    type Value = Option<Point>;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(PointCoordinatesVisitor)
    }
}

pub(super) struct PointCoordinatesVisitor;

impl<'de> Visitor<'de> for PointCoordinatesVisitor {
    type Value = Option<Point>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("coordinate must be an array")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        read_position(&mut seq, true)
    }
}

pub(super) struct PointsSeed;

impl<'de> DeserializeSeed<'de> for PointsSeed {
    type Value = CoordSeq;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(PointsVisitor)
    }
}

pub(super) struct PointsVisitor;

impl<'de> Visitor<'de> for PointsVisitor {
    type Value = CoordSeq;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("coordinates must be an array")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let Some(first) = seq.next_element_seed(PositionSeed)? else {
            return Ok(CoordSeq::empty(CoordinateAxes::XY));
        };
        let axes = CoordinateAxes::from_point(first);
        let mut points = CoordSeqBuilder::with_capacity(axes, seq.size_hint().unwrap_or(0) + 1);
        points.push(first);
        while let Some(point) = seq.next_element_seed(PositionSeed)? {
            // Uniform axes per sequence: a CoordSeq is one set of columns, so
            // every position shares the same Z presence. RFC 7946 makes the
            // third ordinate optional per position, but gometry rejects
            // non-finite coordinates (NaN is not an absent-Z sentinel here) and
            // filling 0 would invent elevation — keep a clean typed reject.
            if CoordinateAxes::from_point(point) != axes {
                return Err(de::Error::custom(geojson_mixed_axes_message()));
            }
            points.push(point);
        }
        points.finish().map_err(de::Error::custom)
    }
}

pub(super) struct LineSeed;

impl<'de> DeserializeSeed<'de> for LineSeed {
    type Value = LineSeq;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        LineSeq::try_new(PointsSeed.deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

pub(super) struct LinesSeed;

impl<'de> DeserializeSeed<'de> for LinesSeed {
    type Value = Vec<LineSeq>;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(LinesVisitor)
    }
}

pub(super) struct LinesVisitor;

impl<'de> Visitor<'de> for LinesVisitor {
    type Value = Vec<LineSeq>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("coordinates must be an array")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut lines = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(line) = seq.next_element_seed(LineSeed)? {
            lines.push(line);
        }
        Ok(lines)
    }
}

pub(super) struct RingSeed;

impl<'de> DeserializeSeed<'de> for RingSeed {
    type Value = Ring;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let points = PointsSeed.deserialize(deserializer)?;
        if points.len() < Ring::MIN_VERTICES_CLOSED {
            return Err(de::Error::custom(format!(
                "Polygon ring requires at least {} coordinates",
                Ring::MIN_VERTICES_CLOSED
            )));
        }
        let first = points.point_at(0);
        let last = points.point_at(points.len() - 1);
        // RFC 7946 §3.1.6: first and last position values must be identical
        // on every active ordinate (not just XY).
        if !crate::geometry::same_active_position(first, last) {
            return Err(de::Error::custom("Polygon ring must be explicitly closed"));
        }
        Ok(Ring::from_trusted_closed(points))
    }
}

pub(super) struct PolygonSeed {
    allow_empty: bool,
}

impl<'de> DeserializeSeed<'de> for PolygonSeed {
    type Value = Option<Polygon>;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(PolygonVisitor {
            allow_empty: self.allow_empty,
        })
    }
}

pub(super) struct PolygonVisitor {
    allow_empty: bool,
}

impl<'de> Visitor<'de> for PolygonVisitor {
    type Value = Option<Polygon>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Polygon coordinates must be an array")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let Some(shell) = seq.next_element_seed(RingSeed)? else {
            return if self.allow_empty {
                Ok(None)
            } else {
                Err(de::Error::custom("Polygon requires a shell"))
            };
        };
        let mut holes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(hole) = seq.next_element_seed(RingSeed)? {
            holes.push(hole);
        }
        Ok(Some(Polygon::new(shell, holes)))
    }
}

pub(super) struct PolygonsSeed;

impl<'de> DeserializeSeed<'de> for PolygonsSeed {
    type Value = Vec<Polygon>;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(PolygonsVisitor)
    }
}

pub(super) struct PolygonsVisitor;

impl<'de> Visitor<'de> for PolygonsVisitor {
    type Value = Vec<Polygon>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MultiPolygon coordinates must be an array")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut polygons = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(polygon) = seq.next_element_seed(PolygonSeed { allow_empty: false })? {
            polygons.push(polygon.expect("non-empty polygon seed always returns a polygon"));
        }
        Ok(polygons)
    }
}

pub(super) struct ShapeCoordinatesSeed(pub(super) IoGeometryKind);

impl<'de> DeserializeSeed<'de> for ShapeCoordinatesSeed {
    type Value = Shape;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        match self.0 {
            IoGeometryKind::Point => PointCoordinatesSeed
                .deserialize(deserializer)
                .map(|point| point.map_or_else(Shape::empty_point, Shape::Point)),
            IoGeometryKind::MultiPoint => {
                PointsSeed.deserialize(deserializer).map(Shape::MultiPoint)
            },
            IoGeometryKind::LineString => LineSeed.deserialize(deserializer).map(Shape::LineString),
            IoGeometryKind::MultiLineString => LinesSeed
                .deserialize(deserializer)
                .map(Shape::MultiLineString),
            IoGeometryKind::Polygon => PolygonSeed { allow_empty: true }
                .deserialize(deserializer)
                .map(|polygon| polygon.map_or_else(Shape::empty_polygon, Shape::Polygon)),
            IoGeometryKind::MultiPolygon => PolygonsSeed
                .deserialize(deserializer)
                .map(Shape::MultiPolygon),
            IoGeometryKind::GeometryCollection => {
                unreachable!("geometry collections do not carry coordinates")
            },
        }
    }
}
