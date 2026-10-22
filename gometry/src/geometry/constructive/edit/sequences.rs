use crate::geometry::{
    CoordSeq, GeometryErrorKind, GeometryKind, LineSeq, MultiLineString, MultiMember, MultiPolygon,
    Polygon, Ring, Shape, ShapeCountQuery,
};

impl Shape {
    pub(crate) fn segments(&self) -> crate::error::Result<Vec<Self>> {
        let count = ShapeCountQuery::default().segments(self)?;
        let mut segments = Vec::with_capacity(count);
        self.try_for_each_segment_chain(|coords| {
            for start in 0..coords.len().saturating_sub(1) {
                segments.push(Self::LineString(LineSeq::row_view(
                    coords,
                    crate::geometry::CoordWindow::checked(
                        start..start.wrapping_add(2),
                        coords.len(),
                    )?,
                )));
            }
            Ok::<_, crate::error::Error>(())
        })?;
        Ok(segments)
    }

    /// Rebuild untrusted variable-length callback output through the same
    /// line/ring admission used by geometry constructors.
    pub(crate) fn try_map_coordinate_sequences<E>(
        &self,
        map: &mut impl FnMut(&CoordSeq) -> Result<CoordSeq, E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        let axes = self.axes();
        Ok(match self {
            Self::Point(point) => {
                let coords = map(&CoordSeq::try_from_points(&[*point])?)?;
                match coords.len() {
                    0 => Self::typed_empty(GeometryKind::Point, axes),
                    1 => Self::Point(coords.point_at(0)),
                    _ => {
                        return Err(GeometryErrorKind::message(
                            "a point coordinate sequence must contain zero or one coordinate",
                        )
                        .into());
                    },
                }
            },
            Self::MultiPoint(points) => {
                if points.is_empty() {
                    self.clone()
                } else {
                    let coords = map(points)?;
                    if coords.len() == points.len() {
                        Self::MultiPoint(points.with_coords(coords))
                    } else {
                        Self::multi_point(axes, coords)
                    }
                }
            },
            Self::LineString(line) => {
                let coords = map(line)?;
                if coords.is_empty() {
                    Self::typed_empty(GeometryKind::LineString, axes)
                } else {
                    Self::LineString(LineSeq::try_new(coords)?)
                }
            },
            Self::MultiLineString(lines) => {
                let mut members = Vec::with_capacity(lines.member_count());
                for member in lines.members_with_values() {
                    members.push(match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(line) => {
                            let coords = map(line)?;
                            if coords.is_empty() {
                                MultiMember::Empty
                            } else {
                                MultiMember::Value(LineSeq::try_new(coords)?)
                            }
                        },
                    });
                }
                Self::MultiLineString(MultiLineString::try_new(axes, members)?)
            },
            Self::Polygon(polygon) => map_polygon(polygon, map)?.map_or_else(
                || Self::typed_empty(GeometryKind::Polygon, axes),
                Self::Polygon,
            ),
            Self::MultiPolygon(polygons) => {
                let mut members = Vec::with_capacity(polygons.member_count());
                for member in polygons.members_with_values() {
                    members.push(match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(polygon) => map_polygon(polygon, map)?
                            .map_or(MultiMember::Empty, MultiMember::Value),
                    });
                }
                Self::MultiPolygon(MultiPolygon::try_new(axes, members)?)
            },
            Self::GeometryCollection(parts) => Self::GeometryCollection(
                parts.try_map_members(|part| part.try_map_coordinate_sequences(map))?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }
}

fn map_polygon<E>(
    polygon: &Polygon,
    map: &mut impl FnMut(&CoordSeq) -> Result<CoordSeq, E>,
) -> Result<Option<Polygon>, E>
where
    E: From<crate::error::Error>,
{
    let shell = map(polygon.shell.coords())?;
    let shell = if shell.is_empty() {
        None
    } else {
        Some(Ring::closed_coordseq(shell)?)
    };
    let mut holes = Vec::with_capacity(polygon.holes.len());
    for hole in polygon.holes.iter() {
        let coords = map(hole.coords())?;
        if !coords.is_empty() {
            holes.push(Ring::closed_coordseq(coords)?);
        }
    }
    match shell {
        Some(shell) => Ok(Some(Polygon::new(shell, holes))),
        None if holes.is_empty() => Ok(None),
        None => Err(GeometryErrorKind::message(
            "mapped polygon has nonempty holes but an empty shell",
        )
        .into()),
    }
}
