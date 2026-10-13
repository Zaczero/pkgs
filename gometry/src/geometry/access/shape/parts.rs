use super::{GeometryKind, MultiLineString, MultiMember, MultiPoint, MultiPolygon, Shape};

impl Shape {
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
