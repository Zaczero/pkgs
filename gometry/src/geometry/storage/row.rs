use std::sync::Arc;

use super::PackedRow;
use crate::error::Result;
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, Coordinates, GeometryKind, Point, PreparedPlan, Shape,
    ShapeCountQuery,
};

pub(crate) struct CoordRow<'a> {
    coords: &'a CoordSeq,
    window: std::ops::Range<usize>,
}

impl<'a> CoordRow<'a> {
    pub(crate) const fn new(coords: &'a CoordSeq, window: std::ops::Range<usize>) -> Self {
        Self { coords, window }
    }

    fn columns(&self) -> (&'a [f64], &'a [f64]) {
        (
            self.coords
                .xs()
                .get(self.window.clone())
                .expect("admitted row x window"),
            self.coords
                .ys()
                .get(self.window.clone())
                .expect("admitted row y window"),
        )
    }
}

impl Coordinates for CoordRow<'_> {
    fn coord_count(&self) -> usize {
        self.window.len()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self.coords.point_at(
            self.window
                .clone()
                .nth(index)
                .expect("coordinate index lies in its row"),
        )
    }

    fn xy_columns(&self) -> Option<(&[f64], &[f64])> {
        Some(self.columns())
    }

    fn z_column(&self) -> Option<&[f64]> {
        self.coords.zs().map(|values| {
            values
                .get(self.window.clone())
                .expect("admitted row z window")
        })
    }

    fn m_column(&self) -> Option<&[f64]> {
        self.coords.ms().map(|values| {
            values
                .get(self.window.clone())
                .expect("admitted row m window")
        })
    }
}

pub(crate) enum PolygonRings<'a> {
    Shape(
        std::iter::Chain<
            std::iter::Once<&'a crate::geometry::Ring>,
            std::slice::Iter<'a, crate::geometry::Ring>,
        >,
    ),
    Packed {
        row: PackedRow<'a>,
        next: usize,
    },
}

impl<'a> Iterator for PolygonRings<'a> {
    type Item = CoordRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Shape(rings) => rings
                .next()
                .map(|ring| CoordRow::new(ring.coords(), 0..ring.len())),
            Self::Packed { row, next } => {
                let PackedRow::Polygons { coords, .. } = *row else {
                    unreachable!("polygon ring cursor contains polygon storage")
                };
                let window = row.polygon_ring_window(*next)?;
                *next = next.wrapping_add(1);
                Some(CoordRow::new(coords, window))
            },
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ShapeRow<'a> {
    /// Persistent prepared handle (index boxed rows, scalar geometry).
    Handle(&'a Arc<PreparedPlan>),
    /// Array-owned mixed storage: plain shape; prepared state lives in the
    /// array-side prepared-row cache (see `PyGeometryArray::prepared_row`).
    Shape(&'a Shape),
    Point(Point),
    /// Homogeneous packed row with its admitted selector/CSR windows.
    Packed(PackedRow<'a>),
}

impl<'a> From<&'a Shape> for ShapeRow<'a> {
    fn from(shape: &'a Shape) -> Self {
        Self::Shape(shape)
    }
}

impl<'a> ShapeRow<'a> {
    pub(crate) fn from_packed(row: PackedRow<'a>) -> Self {
        row.point_value().map_or(Self::Packed(row), Self::Point)
    }

    pub(crate) fn kind(self) -> GeometryKind {
        match self {
            Self::Handle(handle) => GeometryKind::of(handle.shape()),
            Self::Shape(shape) => GeometryKind::of(shape),
            Self::Point(_) => GeometryKind::Point,
            Self::Packed(row) => row.kind(),
        }
    }

    pub(crate) fn axes(self) -> CoordinateAxes {
        match self {
            Self::Handle(handle) => handle.shape().axes(),
            Self::Shape(shape) => shape.axes(),
            Self::Point(point) => CoordinateAxes::from_point(point),
            Self::Packed(row) => row.axes(),
        }
    }

    pub(crate) fn point(self) -> Option<Point> {
        match self {
            Self::Point(point) => Some(point),
            Self::Packed(row) => row.point_value(),
            Self::Handle(handle) => Self::Shape(handle.shape()).point(),
            Self::Shape(Shape::Point(point)) => Some(*point),
            Self::Shape(
                Shape::MultiPoint(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..),
            ) => None,
        }
    }

    pub(crate) fn coordinates(self, counts: &mut ShapeCountQuery<'a>) -> Result<usize> {
        match self {
            Self::Handle(handle) => counts.coordinates(handle.shape()),
            Self::Shape(shape) => counts.coordinates(shape),
            Self::Point(_) => Ok(1),
            Self::Packed(row) => Ok(row.coord_count()),
        }
    }

    pub(crate) fn is_empty(self) -> bool {
        match self {
            Self::Handle(handle) => handle.shape().is_empty(),
            Self::Shape(shape) => shape.is_empty(),
            Self::Point(_) => false,
            Self::Packed(row) => row.is_empty(),
        }
    }

    pub(crate) fn has_area_parts(self) -> bool {
        match self {
            Self::Handle(plan) => plan.shape().has_area_parts(),
            Self::Shape(shape) => shape.has_area_parts(),
            Self::Point(_) => false,
            Self::Packed(row) => row.kind() == GeometryKind::Polygon && !row.is_empty(),
        }
    }

    pub(crate) fn line_columns(self) -> Option<(&'a [f64], &'a [f64])> {
        self.line_coords().map(|coords| coords.columns())
    }

    pub(crate) fn line_coords(self) -> Option<CoordRow<'a>> {
        match self {
            Self::Handle(plan) => Self::Shape(plan.shape()).line_coords(),
            Self::Shape(Shape::LineString(line)) => {
                Some(CoordRow::new(line.as_coords(), 0..line.len()))
            },
            Self::Shape(Shape::MultiLineString(lines)) => match &**lines {
                [line] => Some(CoordRow::new(line.as_coords(), 0..line.len())),
                _ => None,
            },
            Self::Packed(row @ PackedRow::Lines { coords, .. }) => Some(CoordRow::new(
                coords,
                row.vertex_window().expect("line row has a window"),
            )),
            Self::Point(_)
            | Self::Packed(PackedRow::Points { .. } | PackedRow::Polygons { .. })
            | Self::Shape(
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..),
            ) => None,
        }
    }

    pub(crate) fn polygon_rings(self) -> Option<PolygonRings<'a>> {
        match self {
            Self::Handle(plan) => Self::Shape(plan.shape()).polygon_rings(),
            Self::Shape(Shape::Polygon(polygon)) => Some(PolygonRings::Shape(
                std::iter::once(&polygon.shell).chain(polygon.holes.iter()),
            )),
            Self::Packed(row @ PackedRow::Polygons { rings_start, .. }) => {
                Some(PolygonRings::Packed {
                    row,
                    next: rings_start,
                })
            },
            Self::Point(_)
            | Self::Packed(PackedRow::Points { .. } | PackedRow::Lines { .. })
            | Self::Shape(
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..),
            ) => None,
        }
    }

    pub(crate) fn same_stored_geometry(self, other: Self) -> bool {
        if let Self::Handle(plan) = self {
            return Self::Shape(plan.shape()).same_stored_geometry(other);
        }
        if let Self::Handle(plan) = other {
            return self.same_stored_geometry(Self::Shape(plan.shape()));
        }
        if let (Self::Shape(left), Self::Shape(right)) = (self, other) {
            return left == right;
        }
        if self.kind() != other.kind() || self.axes() != other.axes() {
            return false;
        }
        if self.is_empty() || other.is_empty() {
            return self.is_empty() && other.is_empty();
        }
        if let (Some(left), Some(right)) = (self.point(), other.point()) {
            return left == right;
        }
        if let (Some(left), Some(right)) = (self.line_coords(), other.line_coords()) {
            return crate::geometry::coordinate_columns_eq(&left, &right);
        }
        if let (Some(mut left), Some(mut right)) = (self.polygon_rings(), other.polygon_rings()) {
            loop {
                match (left.next(), right.next()) {
                    (Some(left), Some(right)) => {
                        if !crate::geometry::coordinate_columns_eq(&left, &right) {
                            return false;
                        }
                    },
                    (None, None) => return true,
                    _ => return false,
                }
            }
        }
        false
    }

    pub(crate) fn segment_count_capacity_hint(self) -> Option<usize> {
        match self {
            Self::Handle(plan) => plan
                .shape()
                .segment_count_capacity_hint(crate::geometry::RunIdentity::XY),
            Self::Shape(shape) => {
                shape.segment_count_capacity_hint(crate::geometry::RunIdentity::XY)
            },
            Self::Point(_) => Some(0),
            Self::Packed(row) => Some(row.coord_count()),
        }
    }

    pub(crate) fn segment_count_lower_bound(self) -> usize {
        match self {
            Self::Packed(row @ PackedRow::Lines { .. }) => row.coord_count().saturating_sub(1),
            Self::Packed(row @ PackedRow::Polygons { .. }) => row
                .coord_count()
                .saturating_sub(row.polygon_rings().expect("polygon row has rings").len()),
            Self::Handle(_)
            | Self::Shape(_)
            | Self::Point(_)
            | Self::Packed(PackedRow::Points { .. }) => 0,
        }
    }

    pub(crate) fn try_for_each_segment(
        self,
        mut visit: impl FnMut(crate::geometry::Segment) -> Result<()>,
    ) -> Result<()> {
        if let Some((xs, ys)) = self.line_columns() {
            for ([ax, bx], [ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
                visit(crate::geometry::Segment {
                    start: crate::geometry::XY::new(*ax, *ay),
                    end: crate::geometry::XY::new(*bx, *by),
                })?;
            }
            return Ok(());
        }
        match self {
            Self::Handle(plan) => Self::Shape(plan.shape()).try_for_each_segment(visit),
            Self::Shape(shape) => {
                shape.try_for_each_first_segment_chain(crate::geometry::RunIdentity::XY, |chain| {
                    for [start, end] in chain.segment_pairs() {
                        visit(crate::geometry::Segment {
                            start: start.xy(),
                            end: end.xy(),
                        })?;
                    }
                    Ok(())
                })
            },
            Self::Point(_) | Self::Packed(PackedRow::Points { .. }) => Ok(()),
            Self::Packed(PackedRow::Lines { .. } | PackedRow::Polygons { .. }) => {
                unreachable!("packed lines use columns and polygons use areal support")
            },
        }
    }

    pub(crate) fn for_each_point_only(self, mut visit: impl FnMut(Point)) {
        if let Some((xs, ys)) = self.line_columns() {
            let first = xs.first().zip(ys.first()).map(|(&x, &y)| (x, y));
            if first.is_some_and(|first| xs.iter().zip(ys).all(|(&x, &y)| (x, y) == first)) {
                for (&x, &y) in xs.iter().zip(ys) {
                    visit(Point::new_raw_xy(x, y));
                }
            }
            return;
        }
        match self {
            Self::Handle(plan) => Self::Shape(plan.shape()).for_each_point_only(visit),
            Self::Shape(shape) => {
                shape.for_each_first_point_only(crate::geometry::RunIdentity::XY, visit);
            },
            Self::Point(point) => visit(point),
            Self::Packed(PackedRow::Points {
                coords, physical, ..
            }) => visit(coords.point_at(physical)),
            Self::Packed(PackedRow::Lines { .. } | PackedRow::Polygons { .. }) => {
                unreachable!("packed lines use columns and polygons use areal support")
            },
        }
    }

    pub(crate) fn to_shape(self) -> Result<Shape> {
        match self {
            Self::Handle(handle) => Ok(handle.shape().clone()),
            Self::Shape(shape) => Ok(shape.clone()),
            Self::Point(point) => Ok(Shape::Point(point)),
            Self::Packed(row) => row.to_shape(),
        }
    }

    pub(crate) fn with_shape<R>(self, f: impl FnOnce(&Shape) -> R) -> Result<R> {
        match self {
            Self::Handle(handle) => Ok(f(handle.shape())),
            Self::Shape(shape) => Ok(f(shape)),
            Self::Point(point) => Ok(f(&Shape::Point(point))),
            Self::Packed(row) => {
                let shape = row.to_shape()?;
                Ok(f(&shape))
            },
        }
    }

    pub(crate) fn try_with_shape<R, E>(self, f: impl FnOnce(&Shape) -> Result<R, E>) -> Result<R, E>
    where
        E: From<crate::error::Error>,
    {
        self.with_shape(f)
            .map_err(E::from)
            .and_then(|result| result)
    }

    pub(crate) fn quick_bounds(self) -> Option<Bounds> {
        match self {
            Self::Handle(handle) => handle.bounds(),
            Self::Shape(shape) => shape.bounds(),
            Self::Point(point) => {
                (point.x.is_finite() && point.y.is_finite()).then_some(Bounds::from_point(point))
            },
            Self::Packed(row) => row.quick_bounds(),
        }
    }
}
