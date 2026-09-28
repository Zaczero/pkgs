use std::sync::Arc;

use crate::array::{PackedRow, Result};
use crate::geometry::{
    Bounds, CoordinateAxes, GeometryKind, Point, PreparedPlan, Shape, ShapeCountQuery,
};

#[derive(Clone, Copy)]
pub(crate) enum ShapeRow<'a> {
    /// Persistent prepared handle (index boxed rows, scalar geometry).
    Handle(&'a Arc<PreparedPlan>),
    /// Array-owned mixed storage: plain shape; prepared state lives in the
    /// array-side prepared-row cache (see [`PyGeometryArray::prepared_row`]).
    Shape(&'a Shape),
    Point(Point),
    /// Homogeneous packed row with its admitted selector/CSR windows.
    Packed(PackedRow<'a>),
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
            Self::Handle(_) | Self::Shape(_) => None,
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
