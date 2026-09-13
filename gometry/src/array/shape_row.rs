use std::sync::Arc;

use crate::array::{GeometryArrayStorage, polygon_row_point_membership, row_bounds};
use crate::error::Result;
use crate::geometry::{
    Bounds, CoordSeq, CoordWindow, Dimension, LineSeq, Point, PreparedPlan, Shape,
};

#[derive(Clone, Copy)]
pub(crate) struct VertexWindow {
    pub start: usize,
    pub end: usize,
}

#[derive(Clone, Copy)]
pub enum ShapeRow<'a> {
    /// Persistent prepared handle (index boxed rows, scalar geometry).
    Handle(&'a Arc<PreparedPlan>),
    /// Array-owned mixed storage: plain shape; prepared state lives in the
    /// array-side prepared-row cache (see [`PyGeometryArray::prepared_row`]).
    Shape(&'a Shape),
    Point(Point),
    Line(&'a CoordSeq, usize, usize),
    Rings(&'a CoordSeq, &'a [i32], usize, usize),
}

impl<'a> ShapeRow<'a> {
    pub(crate) fn line(coords: &'a CoordSeq, window: CoordWindow) -> Self {
        Self::Line(coords, window.start(), window.end())
    }

    pub(crate) fn rings(
        coords: &'a CoordSeq,
        ring_offsets: &'a [i32],
        rings: std::ops::Range<usize>,
    ) -> Self {
        assert!(
            rings.start <= rings.end && rings.end < ring_offsets.len(),
            "polygon row must reference admitted ring offsets"
        );
        Self::Rings(coords, ring_offsets, rings.start, rings.end)
    }

    fn line_shape(coords: &CoordSeq, start: usize, end: usize) -> Shape {
        let window = CoordWindow::from_range(start..end, coords.len());
        Shape::LineString(LineSeq::row_view(coords, window))
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the callback is invoked once and never named or stored, so a generic identifier adds no meaning"
    )]
    pub fn with_shape<R>(self, f: impl FnOnce(&Shape) -> R) -> crate::error::Result<R> {
        match self {
            Self::Handle(handle) => Ok(f(handle.shape())),
            Self::Shape(shape) => Ok(f(shape)),
            Self::Point(point) => Ok(f(&Shape::Point(point))),
            Self::Line(coords, start, end) => Ok(f(&Self::line_shape(coords, start, end))),
            Self::Rings(coords, ring_offsets, start, end) => {
                let polygon = GeometryArrayStorage::polygon_view(coords, ring_offsets, start..end)?;
                Ok(f(&Shape::Polygon(polygon)))
            },
        }
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the callback is invoked once and never named or stored, so a generic identifier adds no meaning"
    )]
    pub fn try_with_shape<R, E>(self, f: impl FnOnce(&Shape) -> Result<R, E>) -> Result<R, E>
    where
        E: From<crate::error::Error>,
    {
        self.with_shape(f)
            .map_err(E::from)
            .and_then(|result| result)
    }

    pub fn quick_bounds(self) -> Option<Bounds> {
        match self {
            Self::Handle(handle) => handle.bounds(),
            Self::Shape(shape) => shape.bounds(),
            Self::Point(point) => Some(Bounds::from_point(point)),
            Self::Line(coords, start, end) => {
                let window = VertexWindow { start, end };
                let (xs, ys) = (
                    &coords.xs()[window.start..window.end],
                    &coords.ys()[window.start..window.end],
                );
                (!xs.is_empty()).then(|| row_bounds(xs, ys))
            },
            Self::Rings(coords, ring_offsets, start, end) => {
                let window = VertexWindow {
                    start: ring_offsets[start] as usize,
                    end: ring_offsets[end] as usize,
                };
                ShapeRow::Line(coords, window.start, window.end).quick_bounds()
            },
        }
    }
    /// This row's topological dimension — `0` point, `1` curve, `2` areal.
    pub fn topological_dimension(self) -> Dimension {
        match self {
            Self::Handle(handle) => handle.shape().topological_dimension(),
            Self::Shape(shape) => shape.topological_dimension(),
            Self::Point(_) => Dimension::Point,
            Self::Line(..) => Dimension::Curve,
            Self::Rings(..) => Dimension::Surface,
        }
    }

    pub fn contains_point(self, point: Point) -> crate::error::Result<bool> {
        self.point_membership::<false>(point)
    }

    pub fn covers_point(self, point: Point) -> crate::error::Result<bool> {
        self.point_membership::<true>(point)
    }

    fn point_membership<const BOUNDARY: bool>(self, point: Point) -> crate::error::Result<bool> {
        match self {
            Self::Rings(coords, ring_offsets, start, end) => {
                Ok(polygon_row_point_membership::<BOUNDARY>(
                    coords,
                    ring_offsets,
                    start..end,
                    point,
                ))
            },
            _ => self.try_with_shape(|shape| {
                if BOUNDARY {
                    shape.covers_point(point)
                } else {
                    shape.contains_point(point)
                }
            }),
        }
    }
}
