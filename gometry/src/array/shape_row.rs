use std::sync::Arc;

use crate::array::{GeometryArrayStorage, polygon_row_point_membership, row_bounds};
use crate::geometry::{Bounds, CoordSeq, CoordWindow, Dimension, LineSeq, Point, Shape, ShapeData};

#[derive(Clone, Copy)]
pub(crate) struct VertexWindow {
    pub start: usize,
    pub end: usize,
}

#[derive(Clone, Copy)]
pub enum ShapeRow<'a> {
    /// Persistent prepared handle (index boxed rows, scalar geometry).
    Handle(&'a Arc<ShapeData>),
    /// Array-owned mixed storage: plain shape; prepared state lives in the
    /// array-side prepared-row cache (see [`PyGeometryArray::prepared_row`]).
    Shape(&'a Shape),
    Point(Point),
    Line(&'a CoordSeq, usize, usize),
    Rings(&'a CoordSeq, &'a [i32], usize, usize),
}

impl ShapeRow<'_> {
    fn line_shape(coords: &CoordSeq, start: usize, end: usize) -> Shape {
        let window = CoordWindow::trusted(start..end, coords.len());
        Shape::LineString(LineSeq::from_trusted(coords.view(window)))
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the callback is invoked once and never named or stored, so a generic identifier adds no meaning"
    )]
    pub fn with_shape<R>(self, f: impl FnOnce(&Shape) -> R) -> R {
        match self {
            Self::Handle(handle) => f(handle.shape()),
            Self::Shape(shape) => f(shape),
            Self::Point(point) => f(&Shape::Point(point)),
            Self::Line(coords, start, end) => f(&Self::line_shape(coords, start, end)),
            Self::Rings(coords, ring_offsets, start, end) => f(&Shape::Polygon(
                GeometryArrayStorage::polygon_view(coords, ring_offsets, start..end),
            )),
        }
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
            Self::Rings(coords, ring_offsets, start, _) => {
                // Holes lie inside the shell: the shell window IS the bounds.
                let shell = ring_offsets[start] as usize..ring_offsets[start + 1] as usize;
                let window = VertexWindow {
                    start: shell.start,
                    end: shell.end,
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

    pub fn contains_point(self, point: Point) -> bool {
        self.point_membership::<false>(point)
    }

    pub fn covers_point(self, point: Point) -> bool {
        self.point_membership::<true>(point)
    }

    fn point_membership<const BOUNDARY: bool>(self, point: Point) -> bool {
        match self {
            Self::Rings(coords, ring_offsets, start, end) => {
                polygon_row_point_membership::<BOUNDARY>(coords, ring_offsets, start..end, point)
            },
            _ => self.with_shape(|shape| {
                if BOUNDARY {
                    shape.covers_point(point)
                } else {
                    shape.contains_point(point)
                }
            }),
        }
    }
}
