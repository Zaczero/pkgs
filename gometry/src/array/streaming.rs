//! Streaming row accumulator for parse-time bulk imports.

use crate::array::{
    CoordSeq, CoordinateAxes, Frame, GeometryKind, Polygon, PolygonLevel, PyGeometryArray, Ring,
    RingLevel, Shape,
};
use crate::error::ErrorKind;
use crate::geometry::types::ensure_coordseq_vertex_capacity;
use crate::geometry::{CoordWindow, CsrOffsetColumn, GeometryErrorKind, LineSeq};

/// Streaming row accumulator for parse-time bulk imports.
///
/// Homogeneous primitive rows of one axes layout pack straight into growing
/// coordinate/CSR columns — no intermediate `Vec<Shape>` container and no
/// second full-payload representation at final packing. A kind/axes change
/// demotes the accumulated columns to zero-copy row views before continuing
/// through the mixed path.
///
/// Missing rows advance only logical offsets. The caller's validity mask owns
/// missingness and point payload remains present-only.
///
/// All growth is fallible ([`try_push`]) so unbounded Python iterators cannot
/// allocator-abort the process — they surface a clean `MemoryError`.
pub(crate) enum StreamingShapes {
    Empty,
    /// Leading nulls before the first present geometry (kind unknown yet).
    LeadingMissing(usize),
    Points(crate::geometry::CoordSeqBuilder),
    Lines(StreamingLines),
    Polygons(StreamingPolygons),
    Shapes {
        shapes: Vec<Shape>,
        row_aligned: bool,
    },
}

pub(crate) struct StreamingLines {
    coords: crate::geometry::CoordSeqBuilder,
    offsets: Vec<usize>,
}

pub(crate) struct StreamingPolygons {
    coords: crate::geometry::CoordSeqBuilder,
    ring_offsets: Vec<usize>,
    polygon_offsets: Vec<usize>,
}

fn implicit_zero_offsets() -> crate::error::Result<Vec<usize>> {
    let mut offsets = Vec::new();
    crate::try_push_vec(&mut offsets, 0)?;
    Ok(offsets)
}

/// Packed CSR columns are `i32`. Overflow is a structural packed-format limit
/// and demotes to mixed shapes; any other error (including Resource) propagates.
fn packed_i32_capacity(len: usize) -> crate::error::Result<bool> {
    match ensure_coordseq_vertex_capacity(len) {
        Ok(()) => Ok(true),
        Err(error) => match error.kind() {
            ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded) => Ok(false),
            _ => Err(error),
        },
    }
}

impl StreamingShapes {
    pub(crate) const fn new() -> Self {
        Self::Empty
    }

    /// Fallibly append one parsed shape. Prefer this over any infallible push
    /// at a Python-iterator boundary.
    pub(crate) fn try_push(&mut self, shape: Shape) -> crate::error::Result<()> {
        // Flush leading missings as kind-preserving placeholders for `shape`.
        if let Self::LeadingMissing(n) = *self {
            *self = Self::Empty;
            self.seed_with_leading_missing(n, &shape)?;
        }
        match (&mut *self, shape) {
            // Hot demoted lane: fallible Vec growth, no enum surgery.
            (Self::Shapes { shapes, .. }, shape) => crate::try_push_vec(shapes, shape),
            (Self::Empty, Shape::Point(point)) => {
                let mut builder = crate::geometry::CoordSeqBuilder::try_with_capacity(
                    CoordinateAxes::from_point(point),
                    0,
                )?;
                builder.push(point)?;
                *self = Self::Points(builder);
                Ok(())
            },
            (Self::Points(builder), Shape::Point(point))
                if builder.axes() == CoordinateAxes::from_point(point) =>
            {
                builder.push(point)?;
                Ok(())
            },
            (Self::Empty, Shape::LineString(line)) => {
                *self = Self::Lines(StreamingLines::try_from_first(&line)?);
                Ok(())
            },
            (Self::Lines(builder), Shape::LineString(line))
                if builder.coords.axes() == line.axes() =>
            {
                builder.try_push(&line)
            },
            (Self::Empty, Shape::Polygon(polygon)) if polygon_stream_axes(&polygon).is_some() => {
                *self = Self::Polygons(StreamingPolygons::try_from_first(&polygon)?);
                Ok(())
            },
            (Self::Polygons(builder), Shape::Polygon(polygon))
                if polygon_stream_axes(&polygon) == Some(builder.coords.axes()) =>
            {
                builder.try_push(&polygon)
            },
            (_, shape) => {
                let shapes = self.try_demote()?;
                crate::try_push_vec(shapes, shape)
            },
        }
    }

    /// Append a missing-row placeholder in final order (kind-preserving when
    /// the stream already has a homogeneous lane). Nulls establish no CRS and
    /// are never parsed by the caller.
    pub(crate) fn try_push_missing(&mut self) -> crate::error::Result<()> {
        match self {
            Self::Empty => {
                *self = Self::LeadingMissing(1);
                Ok(())
            },
            Self::LeadingMissing(n) => {
                *n = n.saturating_add(1);
                Ok(())
            },
            Self::Points(_) => Ok(()),
            Self::Lines(builder) => builder.try_push_missing(),
            Self::Polygons(builder) => builder.try_push_missing(),
            Self::Shapes {
                shapes,
                row_aligned,
            } => {
                if *row_aligned {
                    crate::try_push_vec(shapes, PyGeometryArray::missing_placeholder())?;
                }
                Ok(())
            },
        }
    }

    /// After `LeadingMissing(n)`, open a homogeneous lane (or Shapes) with `n`
    /// kind-preserving placeholders, then the caller pushes `shape`.
    fn seed_with_leading_missing(&mut self, n: usize, shape: &Shape) -> crate::error::Result<()> {
        match shape {
            Shape::Point(point) => {
                let axes = CoordinateAxes::from_point(*point);
                let builder = crate::geometry::CoordSeqBuilder::try_with_capacity(axes, 0)?;
                *self = Self::Points(builder);
            },
            Shape::LineString(line) => {
                let axes = line.axes();
                let mut out = StreamingLines {
                    coords: crate::geometry::CoordSeqBuilder::try_with_capacity(axes, 0)?,
                    offsets: implicit_zero_offsets()?,
                };
                for _ in 0..n {
                    out.try_push_missing()?;
                }
                *self = Self::Lines(out);
            },
            Shape::Polygon(polygon) if polygon_stream_axes(polygon).is_some() => {
                let axes = polygon_stream_axes(polygon).expect("checked");
                let mut out = StreamingPolygons {
                    coords: crate::geometry::CoordSeqBuilder::try_with_capacity(axes, 0)?,
                    ring_offsets: implicit_zero_offsets()?,
                    polygon_offsets: implicit_zero_offsets()?,
                };
                for _ in 0..n {
                    out.try_push_missing()?;
                }
                *self = Self::Polygons(out);
            },
            _ => {
                let mut shapes = Vec::new();
                crate::boundary::try_reserve_vec(&mut shapes, n)?;
                for _ in 0..n {
                    crate::try_push_vec(&mut shapes, PyGeometryArray::missing_placeholder())?;
                }
                *self = Self::Shapes {
                    shapes,
                    row_aligned: true,
                };
            },
        }
        Ok(())
    }

    /// Re-expand any packed points into a plain shape list and switch lanes.
    pub(crate) fn try_demote(&mut self) -> crate::error::Result<&mut Vec<Shape>> {
        match std::mem::replace(self, Self::Empty) {
            Self::Points(builder) => {
                let seq = builder.finish()?;
                let mut shapes = Vec::new();
                crate::boundary::try_reserve_vec(&mut shapes, seq.len())?;
                for row in 0..seq.len() {
                    crate::try_push_vec(&mut shapes, Shape::Point(seq.point_at(row)))?;
                }
                *self = Self::Shapes {
                    shapes,
                    row_aligned: false,
                };
            },
            Self::Lines(builder) => {
                *self = Self::Shapes {
                    shapes: builder.into_shapes()?,
                    row_aligned: true,
                }
            },
            Self::Polygons(builder) => {
                *self = Self::Shapes {
                    shapes: builder.into_shapes()?,
                    row_aligned: true,
                }
            },
            Self::Shapes {
                shapes,
                row_aligned,
            } => {
                *self = Self::Shapes {
                    shapes,
                    row_aligned,
                }
            },
            Self::Empty | Self::LeadingMissing(_) => {
                *self = Self::Shapes {
                    shapes: Vec::new(),
                    row_aligned: false,
                }
            },
        }
        match self {
            Self::Shapes { shapes, .. } => Ok(shapes),
            _ => unreachable!("demote always leaves the Shapes lane"),
        }
    }

    pub(crate) fn finish(self, frame: Frame) -> crate::error::Result<PyGeometryArray> {
        match self {
            Self::Empty => Ok(PyGeometryArray::from_shapes(Vec::new(), frame)?),
            // All-null stream: generic placeholders (no present kind to pack).
            Self::LeadingMissing(n) => {
                let mut shapes = Vec::new();
                crate::boundary::try_reserve_vec(&mut shapes, n)?;
                for _ in 0..n {
                    crate::try_push_vec(&mut shapes, PyGeometryArray::missing_placeholder())?;
                }
                Ok(PyGeometryArray::from_shapes(shapes, frame)?)
            },
            Self::Points(builder) => Ok(PyGeometryArray::packed_points(builder.finish()?, frame)?),
            Self::Lines(builder) => builder.finish(frame),
            Self::Polygons(builder) => builder.finish(frame),
            Self::Shapes { shapes, .. } => Ok(PyGeometryArray::from_shapes(shapes, frame)?),
        }
    }
}

impl StreamingLines {
    fn try_from_first(line: &LineSeq) -> crate::error::Result<Self> {
        let mut out = Self {
            coords: crate::geometry::CoordSeqBuilder::try_with_capacity(line.axes(), line.len())?,
            offsets: implicit_zero_offsets()?,
        };
        out.try_push(line)?;
        Ok(out)
    }

    fn try_push(&mut self, line: &LineSeq) -> crate::error::Result<()> {
        self.coords.extend_window(line, 0..line.len())?;
        crate::try_push_vec(&mut self.offsets, self.coords.len())
    }

    fn try_push_missing(&mut self) -> crate::error::Result<()> {
        crate::try_push_vec(&mut self.offsets, self.coords.len())
    }

    fn into_parts(self) -> crate::error::Result<(CoordSeq, Vec<usize>)> {
        Ok((self.coords.finish()?, self.offsets))
    }

    fn into_shapes(self) -> crate::error::Result<Vec<Shape>> {
        let (coords, offsets) = self.into_parts()?;
        line_shapes_from_parts(&coords, &offsets)
    }

    fn finish(self, frame: Frame) -> crate::error::Result<PyGeometryArray> {
        let (coords, offsets) = self.into_parts()?;
        if packed_i32_capacity(coords.len())? {
            let offsets = CsrOffsetColumn::try_new(offsets, coords.len())?;
            Ok(PyGeometryArray::packed_lines(coords, offsets, frame)?)
        } else {
            Ok(PyGeometryArray::from_shapes(
                line_shapes_from_parts(&coords, &offsets)?,
                frame,
            )?)
        }
    }
}

fn line_shapes_from_parts(
    coords: &CoordSeq,
    offsets: &[usize],
) -> crate::error::Result<Vec<Shape>> {
    let mut shapes = Vec::new();
    crate::boundary::try_reserve_vec(&mut shapes, offsets.len().saturating_sub(1))?;
    for ends in offsets.windows(2) {
        let row = coords.view(CoordWindow::from_range(ends[0]..ends[1], coords.len()));
        let shape = if row.is_empty() {
            Shape::typed_empty(GeometryKind::LineString, coords.axes())
        } else {
            Shape::LineString(LineSeq::from_proven(row))
        };
        crate::try_push_vec(&mut shapes, shape)?;
    }
    Ok(shapes)
}

impl StreamingPolygons {
    fn try_from_first(polygon: &Polygon) -> crate::error::Result<Self> {
        let axes = polygon_stream_axes(polygon)
            .expect("StreamingPolygons requires one axes layout across every ring");
        let mut out = Self {
            coords: crate::geometry::CoordSeqBuilder::try_with_capacity(
                axes,
                polygon.coord_count(),
            )?,
            ring_offsets: implicit_zero_offsets()?,
            polygon_offsets: implicit_zero_offsets()?,
        };
        out.try_push(polygon)?;
        Ok(out)
    }

    fn try_push(&mut self, polygon: &Polygon) -> crate::error::Result<()> {
        for ring in polygon.rings() {
            self.coords.extend_window(ring, 0..ring.len())?;
            crate::try_push_vec(&mut self.ring_offsets, self.coords.len())?;
        }
        crate::try_push_vec(&mut self.polygon_offsets, self.ring_offsets.len() - 1)
    }

    fn try_push_missing(&mut self) -> crate::error::Result<()> {
        crate::try_push_vec(&mut self.polygon_offsets, self.ring_offsets.len() - 1)
    }

    fn into_parts(self) -> crate::error::Result<(CoordSeq, Vec<usize>, Vec<usize>)> {
        Ok((
            self.coords.finish()?,
            self.ring_offsets,
            self.polygon_offsets,
        ))
    }

    fn into_shapes(self) -> crate::error::Result<Vec<Shape>> {
        let (coords, ring_offsets, polygon_offsets) = self.into_parts()?;
        polygon_shapes_from_parts(&coords, &ring_offsets, &polygon_offsets)
    }

    fn finish(self, frame: Frame) -> crate::error::Result<PyGeometryArray> {
        let (coords, ring_ends, polygon_ends) = self.into_parts()?;
        let ring_count = ring_ends.len().saturating_sub(1);
        if packed_i32_capacity(coords.len())? && packed_i32_capacity(ring_count)? {
            let ring_offsets = CsrOffsetColumn::<RingLevel>::try_new(ring_ends, coords.len())?;
            let polygon_offsets =
                CsrOffsetColumn::<PolygonLevel>::try_new(polygon_ends, ring_count)?;
            Ok(PyGeometryArray::packed_polygons(
                coords,
                ring_offsets,
                polygon_offsets,
                frame,
            )?)
        } else {
            Ok(PyGeometryArray::from_shapes(
                polygon_shapes_from_parts(&coords, &ring_ends, &polygon_ends)?,
                frame,
            )?)
        }
    }
}

/// Streaming pack gate: shared structural admission (shell present, every
/// ring ≥4 XY-closed verts, uniform axes). Axes-only checks used to pack
/// empty/short/unclosed WKB rings into trusted storage (public panic on area).
fn polygon_stream_axes(polygon: &Polygon) -> Option<CoordinateAxes> {
    super::polygon_pack_axes(polygon)
}

fn polygon_shapes_from_parts(
    coords: &CoordSeq,
    ring_offsets: &[usize],
    polygon_offsets: &[usize],
) -> crate::error::Result<Vec<Shape>> {
    let mut shapes = Vec::new();
    crate::boundary::try_reserve_vec(&mut shapes, polygon_offsets.len().saturating_sub(1))?;
    for polygon_ends in polygon_offsets.windows(2) {
        if polygon_ends[0] == polygon_ends[1] {
            crate::try_push_vec(
                &mut shapes,
                Shape::typed_empty(GeometryKind::Polygon, coords.axes()),
            )?;
            continue;
        }
        let mut rings = (polygon_ends[0]..polygon_ends[1]).map(|ring| {
            let row = coords.view(CoordWindow::from_range(
                ring_offsets[ring]..ring_offsets[ring + 1],
                coords.len(),
            ));
            Ring::from_closed(row)
        });
        let shell = rings
            .next()
            .expect("streamed Polygon always contains a shell");
        let mut holes = Vec::new();
        for ring in rings {
            crate::try_push_vec(&mut holes, ring)?;
        }
        crate::try_push_vec(&mut shapes, Shape::Polygon(Polygon::new(shell, holes)?))?;
    }
    Ok(shapes)
}

#[cfg(test)]
mod demotion_tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::{CoordSeq, LineSeq, Point, Ring};
    use crate::test_support::fail_allocations_after;

    fn xy_line(points: &[(f64, f64)]) -> Shape {
        let coords = CoordSeq::try_from_points(
            &points
                .iter()
                .map(|&(x, y)| Point::new_raw_xy(x, y))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        Shape::LineString(LineSeq::try_new(coords).unwrap())
    }

    fn xy_square() -> Shape {
        let coords = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 1.0, 0.0, 0.0],
            vec![0.0, 0.0, 1.0, 1.0, 0.0],
            None,
            None,
        )
        .unwrap();
        Shape::Polygon(Polygon::new(Ring::from_closed(coords), Vec::new()).unwrap())
    }

    fn assert_demote_returns_resource_then_reuses(mut stream: StreamingShapes, reuse: &Shape) {
        let error = fail_allocations_after(0, || stream.try_demote()).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        stream.try_push(reuse.clone()).unwrap();
        let array = stream.finish(Frame::None).unwrap();
        assert_eq!(array.len(), 1);
        assert!(!array.is_row_missing(0));
        assert_eq!(array.geometry_at(0).unwrap().shape.shape(), reuse);
    }

    #[test]
    fn line_try_demote_returns_resource_on_allocation_failure() {
        let line = xy_line(&[(0.0, 0.0), (1.0, 1.0)]);
        let mut stream = StreamingShapes::new();
        stream.try_push(line).unwrap();
        assert_demote_returns_resource_then_reuses(stream, &xy_line(&[(2.0, 3.0), (4.0, 5.0)]));
    }

    #[test]
    fn polygon_try_demote_returns_resource_on_allocation_failure() {
        let mut stream = StreamingShapes::new();
        stream.try_push(xy_square()).unwrap();
        assert_demote_returns_resource_then_reuses(stream, &xy_square());
    }

    #[test]
    fn mixed_kind_demotion_preserves_rows_axes_and_missing_order() {
        let first = xy_line(&[(0.0, 0.0), (1.0, 0.0)]);
        let second = xy_line(&[(2.0, 2.0), (3.0, 3.0)]);
        let point = Shape::Point(Point::new_raw_xy(9.0, 8.0));
        let mut stream = StreamingShapes::new();
        stream.try_push(first.clone()).unwrap();
        stream.try_push_missing().unwrap();
        stream.try_push(second.clone()).unwrap();
        stream.try_push(point.clone()).unwrap();
        let array = stream.finish(Frame::None).unwrap();
        assert_eq!(array.len(), 4);
        assert!(!array.is_row_missing(0));
        assert!(!array.is_row_missing(1));
        assert!(!array.is_row_missing(2));
        assert!(!array.is_row_missing(3));
        assert_eq!(array.geometry_at(0).unwrap().shape.shape(), &first);
        assert_eq!(
            array.geometry_at(1).unwrap().shape.shape(),
            &Shape::typed_empty(GeometryKind::LineString, CoordinateAxes::XY)
        );
        assert_eq!(array.geometry_at(2).unwrap().shape.shape(), &second);
        assert_eq!(array.geometry_at(3).unwrap().shape.shape(), &point);
        assert_eq!(
            array.geometry_at(0).unwrap().shape.shape().axes(),
            CoordinateAxes::XY
        );
        assert_eq!(
            array.geometry_at(3).unwrap().shape.shape().axes(),
            CoordinateAxes::XY
        );
    }
}
