//! Streaming row accumulator for parse-time bulk imports.

use pyo3::exceptions::PyMemoryError;
use pyo3::{PyErr, PyResult};

use super::*;
use crate::geometry::LineSeq;

/// Streaming row accumulator for parse-time bulk imports.
///
/// Homogeneous primitive rows of one axes layout pack straight into growing
/// coordinate/CSR columns — no intermediate `Vec<Shape>` container and no
/// second full-payload representation at final packing. A kind/axes change
/// demotes the accumulated columns to zero-copy row views before continuing
/// through the mixed path.
///
/// All growth is fallible ([`try_push`]) so unbounded Python iterators cannot
/// allocator-abort the process — they surface a clean `MemoryError`.
pub(crate) enum StreamingShapes {
    Empty,
    Points(crate::geometry::CoordSeqBuilder),
    Lines(StreamingLines),
    Polygons(StreamingPolygons),
    Shapes(Vec<Shape>),
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

fn grow_err() -> PyErr {
    PyMemoryError::new_err("failed to grow streaming geometry buffer from untrusted input")
}

/// Fallibly grow a `CoordSeqBuilder` column set by one slot when full.
fn try_grow_coord_builder(builder: &mut crate::geometry::CoordSeqBuilder) -> PyResult<()> {
    if builder.len() < builder.capacity_slots() {
        return Ok(());
    }
    let additional = builder.capacity_slots().max(8);
    builder
        .try_reserve_exact(additional)
        .map_err(|_| grow_err())
}

impl StreamingShapes {
    pub(crate) const fn new() -> Self {
        Self::Empty
    }

    /// Fallibly append one parsed shape. Prefer this over any infallible push
    /// at a Python-iterator boundary.
    pub(crate) fn try_push(&mut self, shape: Shape) -> PyResult<()> {
        match (&mut *self, shape) {
            // Hot demoted lane: fallible Vec growth, no enum surgery.
            (Self::Shapes(shapes), shape) => crate::try_push(shapes, shape),
            (Self::Empty, Shape::Point(point)) => {
                let mut builder = crate::geometry::CoordSeqBuilder::with_capacity(
                    CoordinateAxes::from_point(point),
                    0,
                );
                try_grow_coord_builder(&mut builder)?;
                builder.push(point);
                *self = Self::Points(builder);
                Ok(())
            },
            (Self::Points(builder), Shape::Point(point))
                if builder.axes() == CoordinateAxes::from_point(point) =>
            {
                try_grow_coord_builder(builder)?;
                builder.push(point);
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
                crate::try_push(shapes, shape)
            },
        }
    }

    /// Re-expand any packed points into a plain shape list and switch lanes.
    pub(crate) fn try_demote(&mut self) -> PyResult<&mut Vec<Shape>> {
        match std::mem::replace(self, Self::Empty) {
            Self::Points(builder) => {
                let seq = builder.finish_infallible();
                let mut shapes = crate::try_vec_with_capacity(seq.len())?;
                for row in 0..seq.len() {
                    crate::try_push(&mut shapes, Shape::Point(seq.point_at(row)))?;
                }
                *self = Self::Shapes(shapes);
            },
            Self::Lines(builder) => *self = Self::Shapes(builder.into_shapes()),
            Self::Polygons(builder) => *self = Self::Shapes(builder.into_shapes()),
            Self::Shapes(shapes) => *self = Self::Shapes(shapes),
            Self::Empty => *self = Self::Shapes(Vec::new()),
        }
        match self {
            Self::Shapes(shapes) => Ok(shapes),
            _ => unreachable!("demote always leaves the Shapes lane"),
        }
    }

    pub(crate) fn finish(self, frame: Frame) -> PyGeometryArray {
        match self {
            Self::Empty => PyGeometryArray::from_shapes(Vec::new(), frame),
            Self::Points(builder) => {
                PyGeometryArray::packed_points(builder.finish_infallible(), frame)
            },
            Self::Lines(builder) => builder.finish(frame),
            Self::Polygons(builder) => builder.finish(frame),
            Self::Shapes(shapes) => PyGeometryArray::from_shapes(shapes, frame),
        }
    }
}

impl StreamingLines {
    fn try_from_first(line: &LineSeq) -> PyResult<Self> {
        let mut out = Self {
            coords: crate::geometry::CoordSeqBuilder::with_capacity(line.axes(), 0),
            offsets: vec![0],
        };
        // Fallible exact reserve for the first line's known vertex count.
        out.coords
            .try_reserve_exact(line.len())
            .map_err(|_| grow_err())?;
        out.try_push(line)?;
        Ok(out)
    }

    fn try_push(&mut self, line: &LineSeq) -> PyResult<()> {
        for point in line {
            try_grow_coord_builder(&mut self.coords)?;
            self.coords.push(point);
        }
        crate::try_push(&mut self.offsets, self.coords.len())
    }

    fn into_parts(self) -> (CoordSeq, Vec<usize>) {
        (self.coords.finish_infallible(), self.offsets)
    }

    fn into_shapes(self) -> Vec<Shape> {
        let (coords, offsets) = self.into_parts();
        offsets
            .windows(2)
            .map(|ends| {
                let row = coords.view(crate::geometry::CoordWindow::trusted(
                    ends[0]..ends[1],
                    coords.len(),
                ));
                Shape::LineString(LineSeq::from_trusted(row))
            })
            .collect()
    }

    fn finish(self, frame: Frame) -> PyGeometryArray {
        let (coords, offsets) = self.into_parts();
        match crate::geometry::CsrOffsetColumn::try_new(offsets.clone(), coords.len()) {
            Ok(offsets) => PyGeometryArray::packed_lines(coords, offsets, frame),
            Err(_) => {
                PyGeometryArray::from_shapes(line_shapes_from_parts(&coords, &offsets), frame)
            },
        }
    }
}

fn line_shapes_from_parts(coords: &CoordSeq, offsets: &[usize]) -> Vec<Shape> {
    offsets
        .windows(2)
        .map(|ends| {
            let row = coords.view(crate::geometry::CoordWindow::trusted(
                ends[0]..ends[1],
                coords.len(),
            ));
            Shape::LineString(LineSeq::from_trusted(row))
        })
        .collect()
}

impl StreamingPolygons {
    fn try_from_first(polygon: &Polygon) -> PyResult<Self> {
        let axes = polygon_stream_axes(polygon)
            .expect("StreamingPolygons requires one axes layout across every ring");
        let mut out = Self {
            coords: crate::geometry::CoordSeqBuilder::with_capacity(axes, 0),
            ring_offsets: vec![0],
            polygon_offsets: vec![0],
        };
        out.coords
            .try_reserve_exact(polygon.coord_count())
            .map_err(|_| grow_err())?;
        out.try_push(polygon)?;
        Ok(out)
    }

    fn try_push(&mut self, polygon: &Polygon) -> PyResult<()> {
        for ring in polygon.rings() {
            for point in ring {
                try_grow_coord_builder(&mut self.coords)?;
                self.coords.push(point);
            }
            crate::try_push(&mut self.ring_offsets, self.coords.len())?;
        }
        crate::try_push(&mut self.polygon_offsets, self.ring_offsets.len() - 1)
    }

    fn into_parts(self) -> (CoordSeq, Vec<usize>, Vec<usize>) {
        (
            self.coords.finish_infallible(),
            self.ring_offsets,
            self.polygon_offsets,
        )
    }

    fn into_shapes(self) -> Vec<Shape> {
        let (coords, ring_offsets, polygon_offsets) = self.into_parts();
        polygon_shapes_from_parts(&coords, &ring_offsets, &polygon_offsets)
    }

    fn finish(self, frame: Frame) -> PyGeometryArray {
        let (coords, ring_ends, polygon_ends) = self.into_parts();
        let ring_offsets = crate::geometry::CsrOffsetColumn::<crate::geometry::RingLevel>::try_new(
            ring_ends.clone(),
            coords.len(),
        );
        let polygon_offsets =
            crate::geometry::CsrOffsetColumn::<crate::geometry::PolygonLevel>::try_new(
                polygon_ends.clone(),
                ring_ends.len() - 1,
            );
        match (ring_offsets, polygon_offsets) {
            (Ok(ring_offsets), Ok(polygon_offsets)) => {
                PyGeometryArray::packed_polygons(coords, ring_offsets, polygon_offsets, frame)
            },
            _ => PyGeometryArray::from_shapes(
                polygon_shapes_from_parts(&coords, &ring_ends, &polygon_ends),
                frame,
            ),
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
) -> Vec<Shape> {
    polygon_offsets
        .windows(2)
        .map(|polygon_ends| {
            let mut rings = (polygon_ends[0]..polygon_ends[1]).map(|ring| {
                let row = coords.view(crate::geometry::CoordWindow::trusted(
                    ring_offsets[ring]..ring_offsets[ring + 1],
                    coords.len(),
                ));
                Ring::from_trusted_closed(row)
            });
            let shell = rings
                .next()
                .expect("streamed Polygon always contains a shell");
            Shape::Polygon(Polygon::new(shell, rings.collect()))
        })
        .collect()
}
