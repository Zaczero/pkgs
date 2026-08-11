//! Streaming row accumulator for parse-time bulk imports.

use pyo3::exceptions::PyMemoryError;
use pyo3::{PyErr, PyResult};

use crate::array::{CoordSeq, CoordinateAxes, Frame, Polygon, PyGeometryArray, Ring, Shape};
use crate::geometry::{LineSeq, MOrdinate, Point, ZOrdinate};

/// Streaming row accumulator for parse-time bulk imports.
///
/// Homogeneous primitive rows of one axes layout pack straight into growing
/// coordinate/CSR columns — no intermediate `Vec<Shape>` container and no
/// second full-payload representation at final packing. A kind/axes change
/// demotes the accumulated columns to zero-copy row views before continuing
/// through the mixed path.
///
/// Missing rows push **kind-preserving placeholders in final order** via
/// [`try_push_missing`] so finish + [`PyGeometryArray::with_missing_mask`]
/// never needs a second `scatter_present_rows` / `from_shapes` pass (D2
/// homogeneous GeoArrow + Stage 3 null contract).
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
        // Flush leading missings as kind-preserving placeholders for `shape`.
        if let Self::LeadingMissing(n) = *self {
            *self = Self::Empty;
            self.seed_with_leading_missing(n, &shape)?;
        }
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

    /// Append a missing-row placeholder in final order (kind-preserving when
    /// the stream already has a homogeneous lane). Nulls establish no CRS and
    /// are never parsed by the caller.
    pub(crate) fn try_push_missing(&mut self) -> PyResult<()> {
        match self {
            Self::Empty => {
                *self = Self::LeadingMissing(1);
                Ok(())
            },
            Self::LeadingMissing(n) => {
                *n = n.saturating_add(1);
                Ok(())
            },
            Self::Points(builder) => {
                try_grow_coord_builder(builder)?;
                builder.push(missing_point(builder.axes()));
                Ok(())
            },
            Self::Lines(builder) => builder.try_push_missing(),
            Self::Polygons(builder) => builder.try_push_missing(),
            Self::Shapes(shapes) => crate::try_push(shapes, PyGeometryArray::missing_placeholder()),
        }
    }

    /// After `LeadingMissing(n)`, open a homogeneous lane (or Shapes) with `n`
    /// kind-preserving placeholders, then the caller pushes `shape`.
    fn seed_with_leading_missing(&mut self, n: usize, shape: &Shape) -> PyResult<()> {
        match shape {
            Shape::Point(point) => {
                let axes = CoordinateAxes::from_point(*point);
                let mut builder = crate::geometry::CoordSeqBuilder::with_capacity(axes, n);
                builder.try_reserve_exact(n).map_err(|_| grow_err())?;
                for _ in 0..n {
                    builder.push(missing_point(axes));
                }
                *self = Self::Points(builder);
            },
            Shape::LineString(line) => {
                let axes = line.axes();
                let mut out = StreamingLines {
                    coords: crate::geometry::CoordSeqBuilder::with_capacity(axes, 0),
                    offsets: vec![0],
                };
                out.coords
                    .try_reserve_exact(n.saturating_mul(2))
                    .map_err(|_| grow_err())?;
                for _ in 0..n {
                    out.try_push_missing()?;
                }
                *self = Self::Lines(out);
            },
            Shape::Polygon(polygon) if polygon_stream_axes(polygon).is_some() => {
                let axes = polygon_stream_axes(polygon).expect("checked");
                let mut out = StreamingPolygons {
                    coords: crate::geometry::CoordSeqBuilder::with_capacity(axes, 0),
                    ring_offsets: vec![0],
                    polygon_offsets: vec![0],
                };
                out.coords
                    .try_reserve_exact(n.saturating_mul(4))
                    .map_err(|_| grow_err())?;
                for _ in 0..n {
                    out.try_push_missing()?;
                }
                *self = Self::Polygons(out);
            },
            _ => {
                let mut shapes = crate::try_vec_with_capacity(n)?;
                for _ in 0..n {
                    crate::try_push(&mut shapes, PyGeometryArray::missing_placeholder())?;
                }
                *self = Self::Shapes(shapes);
            },
        }
        Ok(())
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
            Self::Empty | Self::LeadingMissing(_) => *self = Self::Shapes(Vec::new()),
        }
        match self {
            Self::Shapes(shapes) => Ok(shapes),
            _ => unreachable!("demote always leaves the Shapes lane"),
        }
    }

    pub(crate) fn finish(self, frame: Frame) -> PyGeometryArray {
        match self {
            Self::Empty => PyGeometryArray::from_shapes(Vec::new(), frame),
            // All-null stream: generic placeholders (no present kind to pack).
            Self::LeadingMissing(n) => {
                let shapes = vec![PyGeometryArray::missing_placeholder(); n];
                PyGeometryArray::from_shapes(shapes, frame)
            },
            Self::Points(builder) => {
                PyGeometryArray::packed_points(builder.finish_infallible(), frame)
            },
            Self::Lines(builder) => builder.finish(frame),
            Self::Polygons(builder) => builder.finish(frame),
            Self::Shapes(shapes) => PyGeometryArray::from_shapes(shapes, frame),
        }
    }
}

/// Missing-row point matching scatter_present_points (NaN on active axes).
fn missing_point(axes: CoordinateAxes) -> Point {
    let z = axes.has_z().then_some(f64::NAN);
    let m = axes.has_m().then_some(f64::NAN);
    Point::new_unchecked_axes(f64::NAN, f64::NAN, ZOrdinate(z), MOrdinate(m))
}

/// Missing-row line: two NaN vertices (scatter_present_lines convention).
fn missing_line(axes: CoordinateAxes) -> LineSeq {
    let a = missing_point(axes);
    let b = missing_point(axes);
    let mut builder = crate::geometry::CoordSeqBuilder::with_capacity(axes, 2);
    builder.push(a);
    builder.push(b);
    LineSeq::from_trusted(builder.finish_infallible())
}

/// Missing-row polygon: one 4-vertex NaN shell (scatter_present_polygons).
fn missing_polygon(axes: CoordinateAxes) -> Polygon {
    let mut builder = crate::geometry::CoordSeqBuilder::with_capacity(axes, 4);
    for _ in 0..4 {
        builder.push(missing_point(axes));
    }
    let ring = Ring::from_trusted_closed(builder.finish_infallible());
    Polygon::new(ring, Vec::new())
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

    fn try_push_missing(&mut self) -> PyResult<()> {
        self.try_push(&missing_line(self.coords.axes()))
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

    fn try_push_missing(&mut self) -> PyResult<()> {
        self.try_push(&missing_polygon(self.coords.axes()))
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
