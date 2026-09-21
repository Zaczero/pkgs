//! One-pass native row publication for bulk geometry construction.

use std::sync::Arc;

use crate::array::{
    ComputedCaches, CoordSeq, CoordinateAxes, Frame, GeometryArrayStorage, GeometryKind,
    PackedStorage, PolygonLevel, PyGeometryArray, RingLevel, RowSelectionRef, Shape, ShapeRow,
    StorageLayout, ValidityBuilder, ValidityMask,
};
use crate::error::ErrorKind;
use crate::geometry::types::{checked_csr_window, ensure_coordseq_vertex_capacity};
use crate::geometry::{
    CoordWindow, CsrOffsetBuilder, CsrOffsetColumn, GeometryErrorKind, LineSeq, Polygon, Ring,
};

#[cfg(test)]
std::thread_local! {
    static STREAM_ROW_ADVANCES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static STREAM_BULK_EXTENSIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static STREAM_BULK_ROWS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_stream_row_advances() {
    STREAM_ROW_ADVANCES.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn stream_row_advances() -> usize {
    STREAM_ROW_ADVANCES.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_stream_bulk_extensions() {
    STREAM_BULK_EXTENSIONS.with(|count| count.set(0));
    STREAM_BULK_ROWS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn stream_bulk_extensions() -> usize {
    STREAM_BULK_EXTENSIONS.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn stream_bulk_rows() -> usize {
    STREAM_BULK_ROWS.with(std::cell::Cell::get)
}

#[cfg(test)]
fn record_stream_row_advance() {
    STREAM_ROW_ADVANCES.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn record_stream_bulk_extension(rows: usize) {
    STREAM_BULK_EXTENSIONS.with(|count| count.set(count.get() + 1));
    STREAM_BULK_ROWS.with(|count| count.set(count.get() + rows));
}

/// Streaming row accumulator for parse-time bulk imports and native array
/// materialization.
///
/// The accumulator owns logical row advancement, validity, homogeneous kind
/// and axes. Present primitive rows append their immutable coordinate spans
/// directly; only an admitted kind/axes mismatch enters the explicit mixed
/// fallback. A finish publishes the native storage and validity together, so
/// nullable rows are not staged and compacted a second time.
pub(crate) struct StreamingShapes {
    state: StreamingState,
    validity: ValidityBuilder,
    coordinate_capacity: usize,
    row_capacity: usize,
    ring_capacity: usize,
}

enum StreamingState {
    Empty,
    LeadingMissing(usize),
    Points(crate::geometry::CoordSeqBuilder),
    Lines(StreamingLines),
    Polygons(StreamingPolygons),
    /// Present rows only. Missingness remains in `validity` and is scattered
    /// exactly once at publication.
    Shapes(Vec<Shape>),
}

pub(crate) struct StreamingLines {
    coords: crate::geometry::CoordSeqBuilder,
    offsets: CsrOffsetBuilder,
}

pub(crate) struct StreamingPolygons {
    coords: crate::geometry::CoordSeqBuilder,
    ring_offsets: CsrOffsetBuilder,
    polygon_offsets: CsrOffsetBuilder,
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

#[derive(Clone, Copy)]
enum RowKind {
    Point(CoordinateAxes),
    Line(CoordinateAxes),
    Polygon(CoordinateAxes),
}

impl StreamingShapes {
    pub(crate) const fn new() -> Self {
        Self {
            state: StreamingState::Empty,
            validity: ValidityBuilder::new(),
            coordinate_capacity: 0,
            row_capacity: 0,
            ring_capacity: 0,
        }
    }

    /// Supply exact selected-payload hints when a caller already owns them.
    /// Hints only affect the first native reservation; observed growth remains
    /// fallible and no hint controls validity or logical length.
    pub(crate) fn with_capacity(coordinate_capacity: usize, row_capacity: usize) -> Self {
        Self::with_capacity_and_rings(coordinate_capacity, row_capacity, row_capacity)
    }

    /// Supply separate exact selected-payload hints for polygon rings.
    pub(crate) fn with_capacity_and_rings(
        coordinate_capacity: usize,
        row_capacity: usize,
        ring_capacity: usize,
    ) -> Self {
        Self {
            state: StreamingState::Empty,
            validity: ValidityBuilder::with_capacity(row_capacity),
            coordinate_capacity,
            row_capacity,
            ring_capacity,
        }
    }

    /// Fallibly append one parsed shape. Prefer this over any infallible push
    /// at a Python-iterator boundary.
    pub(crate) fn try_push(&mut self, shape: Shape) -> crate::error::Result<()> {
        if self.try_push_shape_native(&shape)? {
            self.validity.push(true)?;
            #[cfg(test)]
            record_stream_row_advance();
            return Ok(());
        }
        self.try_demote()?;
        let StreamingState::Shapes(shapes) = &mut self.state else {
            unreachable!("demotion always leaves the mixed lane");
        };
        crate::try_push_vec(shapes, shape)?;
        self.validity.push(true)?;
        #[cfg(test)]
        record_stream_row_advance();
        Ok(())
    }

    pub(crate) fn try_push_shape_ref(&mut self, shape: &Shape) -> crate::error::Result<()> {
        if self.try_push_shape_native(shape)? {
            self.validity.push(true)?;
            #[cfg(test)]
            record_stream_row_advance();
            return Ok(());
        }
        self.try_demote()?;
        let StreamingState::Shapes(shapes) = &mut self.state else {
            unreachable!("demotion always leaves the mixed lane");
        };
        crate::try_push_vec(shapes, shape.clone())?;
        self.validity.push(true)?;
        #[cfg(test)]
        record_stream_row_advance();
        Ok(())
    }

    /// Append an already-resolved native row. Packed lines and polygons stay
    /// as borrowed coordinate/CSR spans; only the explicit mixed fallback
    /// materializes a `Shape`.
    pub(crate) fn try_push_row(&mut self, row: Option<ShapeRow<'_>>) -> crate::error::Result<()> {
        let Some(row) = row else {
            return self.try_push_missing();
        };
        if self.try_push_row_native(row)? {
            self.validity.push(true)?;
            #[cfg(test)]
            record_stream_row_advance();
            return Ok(());
        }
        self.try_demote()?;
        let shape = row.with_shape(Clone::clone)?;
        let StreamingState::Shapes(shapes) = &mut self.state else {
            unreachable!("demotion always leaves the mixed lane");
        };
        crate::try_push_vec(shapes, shape)?;
        self.validity.push(true)?;
        #[cfg(test)]
        record_stream_row_advance();
        Ok(())
    }

    /// Append a missing logical row. Point payload stays present-only; line
    /// and polygon CSR state receives a zero-width logical window.
    pub(crate) fn try_push_missing(&mut self) -> crate::error::Result<()> {
        match &mut self.state {
            StreamingState::Empty => self.state = StreamingState::LeadingMissing(1),
            StreamingState::LeadingMissing(count) => {
                *count = count
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
            },
            StreamingState::Points(_) | StreamingState::Shapes(_) => {},
            StreamingState::Lines(builder) => builder.try_push_missing()?,
            StreamingState::Polygons(builder) => builder.try_push_missing()?,
        }
        self.validity.push(false)?;
        #[cfg(test)]
        record_stream_row_advance();
        Ok(())
    }

    /// Append a dense packed array whose selected rows occupy one contiguous
    /// physical range. Gathered, nullable, mixed, and incompatible sources
    /// decline without touching the destination so callers can use the normal
    /// logical-row fallback.
    pub(crate) fn try_extend_array(
        &mut self,
        array: &PyGeometryArray,
    ) -> crate::error::Result<bool> {
        if array.missing().is_some() {
            return Ok(false);
        }
        match array.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                let Some(rows) = contiguous_rows(row_map.as_deref(), coords.len()) else {
                    return Ok(false);
                };
                if rows.is_empty() {
                    return Ok(true);
                }
                let axes = coords.axes();
                if !self.admit_kind(RowKind::Point(axes)) {
                    return Ok(false);
                }
                if let StreamingState::Points(builder) = &mut self.state {
                    builder.extend_window(coords, rows.clone())?;
                } else {
                    let mut builder = crate::geometry::CoordSeqBuilder::try_with_capacity(
                        axes,
                        self.coordinate_capacity,
                    )?;
                    builder.extend_window(coords, rows.clone())?;
                    self.state = StreamingState::Points(builder);
                }
                self.validity.extend_present(rows.len())?;
                #[cfg(test)]
                record_stream_bulk_extension(rows.len());
                Ok(true)
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let Some(rows) =
                    contiguous_rows(row_map.as_deref(), offsets.len().saturating_sub(1))
                else {
                    return Ok(false);
                };
                if rows.is_empty() {
                    return Ok(true);
                }
                let axes = coords.axes();
                if !self.admit_kind(RowKind::Line(axes)) {
                    return Ok(false);
                }
                if let StreamingState::Lines(builder) = &mut self.state {
                    builder.try_extend(coords, offsets.as_slice(), rows.clone())?;
                } else {
                    let mut builder = StreamingLines::try_new(
                        axes,
                        self.coordinate_capacity,
                        self.row_capacity,
                        self.leading_missing(),
                    )?;
                    builder.try_extend(coords, offsets.as_slice(), rows.clone())?;
                    self.state = StreamingState::Lines(builder);
                }
                self.validity.extend_present(rows.len())?;
                #[cfg(test)]
                record_stream_bulk_extension(rows.len());
                Ok(true)
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let Some(rows) =
                    contiguous_rows(row_map.as_deref(), polygon_offsets.len().saturating_sub(1))
                else {
                    return Ok(false);
                };
                if rows.is_empty() {
                    return Ok(true);
                }
                let axes = coords.axes();
                if !self.admit_kind(RowKind::Polygon(axes)) {
                    return Ok(false);
                }
                if let StreamingState::Polygons(builder) = &mut self.state {
                    builder.try_extend(
                        coords,
                        ring_offsets.as_slice(),
                        polygon_offsets.as_slice(),
                        rows.clone(),
                    )?;
                } else {
                    let mut builder = StreamingPolygons::try_new(
                        axes,
                        self.coordinate_capacity,
                        self.row_capacity,
                        self.ring_capacity,
                        self.leading_missing(),
                    )?;
                    builder.try_extend(
                        coords,
                        ring_offsets.as_slice(),
                        polygon_offsets.as_slice(),
                        rows.clone(),
                    )?;
                    self.state = StreamingState::Polygons(builder);
                }
                self.validity.extend_present(rows.len())?;
                #[cfg(test)]
                record_stream_bulk_extension(rows.len());
                Ok(true)
            },
            GeometryArrayStorage::Mixed(_) => Ok(false),
        }
    }

    fn try_push_shape_native(&mut self, shape: &Shape) -> crate::error::Result<bool> {
        match shape {
            Shape::Point(point) => self.try_push_point_native(*point),
            Shape::LineString(line) => self.try_push_line_native(line),
            Shape::Polygon(polygon) if polygon_stream_axes(polygon).is_some() => {
                self.try_push_polygon_native(polygon)
            },
            _ => Ok(false),
        }
    }

    fn try_push_row_native(&mut self, row: ShapeRow<'_>) -> crate::error::Result<bool> {
        match row {
            ShapeRow::Point(point) => self.try_push_point_native(point),
            ShapeRow::Line(coords, start, end) => {
                self.try_push_line_span_native(coords, start..end)
            },
            ShapeRow::Rings(coords, ring_offsets, start, end) => {
                self.try_push_polygon_spans_native(coords, ring_offsets, start..end)
            },
            ShapeRow::Handle(handle) => self.try_push_shape_native(handle.shape()),
            ShapeRow::Shape(shape) => self.try_push_shape_native(shape),
        }
    }

    fn try_push_point_native(
        &mut self,
        point: crate::geometry::Point,
    ) -> crate::error::Result<bool> {
        let axes = CoordinateAxes::from_point(point);
        if matches!(self.state, StreamingState::Shapes(_)) || !self.admit_kind(RowKind::Point(axes))
        {
            return Ok(false);
        }
        if let StreamingState::Points(builder) = &mut self.state {
            builder.push(point)?;
        } else {
            let mut builder = crate::geometry::CoordSeqBuilder::try_with_capacity(
                axes,
                self.coordinate_capacity,
            )?;
            builder.push(point)?;
            self.state = StreamingState::Points(builder);
        }
        Ok(true)
    }

    fn try_push_line_native(&mut self, line: &LineSeq) -> crate::error::Result<bool> {
        self.try_push_line_span_native(line.as_coords(), 0..line.len())
    }

    fn try_push_line_span_native(
        &mut self,
        coords: &CoordSeq,
        window: std::ops::Range<usize>,
    ) -> crate::error::Result<bool> {
        let axes = coords.axes();
        if matches!(self.state, StreamingState::Shapes(_)) || !self.admit_kind(RowKind::Line(axes))
        {
            return Ok(false);
        }
        if let StreamingState::Lines(builder) = &mut self.state {
            builder.try_push_span(coords, window)?;
        } else {
            let mut builder = StreamingLines::try_new(
                axes,
                self.coordinate_capacity,
                self.row_capacity,
                self.leading_missing(),
            )?;
            builder.try_push_span(coords, window)?;
            self.state = StreamingState::Lines(builder);
        }
        Ok(true)
    }

    fn try_push_polygon_native(&mut self, polygon: &Polygon) -> crate::error::Result<bool> {
        let axes = polygon_stream_axes(polygon).expect("polygon admission checked");
        if matches!(self.state, StreamingState::Shapes(_))
            || !self.admit_kind(RowKind::Polygon(axes))
        {
            return Ok(false);
        }
        if let StreamingState::Polygons(builder) = &mut self.state {
            builder.try_push(polygon)?;
        } else {
            let mut builder = StreamingPolygons::try_new(
                axes,
                self.coordinate_capacity.max(polygon.coord_count()),
                self.row_capacity,
                self.ring_capacity,
                self.leading_missing(),
            )?;
            builder.try_push(polygon)?;
            self.state = StreamingState::Polygons(builder);
        }
        Ok(true)
    }

    fn try_push_polygon_spans_native(
        &mut self,
        coords: &CoordSeq,
        ring_offsets: &[i32],
        rings: std::ops::Range<usize>,
    ) -> crate::error::Result<bool> {
        if rings.clone().any(|ring| {
            let start = ring_offsets[ring] as usize;
            let end = ring_offsets[ring + 1] as usize;
            super::CheckedRingWindow::checked(coords, start..end).is_err()
        }) {
            return Ok(false);
        }
        let axes = coords.axes();
        if matches!(self.state, StreamingState::Shapes(_))
            || !self.admit_kind(RowKind::Polygon(axes))
        {
            return Ok(false);
        }
        if let StreamingState::Polygons(builder) = &mut self.state {
            builder.try_push_spans(coords, ring_offsets, rings)?;
        } else {
            let mut builder = StreamingPolygons::try_new(
                axes,
                self.coordinate_capacity,
                self.row_capacity,
                self.ring_capacity,
                self.leading_missing(),
            )?;
            builder.try_push_spans(coords, ring_offsets, rings)?;
            self.state = StreamingState::Polygons(builder);
        }
        Ok(true)
    }

    const fn leading_missing(&self) -> usize {
        match self.state {
            StreamingState::LeadingMissing(count) => count,
            _ => 0,
        }
    }

    fn admit_kind(&self, kind: RowKind) -> bool {
        match (&self.state, kind) {
            (StreamingState::Empty | StreamingState::LeadingMissing(_), _) => true,
            (StreamingState::Points(_), RowKind::Point(axes))
            | (StreamingState::Lines(_), RowKind::Line(axes))
            | (StreamingState::Polygons(_), RowKind::Polygon(axes)) => self.state_axes() == axes,
            _ => false,
        }
    }

    const fn state_axes(&self) -> CoordinateAxes {
        match &self.state {
            StreamingState::Points(builder) => builder.axes(),
            StreamingState::Lines(builder) => builder.coords.axes(),
            StreamingState::Polygons(builder) => builder.coords.axes(),
            StreamingState::Empty
            | StreamingState::LeadingMissing(_)
            | StreamingState::Shapes(_) => CoordinateAxes::XY,
        }
    }

    /// Re-expand native state into present-only mixed shapes. Validity remains
    /// owned by this accumulator and is therefore not represented as scalar
    /// placeholder shapes.
    pub(crate) fn try_demote(&mut self) -> crate::error::Result<&mut Vec<Shape>> {
        let state = std::mem::replace(&mut self.state, StreamingState::Empty);
        let result = (|| -> crate::error::Result<Vec<Shape>> {
            match state {
                StreamingState::Points(builder) => {
                    let coords = builder.finish()?;
                    let mut shapes = Vec::new();
                    crate::boundary::try_reserve_vec(&mut shapes, coords.len())?;
                    for point in &coords {
                        crate::try_push_vec(&mut shapes, Shape::Point(point))?;
                    }
                    Ok(shapes)
                },
                StreamingState::Lines(builder) => builder.into_present_shapes(&self.validity),
                StreamingState::Polygons(builder) => builder.into_present_shapes(&self.validity),
                StreamingState::Shapes(shapes) => Ok(shapes),
                StreamingState::Empty | StreamingState::LeadingMissing(_) => Ok(Vec::new()),
            }
        })();
        let shapes = match result {
            Ok(shapes) => shapes,
            Err(error) => {
                // The native state was consumed while attempting the
                // demotion. It cannot be reused as a valid row stream after
                // a failed publication.
                self.validity = ValidityBuilder::new();
                return Err(error);
            },
        };
        self.state = StreamingState::Shapes(shapes);
        let StreamingState::Shapes(shapes) = &mut self.state else {
            unreachable!("demotion always leaves the mixed lane");
        };
        Ok(shapes)
    }

    /// Finish one native storage/mask publication. Length checks happen before
    /// an all-present mask is normalized to `None`.
    pub(crate) fn finish_parts(
        self,
    ) -> crate::error::Result<(GeometryArrayStorage, Option<ValidityMask>, StorageLayout)> {
        let Self {
            state, validity, ..
        } = self;
        let logical_len = validity.len();
        let present_len = validity.present_count();
        let (storage, layout) = match state {
            StreamingState::Empty | StreamingState::LeadingMissing(_) => (
                GeometryArrayStorage::Mixed(Vec::new()),
                StorageLayout::PresentOnly,
            ),
            StreamingState::Shapes(shapes) => {
                if shapes.len() != present_len {
                    return Err(GeometryErrorKind::message(
                        "mixed present payload length does not match validity",
                    ));
                }
                (
                    GeometryArrayStorage::Mixed(shapes),
                    StorageLayout::PresentOnly,
                )
            },
            StreamingState::Points(builder) => {
                let coords = builder.finish()?;
                if coords.len() != present_len {
                    return Err(GeometryErrorKind::message(
                        "point payload length does not match validity",
                    ));
                }
                (
                    GeometryArrayStorage::Packed(PackedStorage::Points {
                        coords: Arc::try_new(coords).map_err(crate::error::Error::from)?,
                        row_map: crate::array::RowSelection::Identity,
                    }),
                    StorageLayout::PresentOnly,
                )
            },
            StreamingState::Lines(builder) => {
                let (coords, offsets) = builder.into_parts()?;
                if offsets.len()
                    != logical_len
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?
                {
                    return Err(GeometryErrorKind::message(
                        "line logical offsets length does not match validity",
                    ));
                }
                let storage = if packed_i32_capacity(coords.len())? {
                    GeometryArrayStorage::Packed(PackedStorage::Lines {
                        coords: Arc::try_new(coords).map_err(crate::error::Error::from)?,
                        offsets,
                        row_map: crate::array::RowSelection::Identity,
                    })
                } else {
                    GeometryArrayStorage::Mixed(line_shapes_from_parts(&coords, &offsets)?)
                };
                let layout = if matches!(storage, GeometryArrayStorage::Packed(_)) {
                    StorageLayout::LogicalRows
                } else {
                    StorageLayout::PresentOnly
                };
                (storage, layout)
            },
            StreamingState::Polygons(builder) => {
                let (coords, ring_offsets, polygon_offsets) = builder.into_parts()?;
                if polygon_offsets.len()
                    != logical_len
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?
                {
                    return Err(GeometryErrorKind::message(
                        "polygon logical offsets length does not match validity",
                    ));
                }
                let ring_count = ring_offsets.len().saturating_sub(1);
                let storage =
                    if packed_i32_capacity(coords.len())? && packed_i32_capacity(ring_count)? {
                        GeometryArrayStorage::Packed(PackedStorage::Polygons {
                            coords: Arc::try_new(coords).map_err(crate::error::Error::from)?,
                            ring_offsets: ring_offsets.cast_level::<RingLevel>(),
                            polygon_offsets: polygon_offsets.cast_level::<PolygonLevel>(),
                            row_map: crate::array::RowSelection::Identity,
                        })
                    } else {
                        GeometryArrayStorage::Mixed(polygon_shapes_from_parts(
                            &coords,
                            &ring_offsets,
                            &polygon_offsets,
                        )?)
                    };
                let layout = if matches!(storage, GeometryArrayStorage::Packed(_)) {
                    StorageLayout::LogicalRows
                } else {
                    StorageLayout::PresentOnly
                };
                (storage, layout)
            },
        };
        let payload_len = storage.len();
        let expected = match layout {
            StorageLayout::PresentOnly => present_len,
            StorageLayout::LogicalRows => logical_len,
        };
        if payload_len != expected {
            return Err(GeometryErrorKind::message(
                "native payload length does not match its declared layout",
            ));
        }
        let mask = validity.finish();
        Ok((storage, mask, layout))
    }

    pub(crate) fn finish(self, frame: Frame) -> crate::error::Result<PyGeometryArray> {
        let (storage, missing, layout) = self.finish_parts()?;
        PyGeometryArray::from_result_storage(
            Arc::try_new(storage).map_err(crate::error::Error::from)?,
            frame,
            missing,
            layout,
            ComputedCaches::fresh()?,
        )
    }
}

fn contiguous_rows(
    selection: RowSelectionRef<'_>,
    physical_len: usize,
) -> Option<std::ops::Range<usize>> {
    match selection {
        RowSelectionRef::Identity => Some(0..physical_len),
        RowSelectionRef::Window { start, len } => start
            .checked_add(len)
            .filter(|&end| end <= physical_len)
            .map(|end| start..end),
        RowSelectionRef::Gather(_) => None,
    }
}

impl StreamingLines {
    fn try_new(
        axes: CoordinateAxes,
        coordinate_capacity: usize,
        row_capacity: usize,
        leading_missing: usize,
    ) -> crate::error::Result<Self> {
        let offsets = CsrOffsetBuilder::with_capacity(row_capacity)?;
        let mut out = Self {
            coords: crate::geometry::CoordSeqBuilder::try_with_capacity(axes, coordinate_capacity)?,
            offsets,
        };
        for _ in 0..leading_missing {
            out.try_push_missing()?;
        }
        Ok(out)
    }

    fn try_push_span(
        &mut self,
        coords: &CoordSeq,
        window: std::ops::Range<usize>,
    ) -> crate::error::Result<()> {
        self.coords.extend_window(coords, window)?;
        self.offsets.push_end(self.coords.len(), i32::MAX as usize)
    }

    fn try_extend(
        &mut self,
        coords: &CoordSeq,
        offsets: &[i32],
        rows: std::ops::Range<usize>,
    ) -> crate::error::Result<()> {
        let window = checked_csr_window(offsets, rows, coords.len())?;
        let output_start = self.coords.len();
        self.coords
            .extend_window(coords, window.source_start()..window.source_end())?;
        self.offsets.extend_rebased(window, output_start)
    }

    fn try_push_missing(&mut self) -> crate::error::Result<()> {
        self.offsets.push_end(self.coords.len(), i32::MAX as usize)
    }

    fn into_parts(self) -> crate::error::Result<(CoordSeq, CsrOffsetColumn)> {
        let Self { coords, offsets } = self;
        let coords = coords.finish()?;
        let offsets = offsets.finish(coords.len())?;
        Ok((coords, offsets))
    }

    fn into_present_shapes(self, validity: &ValidityBuilder) -> crate::error::Result<Vec<Shape>> {
        let (coords, offsets) = self.into_parts()?;
        let mut shapes = Vec::new();
        crate::boundary::try_reserve_vec(&mut shapes, validity.present_count())?;
        let mut present = 0;
        for logical in 0..validity.len() {
            if validity.is_present(logical) {
                let start = offsets[logical] as usize;
                let end = offsets[logical + 1] as usize;
                let row = coords.view(CoordWindow::from_range(start..end, coords.len()));
                let shape = if row.is_empty() {
                    Shape::typed_empty(GeometryKind::LineString, coords.axes())
                } else {
                    Shape::LineString(LineSeq::from_proven(row))
                };
                crate::try_push_vec(&mut shapes, shape)?;
                present += 1;
            }
        }
        debug_assert_eq!(present, validity.present_count());
        Ok(shapes)
    }
}

impl StreamingPolygons {
    fn try_new(
        axes: CoordinateAxes,
        coordinate_capacity: usize,
        row_capacity: usize,
        ring_capacity: usize,
        leading_missing: usize,
    ) -> crate::error::Result<Self> {
        let mut out = Self {
            coords: crate::geometry::CoordSeqBuilder::try_with_capacity(axes, coordinate_capacity)?,
            ring_offsets: CsrOffsetBuilder::with_capacity(ring_capacity)?,
            polygon_offsets: CsrOffsetBuilder::with_capacity(row_capacity)?,
        };
        for _ in 0..leading_missing {
            out.try_push_missing()?;
        }
        Ok(out)
    }

    fn try_push(&mut self, polygon: &Polygon) -> crate::error::Result<()> {
        for ring in polygon.rings() {
            self.coords.extend_window(ring, 0..ring.len())?;
            self.ring_offsets
                .push_end(self.coords.len(), i32::MAX as usize)?;
        }
        self.polygon_offsets
            .push_end(self.ring_offsets.row_count(), i32::MAX as usize)
    }

    fn try_push_spans(
        &mut self,
        coords: &CoordSeq,
        ring_offsets: &[i32],
        rings: std::ops::Range<usize>,
    ) -> crate::error::Result<()> {
        for ring in rings {
            let start = ring_offsets[ring] as usize;
            let end = ring_offsets[ring + 1] as usize;
            self.coords.extend_window(coords, start..end)?;
            self.ring_offsets
                .push_end(self.coords.len(), i32::MAX as usize)?;
        }
        self.polygon_offsets
            .push_end(self.ring_offsets.row_count(), i32::MAX as usize)
    }

    fn try_extend(
        &mut self,
        coords: &CoordSeq,
        ring_offsets: &[i32],
        polygon_offsets: &[i32],
        rows: std::ops::Range<usize>,
    ) -> crate::error::Result<()> {
        let polygon_window =
            checked_csr_window(polygon_offsets, rows, ring_offsets.len().saturating_sub(1))?;
        let ring_window = checked_csr_window(
            ring_offsets,
            polygon_window.source_start()..polygon_window.source_end(),
            coords.len(),
        )?;
        let output_coordinate_start = self.coords.len();
        let output_ring_start = self.ring_offsets.row_count();
        self.coords
            .extend_window(coords, ring_window.source_start()..ring_window.source_end())?;
        self.ring_offsets
            .extend_rebased(ring_window, output_coordinate_start)?;
        self.polygon_offsets
            .extend_rebased(polygon_window, output_ring_start)
    }

    fn try_push_missing(&mut self) -> crate::error::Result<()> {
        self.polygon_offsets
            .push_end(self.ring_offsets.row_count(), i32::MAX as usize)
    }

    fn into_parts(
        self,
    ) -> crate::error::Result<(
        CoordSeq,
        CsrOffsetColumn<RingLevel>,
        CsrOffsetColumn<PolygonLevel>,
    )> {
        let Self {
            coords,
            ring_offsets,
            polygon_offsets,
        } = self;
        let coords = coords.finish()?;
        let ring_count = ring_offsets.row_count();
        let ring_offsets = ring_offsets.finish(coords.len())?.cast_level::<RingLevel>();
        let polygon_offsets = polygon_offsets
            .finish(ring_count)?
            .cast_level::<PolygonLevel>();
        Ok((coords, ring_offsets, polygon_offsets))
    }

    fn into_present_shapes(self, validity: &ValidityBuilder) -> crate::error::Result<Vec<Shape>> {
        let (coords, ring_offsets, polygon_offsets) = self.into_parts()?;
        let mut shapes = Vec::new();
        crate::boundary::try_reserve_vec(&mut shapes, validity.present_count())?;
        for logical in 0..validity.len() {
            if !validity.is_present(logical) {
                continue;
            }
            let rings = polygon_offsets[logical] as usize..polygon_offsets[logical + 1] as usize;
            if rings.is_empty() {
                crate::try_push_vec(
                    &mut shapes,
                    Shape::typed_empty(GeometryKind::Polygon, coords.axes()),
                )?;
                continue;
            }
            let ring = |index: usize| {
                let start = ring_offsets[index] as usize;
                let end = ring_offsets[index + 1] as usize;
                Ring::try_from_closed(
                    coords.view(CoordWindow::from_range(start..end, coords.len())),
                )
            };
            let shell = ring(rings.start)?;
            let mut holes = Vec::new();
            crate::boundary::try_reserve_vec(&mut holes, rings.len() - 1)?;
            for index in rings.start + 1..rings.end {
                crate::try_push_vec(&mut holes, ring(index)?)?;
            }
            crate::try_push_vec(&mut shapes, Shape::Polygon(Polygon::new(shell, holes)))?;
        }
        Ok(shapes)
    }
}

fn polygon_stream_axes(polygon: &Polygon) -> Option<CoordinateAxes> {
    super::polygon_pack_axes(polygon)
}

fn line_shapes_from_parts(
    coords: &CoordSeq,
    offsets: &CsrOffsetColumn,
) -> crate::error::Result<Vec<Shape>> {
    let mut shapes = Vec::new();
    crate::boundary::try_reserve_vec(&mut shapes, offsets.len().saturating_sub(1))?;
    for [start, end] in offsets.array_windows::<2>() {
        let row = coords.view(CoordWindow::from_range(
            *start as usize..*end as usize,
            coords.len(),
        ));
        crate::try_push_vec(
            &mut shapes,
            if row.is_empty() {
                Shape::typed_empty(GeometryKind::LineString, coords.axes())
            } else {
                Shape::LineString(LineSeq::from_proven(row))
            },
        )?;
    }
    Ok(shapes)
}

fn polygon_shapes_from_parts(
    coords: &CoordSeq,
    ring_offsets: &CsrOffsetColumn<RingLevel>,
    polygon_offsets: &CsrOffsetColumn<PolygonLevel>,
) -> crate::error::Result<Vec<Shape>> {
    let mut shapes = Vec::new();
    crate::boundary::try_reserve_vec(&mut shapes, polygon_offsets.len().saturating_sub(1))?;
    for [start, end] in polygon_offsets.array_windows::<2>() {
        let rings = *start as usize..*end as usize;
        if rings.is_empty() {
            crate::try_push_vec(
                &mut shapes,
                Shape::typed_empty(GeometryKind::Polygon, coords.axes()),
            )?;
            continue;
        }
        let ring = |index: usize| {
            let start = ring_offsets[index] as usize;
            let end = ring_offsets[index + 1] as usize;
            Ring::try_from_closed(coords.view(CoordWindow::from_range(start..end, coords.len())))
        };
        let shell = ring(rings.start)?;
        let mut holes = Vec::new();
        for index in rings.start + 1..rings.end {
            crate::try_push_vec(&mut holes, ring(index)?)?;
        }
        crate::try_push_vec(&mut shapes, Shape::Polygon(Polygon::new(shell, holes)))?;
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
        Shape::Polygon(Polygon::new(Ring::from_closed(coords).unwrap(), Vec::new()))
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
        assert!(array.is_row_missing(1));
        assert!(!array.is_row_missing(2));
        assert!(!array.is_row_missing(3));
        assert_eq!(array.geometry_at(0).unwrap().shape.shape(), &first);
        assert!(matches!(
            array.logical_row(1),
            Some(crate::array::LogicalRow::Missing { .. })
        ));
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

    #[test]
    fn nullable_native_lanes_publish_one_storage_and_validity_owner() {
        reset_stream_row_advances();
        let mut points = StreamingShapes::new();
        points.try_push_missing().unwrap();
        points
            .try_push(Shape::Point(Point::new_raw_xy(1.0, 2.0)))
            .unwrap();
        points
            .try_push(Shape::Point(Point::new_raw_xy(3.0, 4.0)))
            .unwrap();
        let points = points.finish(Frame::None).unwrap();
        assert_eq!(stream_row_advances(), 3);
        assert!(points.is_compact_nullable());
        assert_eq!(points.len(), 3);
        assert_eq!(
            points
                .logical_rows()
                .map(|row| matches!(row, crate::array::LogicalRow::Missing { .. }))
                .collect::<Vec<_>>(),
            [true, false, false]
        );

        let line = xy_line(&[(0.0, 0.0), (1.0, 1.0)]);
        let mut lines = StreamingShapes::new();
        lines.try_push_missing().unwrap();
        lines.try_push(line.clone()).unwrap();
        lines.try_push_missing().unwrap();
        lines.try_push(line).unwrap();
        let lines = lines.finish(Frame::None).unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = lines.storage()
        else {
            panic!("homogeneous nullable lines must stay packed");
        };
        assert!(lines.is_compact_nullable());
        assert_eq!(coords.len(), 4);
        assert_eq!(offsets.as_slice(), &[0, 0, 2, 2, 4]);

        let polygon = xy_square();
        let mut polygons = StreamingShapes::new();
        polygons.try_push_missing().unwrap();
        polygons.try_push(polygon.clone()).unwrap();
        polygons.try_push_missing().unwrap();
        polygons.try_push(polygon).unwrap();
        let polygons = polygons.finish(Frame::None).unwrap();
        let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            ..
        }) = polygons.storage()
        else {
            panic!("homogeneous nullable polygons must stay packed");
        };
        assert!(polygons.is_compact_nullable());
        assert_eq!(coords.len(), 10);
        assert_eq!(ring_offsets.as_slice(), &[0, 5, 10]);
        assert_eq!(polygon_offsets.as_slice(), &[0, 0, 1, 1, 2]);
    }

    #[test]
    fn borrowed_row_append_has_no_shape_iterator_staging_counter() {
        let source = PyGeometryArray::from_shapes(
            vec![
                xy_line(&[(0.0, 0.0), (1.0, 1.0)]),
                xy_line(&[(2.0, 2.0), (3.0, 3.0)]),
                xy_line(&[(4.0, 4.0), (5.0, 5.0)]),
            ],
            Frame::None,
        )
        .unwrap();

        crate::array::reset_shape_iter_materializations();
        let staged = source.storage().iter_shapes().count();
        assert_eq!(staged, 3);
        assert!(
            crate::array::shape_iter_materializations() > 0,
            "the staged negative control must exercise shape materialization"
        );

        crate::array::reset_shape_iter_materializations();
        let mut builder = StreamingShapes::with_capacity(6, 3);
        for row in source.row_operation().rows_with_missing(|| ()) {
            match row {
                crate::array::ArrayRow::Missing { .. } => builder.try_push_missing().unwrap(),
                crate::array::ArrayRow::Present(row) => {
                    builder.try_push_row(Some(row.row())).unwrap();
                },
            }
        }
        let direct = builder.finish(Frame::None).unwrap();
        let (
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: source_coords,
                offsets: source_offsets,
                ..
            }),
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: direct_coords,
                offsets: direct_offsets,
                ..
            }),
        ) = (source.storage(), direct.storage())
        else {
            panic!("native row append must retain packed line storage");
        };
        assert_eq!(direct_coords.xs(), source_coords.xs());
        assert_eq!(direct_coords.ys(), source_coords.ys());
        assert_eq!(direct_offsets.as_slice(), source_offsets.as_slice());
        assert_eq!(
            crate::array::shape_iter_materializations(),
            0,
            "native row append must not stage Shape values"
        );
    }
}
