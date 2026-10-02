//! One-pass native row publication for bulk geometry construction.

use std::sync::Arc;

use crate::array::{
    ComputedCaches, CoordSeq, CoordinateAxes, Frame, GeometryKind, PolygonLevel, PyGeometryArray,
    RingLevel, Shape, StorageLayout,
};
use crate::geometry::storage::{
    GeometryArrayStorage, PackedRow, PackedStorage, RowSelectionRef, ShapeRow, ValidityBuilder,
    ValidityMask, admit_packed_total, csr_value_window, materialized_packed_lines_len,
    materialized_packed_polygons_len,
};
use crate::geometry::types::checked_csr_window;
use crate::geometry::{
    CoordWindow, CsrOffsetBuilder, CsrOffsetColumn, GeometryErrorKind, LineSeq, Polygon,
    ShapeCountQuery, storage,
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
    STREAM_ROW_ADVANCES.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(test)]
fn record_stream_bulk_extension(rows: usize) {
    STREAM_BULK_EXTENSIONS.with(|count| count.set(count.get().saturating_add(1)));
    STREAM_BULK_ROWS.with(|count| count.set(count.get().saturating_add(rows)));
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

#[derive(Clone, Copy)]
enum RowKind {
    Point(CoordinateAxes),
    Line(CoordinateAxes),
    Polygon(CoordinateAxes),
}

// Check representation size separately from allocation; Vec's allocator
// failure remains process-fatal.
fn admit_vec_capacity<T>(capacity: usize) -> crate::error::Result<()> {
    std::alloc::Layout::array::<T>(capacity)
        .map(|_| ())
        .map_err(|_| crate::error::ResourceError::SizeOverflow.into())
}

fn admit_offset_rows_capacity(rows: usize) -> crate::error::Result<()> {
    // CsrOffsetBuilder stores the implicit initial zero as an extra entry.
    let offsets = rows
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    admit_vec_capacity::<i32>(offsets)
}

fn shape_vec_capacity(required: usize, hint: usize) -> crate::error::Result<usize> {
    let capacity = required.max(hint);
    admit_vec_capacity::<Shape>(capacity)?;
    Ok(capacity)
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
    /// Hints only affect the first native reservation; no hint controls
    /// validity or logical length.
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
            coordinate_capacity: coordinate_capacity.min(i32::MAX as usize),
            row_capacity,
            ring_capacity: ring_capacity.min(i32::MAX as usize),
        }
    }

    /// Append one parsed shape, retaining coordinate and CSR admission errors.
    pub(crate) fn try_push(&mut self, shape: Shape) -> crate::error::Result<()> {
        if self.try_push_shape_native(&shape)? {
            self.validity.push(true);
            #[cfg(test)]
            record_stream_row_advance();
            return Ok(());
        }
        self.try_demote()?.push(shape);
        self.validity.push(true);
        #[cfg(test)]
        record_stream_row_advance();
        Ok(())
    }

    pub(crate) fn try_push_shape_ref(&mut self, shape: &Shape) -> crate::error::Result<()> {
        if self.try_push_shape_native(shape)? {
            self.validity.push(true);
            #[cfg(test)]
            record_stream_row_advance();
            return Ok(());
        }
        self.try_demote()?.push(shape.clone());
        self.validity.push(true);
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
            self.validity.push(true);
            #[cfg(test)]
            record_stream_row_advance();
            return Ok(());
        }
        let shapes = self.try_demote()?;
        let shape = row.with_shape(Clone::clone)?;
        shapes.push(shape);
        self.validity.push(true);
        #[cfg(test)]
        record_stream_row_advance();
        Ok(())
    }

    /// Append a missing logical row. Point payload stays present-only; line
    /// and polygon CSR state receives a zero-width logical window.
    pub(crate) fn try_push_missing(&mut self) -> crate::error::Result<()> {
        match &mut self.state {
            StreamingState::Empty | StreamingState::Points(_) | StreamingState::Shapes(_) => {},
            StreamingState::Lines(builder) => builder.try_push_missing()?,
            StreamingState::Polygons(builder) => builder.try_push_missing()?,
        }
        self.validity.push(false);
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
                let count = rows.len();
                let axes = coords.axes();
                if !self.admit_kind(RowKind::Point(axes)) {
                    return Ok(false);
                }
                let existing = match &self.state {
                    StreamingState::Points(builder) => builder.len(),
                    StreamingState::Empty
                    | StreamingState::Lines(_)
                    | StreamingState::Polygons(_)
                    | StreamingState::Shapes(_) => 0,
                };
                admit_packed_total(existing, count)?;
                if let StreamingState::Points(builder) = &mut self.state {
                    builder.extend_window(coords, rows.clone());
                } else {
                    let mut builder = crate::geometry::CoordSeqBuilder::with_capacity(
                        axes,
                        self.coordinate_capacity,
                    );
                    builder.extend_window(coords, rows.clone());
                    self.state = StreamingState::Points(builder);
                }
                self.validity.extend_present(rows.len());
                #[cfg(test)]
                record_stream_bulk_extension(rows.len());
                Ok(true)
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
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
                let incoming = materialized_packed_lines_len(offsets.as_slice(), map)?;
                let existing = match &self.state {
                    StreamingState::Lines(builder) => builder.coords.len(),
                    StreamingState::Empty
                    | StreamingState::Points(_)
                    | StreamingState::Polygons(_)
                    | StreamingState::Shapes(_) => 0,
                };
                admit_packed_total(existing, incoming)?;
                if let StreamingState::Lines(builder) = &mut self.state {
                    builder.try_extend_admitted(coords, offsets.as_slice(), rows.clone())?;
                } else {
                    let mut builder = StreamingLines::try_new(
                        axes,
                        self.coordinate_capacity,
                        self.row_capacity,
                        self.validity.len(),
                    )?;
                    builder.try_extend_admitted(coords, offsets.as_slice(), rows.clone())?;
                    self.state = StreamingState::Lines(builder);
                }
                self.validity.extend_present(rows.len());
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
                let map = row_map.as_deref();
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
                let (incoming_vertices, incoming_rings) = materialized_packed_polygons_len(
                    ring_offsets.as_slice(),
                    polygon_offsets.as_slice(),
                    map,
                )?;
                let (existing_vertices, existing_rings) = match &self.state {
                    StreamingState::Polygons(builder) => {
                        (builder.coords.len(), builder.ring_offsets.row_count())
                    },
                    StreamingState::Empty
                    | StreamingState::Points(_)
                    | StreamingState::Lines(_)
                    | StreamingState::Shapes(_) => (0, 0),
                };
                admit_packed_total(existing_vertices, incoming_vertices)?;
                admit_packed_total(existing_rings, incoming_rings)?;
                if let StreamingState::Polygons(builder) = &mut self.state {
                    builder.try_extend_admitted(
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
                        self.validity.len(),
                    )?;
                    builder.try_extend_admitted(
                        coords,
                        ring_offsets.as_slice(),
                        polygon_offsets.as_slice(),
                        rows.clone(),
                    )?;
                    self.state = StreamingState::Polygons(builder);
                }
                self.validity.extend_present(rows.len());
                #[cfg(test)]
                record_stream_bulk_extension(rows.len());
                Ok(true)
            },
            GeometryArrayStorage::Mixed(_) => Ok(false),
        }
    }

    fn try_push_shape_native(&mut self, shape: &Shape) -> crate::error::Result<bool> {
        match shape {
            Shape::Point(point) => self.push_point_native(*point),
            Shape::LineString(line) => self.try_push_line_native(line),
            Shape::Polygon(polygon) if super::polygon_pack_axes(polygon).is_some() => {
                self.try_push_polygon_native(polygon)
            },
            Shape::MultiPoint(_)
            | Shape::MultiLineString(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..)
            | Shape::Polygon(_) => Ok(false),
        }
    }

    fn try_push_row_native(&mut self, row: ShapeRow<'_>) -> crate::error::Result<bool> {
        match row {
            ShapeRow::Point(point) => self.push_point_native(point),
            ShapeRow::Packed(PackedRow::Lines {
                coords,
                vertices_start,
                vertices_end,
                ..
            }) => self.try_push_line_span_native(coords, vertices_start..vertices_end),
            ShapeRow::Packed(PackedRow::Polygons {
                coords,
                ring_offsets,
                rings_start,
                rings_end,
                ..
            }) => self.try_push_polygon_spans_native(
                coords,
                ring_offsets.as_slice(),
                rings_start..rings_end,
            ),
            ShapeRow::Packed(_) => Ok(false),
            ShapeRow::Handle(handle) => self.try_push_shape_native(handle.shape()),
            ShapeRow::Shape(shape) => self.try_push_shape_native(shape),
        }
    }

    fn push_point_native(&mut self, point: crate::geometry::Point) -> crate::error::Result<bool> {
        let axes = CoordinateAxes::from_point(point);
        if matches!(self.state, StreamingState::Shapes(_)) || !self.admit_kind(RowKind::Point(axes))
        {
            return Ok(false);
        }
        if let StreamingState::Points(builder) = &mut self.state {
            admit_packed_total(builder.len(), 1)?;
            builder.push(point);
        } else {
            let mut builder =
                crate::geometry::CoordSeqBuilder::with_capacity(axes, self.coordinate_capacity);
            builder.push(point);
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
                self.validity.len(),
            )?;
            builder.try_push_span(coords, window)?;
            self.state = StreamingState::Lines(builder);
        }
        Ok(true)
    }

    fn try_push_polygon_native(&mut self, polygon: &Polygon) -> crate::error::Result<bool> {
        let axes = super::polygon_pack_axes(polygon).expect("polygon admission checked");
        if matches!(self.state, StreamingState::Shapes(_))
            || !self.admit_kind(RowKind::Polygon(axes))
        {
            return Ok(false);
        }
        if let StreamingState::Polygons(builder) = &mut self.state {
            builder.try_push(polygon)?;
        } else {
            let polygon_capacity = ShapeCountQuery::default()
                .polygon_coordinates(polygon)
                .ok()
                .filter(|&capacity| i32::try_from(capacity).is_ok());
            let mut builder = StreamingPolygons::try_new(
                axes,
                self.coordinate_capacity,
                self.row_capacity,
                self.ring_capacity,
                self.validity.len(),
            )?;
            if let Some(capacity) = polygon_capacity {
                builder.coords.reserve_hint(capacity);
            }
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
            let &[start, end] = ring_offsets
                .get(ring..)
                .and_then(|offsets| offsets.first_chunk::<2>())
                .expect("polygon ring range is bounded by its admitted offsets");
            let start = start as usize;
            let end = end as usize;
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
                self.validity.len(),
            )?;
            builder.try_push_spans(coords, ring_offsets, rings)?;
            self.state = StreamingState::Polygons(builder);
        }
        Ok(true)
    }

    fn admit_kind(&self, kind: RowKind) -> bool {
        match (&self.state, kind) {
            (StreamingState::Empty, _) => true,
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
            StreamingState::Empty | StreamingState::Shapes(_) => CoordinateAxes::XY,
        }
    }

    /// Re-expand native state into present-only mixed shapes. Validity remains
    /// owned by this accumulator and is therefore not represented as scalar
    /// placeholder shapes.
    fn try_demote(&mut self) -> crate::error::Result<&mut Vec<Shape>> {
        let state = std::mem::replace(&mut self.state, StreamingState::Empty);
        let result = (|| -> crate::error::Result<Vec<Shape>> {
            match state {
                StreamingState::Points(builder) => {
                    let coords = builder.finish()?;
                    let capacity = shape_vec_capacity(coords.len(), self.row_capacity)?;
                    let mut shapes = Vec::with_capacity(capacity);
                    shapes.extend(coords.iter().map(Shape::Point));
                    Ok(shapes)
                },
                StreamingState::Lines(builder) => {
                    builder.into_present_shapes(&self.validity, self.row_capacity)
                },
                StreamingState::Polygons(builder) => {
                    builder.into_present_shapes(&self.validity, self.row_capacity)
                },
                StreamingState::Shapes(shapes) => Ok(shapes),
                StreamingState::Empty => {
                    let capacity = shape_vec_capacity(0, self.row_capacity)?;
                    Ok(Vec::with_capacity(capacity))
                },
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
            StreamingState::Empty => (
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
                        coords: Arc::new(coords),
                        row_map: storage::RowSelection::Identity,
                    }),
                    StorageLayout::PresentOnly,
                )
            },
            StreamingState::Lines(builder) => {
                let (coords, offsets) = builder.into_parts()?;
                if offsets.len() != logical_len.wrapping_add(1) {
                    return Err(GeometryErrorKind::message(
                        "line logical offsets length does not match validity",
                    ));
                }
                (
                    GeometryArrayStorage::Packed(PackedStorage::Lines {
                        coords: Arc::new(coords),
                        offsets,
                        row_map: storage::RowSelection::Identity,
                    }),
                    StorageLayout::LogicalRows,
                )
            },
            StreamingState::Polygons(builder) => {
                let (coords, ring_offsets, polygon_offsets) = builder.into_parts()?;
                if polygon_offsets.len() != logical_len.wrapping_add(1) {
                    return Err(GeometryErrorKind::message(
                        "polygon logical offsets length does not match validity",
                    ));
                }
                (
                    GeometryArrayStorage::Packed(PackedStorage::Polygons {
                        coords: Arc::new(coords),
                        ring_offsets,
                        polygon_offsets,
                        row_map: storage::RowSelection::Identity,
                    }),
                    StorageLayout::LogicalRows,
                )
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
            Arc::new(storage),
            frame,
            missing,
            layout,
            ComputedCaches::fresh(),
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
        admit_offset_rows_capacity(row_capacity)?;
        let offsets = CsrOffsetBuilder::with_capacity(row_capacity);
        let mut out = Self {
            coords: crate::geometry::CoordSeqBuilder::with_capacity(axes, coordinate_capacity),
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
        let output_end = admit_packed_total(self.coords.len(), window.len())?;
        self.coords.extend_window(coords, window);
        self.offsets.push_end(output_end, i32::MAX as usize)
    }

    fn try_extend_admitted(
        &mut self,
        coords: &CoordSeq,
        offsets: &[i32],
        rows: std::ops::Range<usize>,
    ) -> crate::error::Result<()> {
        let row_count = rows.len();
        let window = checked_csr_window(offsets, rows, coords.len())?;
        self.offsets.reserve_rows(row_count);
        let output_start = self.coords.len();
        self.coords
            .extend_window(coords, window.source_start()..window.source_end());
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

    fn into_present_shapes(
        self,
        validity: &ValidityBuilder,
        capacity: usize,
    ) -> crate::error::Result<Vec<Shape>> {
        let (coords, offsets) = self.into_parts()?;
        let capacity = shape_vec_capacity(validity.present_count(), capacity)?;
        let mut shapes = Vec::with_capacity(capacity);
        for (logical, [start, end]) in offsets.array_windows::<2>().enumerate() {
            if !validity.is_present(logical) {
                continue;
            }
            let row = coords.view(CoordWindow::from_range(
                *start as usize..*end as usize,
                coords.len(),
            ));
            shapes.push(if row.is_empty() {
                Shape::typed_empty(GeometryKind::LineString, coords.axes())
            } else {
                Shape::LineString(LineSeq::from_proven(row))
            });
        }
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
        admit_offset_rows_capacity(ring_capacity)?;
        admit_offset_rows_capacity(row_capacity)?;
        let mut out = Self {
            coords: crate::geometry::CoordSeqBuilder::with_capacity(axes, coordinate_capacity),
            ring_offsets: CsrOffsetBuilder::with_capacity(ring_capacity),
            polygon_offsets: CsrOffsetBuilder::with_capacity(row_capacity),
        };
        for _ in 0..leading_missing {
            out.try_push_missing()?;
        }
        Ok(out)
    }

    fn try_push(&mut self, polygon: &Polygon) -> crate::error::Result<()> {
        let mut output_vertices = self.coords.len();
        let mut output_rings = self.ring_offsets.row_count();
        for ring in polygon.rings() {
            output_vertices = admit_packed_total(output_vertices, ring.len())?;
            output_rings = admit_packed_total(output_rings, 1)?;
        }
        for ring in polygon.rings() {
            self.coords.extend_window(ring, 0..ring.len());
            self.ring_offsets
                .push_end(self.coords.len(), i32::MAX as usize)?;
        }
        self.polygon_offsets
            .push_end(output_rings, i32::MAX as usize)
    }

    fn try_push_spans(
        &mut self,
        coords: &CoordSeq,
        ring_offsets: &[i32],
        rings: std::ops::Range<usize>,
    ) -> crate::error::Result<()> {
        let output_end = admit_packed_total(
            self.coords.len(),
            csr_value_window(ring_offsets, rings.clone()).len(),
        )?;
        let output_rings = admit_packed_total(self.ring_offsets.row_count(), rings.len())?;
        for ring in rings {
            let window = RowSelectionRef::Identity.csr_window(ring_offsets, ring);
            self.coords.extend_window(coords, window);
            self.ring_offsets
                .push_end(self.coords.len(), i32::MAX as usize)?;
        }
        debug_assert_eq!(self.coords.len(), output_end);
        self.polygon_offsets
            .push_end(output_rings, i32::MAX as usize)
    }

    fn try_extend_admitted(
        &mut self,
        coords: &CoordSeq,
        ring_offsets: &[i32],
        polygon_offsets: &[i32],
        rows: std::ops::Range<usize>,
    ) -> crate::error::Result<()> {
        let row_count = rows.len();
        let polygon_window =
            checked_csr_window(polygon_offsets, rows, ring_offsets.len().saturating_sub(1))?;
        let ring_window = checked_csr_window(
            ring_offsets,
            polygon_window.source_start()..polygon_window.source_end(),
            coords.len(),
        )?;
        let source_rings = polygon_window.source_start()..polygon_window.source_end();
        self.ring_offsets.reserve_rows(source_rings.len());
        self.polygon_offsets.reserve_rows(row_count);
        let output_coordinate_start = self.coords.len();
        let output_ring_start = self.ring_offsets.row_count();
        self.coords
            .extend_window(coords, ring_window.source_start()..ring_window.source_end());
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

    fn into_present_shapes(
        self,
        validity: &ValidityBuilder,
        capacity: usize,
    ) -> crate::error::Result<Vec<Shape>> {
        let (coords, ring_offsets, polygon_offsets) = self.into_parts()?;
        let capacity = shape_vec_capacity(validity.present_count(), capacity)?;
        let mut shapes = Vec::with_capacity(capacity);
        for logical in 0..polygon_offsets.len().saturating_sub(1) {
            if !validity.is_present(logical) {
                continue;
            }
            shapes.push(
                PackedRow::polygon(
                    logical,
                    &coords,
                    &ring_offsets,
                    &polygon_offsets,
                    RowSelectionRef::Identity,
                )
                .to_shape()?,
            );
        }
        Ok(shapes)
    }
}

#[cfg(test)]
mod demotion_tests {
    use super::*;
    use crate::geometry::{Point, Ring};

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
    fn packed_point_stream_checks_shape_layout_only_when_demoting() {
        let capacity = isize::MAX as usize;
        let mut points = StreamingShapes::with_capacity(0, capacity);
        points
            .try_push(Shape::Point(Point::new_raw_xy(1.0, 2.0)))
            .unwrap();
        assert!(matches!(&points.state, StreamingState::Points(_)));
        let points = points.finish(Frame::None).unwrap();
        assert!(matches!(
            points.storage(),
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) if coords.len() == 1
        ));

        let mut mixed = StreamingShapes::with_capacity(0, capacity);
        mixed
            .try_push(Shape::Point(Point::new_raw_xy(1.0, 2.0)))
            .unwrap();
        let error = mixed
            .try_push(xy_line(&[(0.0, 0.0), (1.0, 1.0)]))
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
    }

    #[test]
    fn packed_line_and_polygon_offset_layouts_fail_before_reservation() {
        let capacity = isize::MAX as usize;
        let mut lines = StreamingShapes::with_capacity(0, capacity);
        let error = lines
            .try_push(xy_line(&[(0.0, 0.0), (1.0, 1.0)]))
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));

        let mut polygons = StreamingShapes::with_capacity(0, capacity);
        let error = polygons.try_push(xy_square()).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
    }

    #[test]
    fn line_and_polygon_shape_demotions_admit_present_row_capacity() {
        let capacity = isize::MAX as usize;
        let mut validity = ValidityBuilder::new();
        validity.push(true);

        let Shape::LineString(line) = xy_line(&[(0.0, 0.0), (1.0, 1.0)]) else {
            unreachable!()
        };
        let mut lines = StreamingLines::try_new(line.axes(), line.len(), 1, 0).unwrap();
        lines
            .try_push_span(line.as_coords(), 0..line.len())
            .unwrap();
        let error = lines.into_present_shapes(&validity, capacity).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));

        let Shape::Polygon(polygon) = xy_square() else {
            unreachable!()
        };
        let mut polygons = StreamingPolygons::try_new(polygon.axes(), 5, 1, 1, 0).unwrap();
        polygons.try_push(&polygon).unwrap();
        let error = polygons
            .into_present_shapes(&validity, capacity)
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
    }

    #[test]
    fn empty_to_mixed_demotion_checks_shape_layout() {
        let mut rows = StreamingShapes::with_capacity(0, isize::MAX as usize);
        let error = rows
            .try_push(Shape::typed_empty(
                GeometryKind::MultiPoint,
                CoordinateAxes::XY,
            ))
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
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

        storage::reset_shape_iter_materializations();
        let staged = source.storage().iter_shapes().count();
        assert_eq!(staged, 3);
        assert!(
            storage::shape_iter_materializations() > 0,
            "the staged negative control must exercise shape materialization"
        );

        storage::reset_shape_iter_materializations();
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
            storage::shape_iter_materializations(),
            0,
            "native row append must not stage Shape values"
        );
    }
}
