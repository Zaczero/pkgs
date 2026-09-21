use std::marker::PhantomData;

use crate::HeapSize as _;
use crate::error::Result;
use crate::geometry::types::{GeometryErrorKind, Shape, SharedSlice};

/// The seven geometry kinds, as a `Copy` discriminant of [`Shape`]. Used to
/// pick the typed Python subclass for a returned geometry without cloning the
/// shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum GeometryKind {
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    GeometryCollection,
}

impl GeometryKind {
    pub const fn of(shape: &Shape) -> Self {
        match shape {
            Shape::Point(_) | Shape::Empty(Self::Point, _) => Self::Point,
            Shape::MultiPoint(_) | Shape::Empty(Self::MultiPoint, _) => Self::MultiPoint,
            Shape::LineString(_) | Shape::Empty(Self::LineString, _) => Self::LineString,
            Shape::MultiLineString(_) | Shape::Empty(Self::MultiLineString, _) => {
                Self::MultiLineString
            },
            Shape::Polygon(_) | Shape::Empty(Self::Polygon, _) => Self::Polygon,
            Shape::MultiPolygon(_) | Shape::Empty(Self::MultiPolygon, _) => Self::MultiPolygon,
            Shape::GeometryCollection(_) | Shape::Empty(Self::GeometryCollection, _) => {
                Self::GeometryCollection
            },
        }
    }

    pub const fn geometry_type(self) -> &'static str {
        match self {
            Self::Point => "Point",
            Self::MultiPoint => "MultiPoint",
            Self::LineString => "LineString",
            Self::MultiLineString => "MultiLineString",
            Self::Polygon => "Polygon",
            Self::MultiPolygon => "MultiPolygon",
            Self::GeometryCollection => "GeometryCollection",
        }
    }

    pub const fn topological_dimension(self) -> crate::geometry::Dimension {
        match self {
            Self::Point | Self::MultiPoint | Self::GeometryCollection => {
                crate::geometry::Dimension::Point
            },
            Self::LineString | Self::MultiLineString => crate::geometry::Dimension::Curve,
            Self::Polygon | Self::MultiPolygon => crate::geometry::Dimension::Surface,
        }
    }
}

/// Amortized CSR offset column builder for hot mutators (simplify, densify, …).
pub(crate) struct CsrOffsetBuilder {
    builder: crate::boundary::CsrBuilder<()>,
}

impl Default for CsrOffsetBuilder {
    fn default() -> Self {
        Self {
            builder: crate::boundary::CsrBuilder::new(),
        }
    }
}

impl CsrOffsetBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_capacity(rows: usize) -> Result<Self> {
        Ok(Self {
            builder: crate::boundary::CsrBuilder::with_capacity(0, rows)?,
        })
    }

    /// Append the end offset of the next row; `vertex_index` must not exceed
    /// `vertex_cap`.
    pub(crate) fn push_end(&mut self, vertex_index: usize, vertex_cap: usize) -> Result<()> {
        if vertex_index > vertex_cap {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        }
        self.builder.push_end(vertex_index)
    }

    /// Append a prevalidated contiguous CSR window rebased into the current
    /// output coordinate space. Only its closing ends are appended.
    pub(crate) fn extend_rebased(
        &mut self,
        window: CheckedCsrWindow<'_>,
        output_base: usize,
    ) -> Result<()> {
        if window.start_row == window.end_row {
            return Ok(());
        }
        let start = window
            .start_row
            .checked_add(1)
            .ok_or(GeometryErrorKind::MalformedCsrOffsets)?;
        let Some(ends) = window.source.get(start..=window.end_row) else {
            return Err(GeometryErrorKind::MalformedCsrOffsets.into());
        };
        for &raw_end in ends {
            let end = raw_end as usize;
            let relative = end - window.source_start;
            let rebased = output_base
                .checked_add(relative)
                .ok_or(GeometryErrorKind::OffsetCapacityExceeded)?;
            self.push_end(rebased, i32::MAX as usize)?;
        }
        Ok(())
    }

    pub(crate) const fn row_count(&self) -> usize {
        self.builder.row_count()
    }

    pub(crate) fn finish(self, vertex_cap: usize) -> Result<CsrOffsetColumn> {
        let (_, offsets) = self.builder.finish();
        if offsets
            .last()
            .is_some_and(|&last| last as usize > vertex_cap)
        {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        }
        let mut values = Vec::new();
        crate::boundary::try_reserve_vec(&mut values, offsets.len())?;
        for offset in offsets {
            crate::boundary::try_push_vec(
                &mut values,
                i32::try_from(offset).map_err(|_| GeometryErrorKind::OffsetCapacityExceeded)?,
            )?;
        }
        Ok(CsrOffsetColumn {
            offsets: SharedSlice::from_vec(values),
            _level: PhantomData,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RingLevel;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PolygonLevel;

/// Validated frozen CSR offset column for packed lineal / polygonal storage.
#[derive(Clone, Debug)]
pub(crate) struct CsrOffsetColumn<Level = ()> {
    offsets: SharedSlice<i32>,
    _level: PhantomData<Level>,
}

impl<Level> std::ops::Deref for CsrOffsetColumn<Level> {
    type Target = [i32];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<Level> crate::HeapSize for CsrOffsetColumn<Level> {
    fn heap_bytes(&self) -> usize {
        self.offsets.heap_bytes()
    }
}

impl<Level> CsrOffsetColumn<Level> {
    /// Borrow the validated `i32` CSR ends.
    pub(crate) fn as_slice(&self) -> &[i32] {
        &self.offsets
    }
    /// Single O(n) validation at freeze: monotonic, starts at 0, last <=
    /// `end_cap`.
    ///
    /// `end_cap` is the maximum allowed value of the last CSR end: vertex
    /// count for line/`ring_offsets` columns, ring count for `polygon_offsets`.
    pub(crate) fn try_new(ends: Vec<usize>, end_cap: usize) -> Result<Self> {
        validate_csr_ends(&ends, end_cap)?;
        let offsets = offsets_from_ends(ends)?;
        Ok(Self {
            offsets,
            _level: PhantomData,
        })
    }

    /// Share the primitive offset owner for `GeoArrow` export.
    pub(crate) fn shared_offsets(&self) -> SharedSlice<i32> {
        self.offsets.clone()
    }

    /// Validate a foreign-owned `i32` offset buffer and wrap without copying.
    ///
    /// `end_cap` is the maximum allowed value of the last CSR end (see
    /// [`Self::try_new`]).
    pub(crate) fn try_from_shared_i32(offsets: SharedSlice<i32>, end_cap: usize) -> Result<Self> {
        validate_csr_offsets_i32(&offsets, end_cap)?;
        Ok(Self {
            offsets,
            _level: PhantomData,
        })
    }

    pub(crate) fn cast_level<Next>(self) -> CsrOffsetColumn<Next> {
        CsrOffsetColumn {
            offsets: self.offsets,
            _level: PhantomData,
        }
    }

    pub(crate) fn logical_heap_bytes_for_count(&self, count: usize) -> usize {
        if count == self.len() {
            self.offsets.heap_bytes()
        } else if count == 0 {
            0
        } else {
            crate::heap_size::ARC_CONTROL_BYTES
                + std::mem::size_of::<Vec<i32>>()
                + count * std::mem::size_of::<i32>()
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CheckedCsrWindow<'a> {
    source: &'a [i32],
    start_row: usize,
    end_row: usize,
    source_start: usize,
    source_end: usize,
}

impl CheckedCsrWindow<'_> {
    pub(crate) const fn source_start(self) -> usize {
        self.source_start
    }

    pub(crate) const fn source_end(self) -> usize {
        self.source_end
    }
}

/// Validate every source boundary in a contiguous CSR row range before any
/// caller indexes or casts its endpoints.
pub(crate) fn checked_csr_window(
    source: &[i32],
    rows: std::ops::Range<usize>,
    value_cap: usize,
) -> Result<CheckedCsrWindow<'_>> {
    if rows.start > rows.end {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    }
    let Some(values) = source.get(rows.start..=rows.end) else {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    };
    let mut previous = None;
    for &raw in values {
        if raw < 0 {
            return Err(GeometryErrorKind::MalformedCsrOffsets.into());
        }
        let value = raw as usize;
        if value > value_cap {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        }
        if previous.is_some_and(|previous| value < previous) {
            return Err(GeometryErrorKind::MalformedCsrOffsets.into());
        }
        previous = Some(value);
    }
    let source_start = values
        .first()
        .copied()
        .ok_or(GeometryErrorKind::MalformedCsrOffsets)?;
    let source_end = values
        .last()
        .copied()
        .ok_or(GeometryErrorKind::MalformedCsrOffsets)?;
    if rows.start == 0 && source_start != 0 {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    }
    Ok(CheckedCsrWindow {
        source,
        start_row: rows.start,
        end_row: rows.end,
        source_start: source_start as usize,
        source_end: source_end as usize,
    })
}

fn validate_csr_offsets_i32(offsets: &[i32], end_cap: usize) -> Result<()> {
    let Some(&first) = offsets.first() else {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    };
    if first != 0 {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    }
    let Some(&last) = offsets.last() else {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    };
    if last < 0 {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    }
    if last as usize > end_cap {
        return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
    }
    if !offsets.is_sorted() {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    }
    Ok(())
}

fn offsets_from_ends(ends: Vec<usize>) -> Result<SharedSlice<i32>> {
    let mut offsets = Vec::new();
    crate::boundary::try_reserve_vec(&mut offsets, ends.len())?;
    for end in ends {
        crate::boundary::try_push_vec(
            &mut offsets,
            i32::try_from(end).map_err(|_| GeometryErrorKind::OffsetCapacityExceeded)?,
        )?;
    }
    Ok(SharedSlice::from_vec(offsets))
}

fn validate_csr_ends(ends: &[usize], vertex_cap: usize) -> Result<()> {
    let Some(&first) = ends.first() else {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    };
    if first != 0 {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    }
    let Some(&last) = ends.last() else {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    };
    if last > vertex_cap {
        return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
    }
    if !ends.is_sorted() {
        return Err(GeometryErrorKind::MalformedCsrOffsets.into());
    }
    Ok(())
}

/// Release-checked vertex-column capacity for CSR-derived packed storage.
pub(crate) fn ensure_coordseq_vertex_capacity(len: usize) -> Result<()> {
    if len > i32::MAX as usize {
        return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::checked_csr_window;

    #[test]
    fn checked_csr_window_rejects_malformed_boundaries_without_panicking() {
        for (offsets, rows, cap) in [
            (&[0, -1, 2][..], 0..2, 2),
            (&[0, 2, 1][..], 0..2, 2),
            (&[0, 2][..], 0..3, 2),
            (&[0, 3][..], 0..1, 2),
            (&[1, 2][..], 0..1, 2),
            (&[0, 2][..], std::ops::Range { start: 2, end: 1 }, 2),
            (&[][..], 0..0, 0),
        ] {
            let result = std::panic::catch_unwind(|| checked_csr_window(offsets, rows, cap));
            assert!(result.is_ok(), "malformed CSR boundary panicked");
            assert!(
                result.unwrap().is_err(),
                "malformed CSR boundary was accepted"
            );
        }
    }
}
