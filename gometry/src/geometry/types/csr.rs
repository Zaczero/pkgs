use std::marker::PhantomData;
use std::ops::Range;

use crate::collections::csr;
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
    builder: csr::CsrBuilder<(), i32>,
}

impl Default for CsrOffsetBuilder {
    fn default() -> Self {
        Self {
            builder: csr::CsrBuilder::new(),
        }
    }
}

impl CsrOffsetBuilder {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_capacity(rows: usize) -> Self {
        Self {
            builder: csr::CsrBuilder::with_capacity(0, rows),
        }
    }

    pub(crate) fn reserve_rows(&mut self, additional: usize) {
        self.builder.reserve_rows(additional);
    }

    /// Append the end offset of the next row; `vertex_index` must not exceed
    /// `vertex_cap`.
    pub(crate) fn push_end(&mut self, vertex_index: usize, vertex_cap: usize) -> Result<()> {
        if vertex_index > vertex_cap {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        }
        self.builder.push_end(vertex_index)
    }

    /// Append ends from a source window whose rebased final end was admitted.
    pub(crate) fn extend_rebased<Level>(&mut self, window: RebasedCsrWindow<'_, Level>) {
        let source_start = window.window.source_start;
        let output_base = window.output_base;
        let ends = window.window.closing_offsets().iter().map(|&raw_end| {
            let relative = (raw_end as usize).wrapping_sub(source_start);
            output_base.wrapping_add(relative) as i32
        });
        self.builder.extend_admitted_i32_ends(ends);
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
        Ok(CsrOffsetColumn {
            offsets: SharedSlice::from_vec(offsets),
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
        self.offsets.shallow_heap_bytes()
    }
}

impl<Level> CsrOffsetColumn<Level> {
    /// Borrow the validated `i32` CSR ends.
    pub(crate) fn as_slice(&self) -> &[i32] {
        &self.offsets
    }

    /// Select contiguous rows from this admitted offset owner.
    ///
    /// The borrowed owner retains the monotonicity, zero origin, and parent
    /// value bounds established when this column was constructed.
    pub(crate) fn window(&self, rows: Range<usize>) -> Result<CsrOffsetWindow<'_, Level>> {
        if rows.start > rows.end {
            return Err(GeometryErrorKind::MalformedCsrOffsets.into());
        }
        let Some(boundaries) = self.as_slice().get(rows.start..=rows.end) else {
            return Err(GeometryErrorKind::MalformedCsrOffsets.into());
        };
        let Some(&source_start) = boundaries.first() else {
            return Err(GeometryErrorKind::MalformedCsrOffsets.into());
        };
        let Some(&source_end) = boundaries.last() else {
            return Err(GeometryErrorKind::MalformedCsrOffsets.into());
        };
        Ok(CsrOffsetWindow {
            owner: self,
            row_start: rows.start,
            row_end: rows.end,
            source_start: source_start as usize,
            source_end: source_end as usize,
        })
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
            self.offsets.shallow_heap_bytes()
        } else if count == 0 {
            0
        } else {
            crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(std::mem::size_of::<Vec<i32>>())
                .saturating_add(count.saturating_mul(std::mem::size_of::<i32>()))
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CsrOffsetWindow<'a, Level> {
    owner: &'a CsrOffsetColumn<Level>,
    row_start: usize,
    row_end: usize,
    source_start: usize,
    source_end: usize,
}

impl<'a, Level> CsrOffsetWindow<'a, Level> {
    pub(crate) const fn row_count(self) -> usize {
        self.row_end.wrapping_sub(self.row_start)
    }

    /// Values addressed by this level's parent CSR column.
    pub(crate) const fn values(self) -> Range<usize> {
        self.source_start..self.source_end
    }

    /// Admit the destination's final offset before any parallel payload is
    /// appended. Every intermediate rebased end is then representable too.
    pub(crate) fn rebase(self, output_base: usize) -> Result<RebasedCsrWindow<'a, Level>> {
        let output_end = output_base
            .checked_add(self.source_end.wrapping_sub(self.source_start))
            .ok_or(GeometryErrorKind::OffsetCapacityExceeded)?;
        i32::try_from(output_end).map_err(|_| GeometryErrorKind::OffsetCapacityExceeded)?;
        Ok(RebasedCsrWindow {
            window: self,
            output_base,
        })
    }

    fn closing_offsets(self) -> &'a [i32] {
        if self.row_start == self.row_end {
            return &[];
        }
        self.owner
            .as_slice()
            .get(self.row_start.wrapping_add(1)..=self.row_end)
            .expect("CSR row window belongs to its admitted offset owner")
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RebasedCsrWindow<'a, Level> {
    window: CsrOffsetWindow<'a, Level>,
    output_base: usize,
}

impl<Level> RebasedCsrWindow<'_, Level> {
    pub(crate) const fn row_count(self) -> usize {
        self.window.row_count()
    }

    pub(crate) const fn values(self) -> Range<usize> {
        self.window.values()
    }
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
    let mut offsets = Vec::with_capacity(ends.len());
    for end in ends {
        offsets.push(i32::try_from(end).map_err(|_| GeometryErrorKind::OffsetCapacityExceeded)?);
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
    use super::{CsrOffsetBuilder, CsrOffsetColumn, GeometryErrorKind, PolygonLevel, RingLevel};
    use crate::error::ErrorKind;

    #[test]
    fn packed_offsets_admit_their_stored_width_and_preserve_error_kinds() {
        let maximum = i32::MAX as usize;
        let mut builder = CsrOffsetBuilder::new();
        builder.push_end(maximum, maximum).unwrap();
        let column = builder.finish(maximum).unwrap();
        assert_eq!(column.as_slice(), &[0, i32::MAX]);

        let mut builder = CsrOffsetBuilder::new();
        let error = builder
            .push_end(maximum.wrapping_add(1), usize::MAX)
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));
        builder.push_end(1, 1).unwrap();
        let error = builder.push_end(0, 1).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::MalformedCsrOffsets)
        ));
        builder.push_end(0, 1).unwrap();
        builder.push_end(1, 1).unwrap();
        assert_eq!(builder.finish(1).unwrap().as_slice(), &[0, 0, 1]);

        let mut builder = CsrOffsetBuilder::new();
        let error = builder.push_end(2, 1).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));
    }

    #[test]
    fn packed_offset_finish_allocates_only_the_shared_owner() {
        for rows in [0, 3, 1024] {
            let mut builder = CsrOffsetBuilder::with_capacity(rows);
            for end in 1..=rows {
                builder.push_end(end, rows).unwrap();
            }
            let (column, allocations) =
                crate::test_support::allocation_count(|| builder.finish(rows));
            let column = column.unwrap();
            assert_eq!(allocations, 1);
            assert_eq!(column.len(), rows.wrapping_add(1));
            assert!(column.iter().copied().eq(0..=rows as i32));
        }
    }

    #[test]
    fn typed_csr_windows_keep_parent_ranges_and_rebase_from_nonzero_bases() {
        let polygon_offsets =
            CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 2, 3], 3).expect("polygons");
        let polygon_window = polygon_offsets.window(1..2).expect("selected polygon");
        assert_eq!(polygon_window.values(), 2..3);

        let ring_offsets =
            CsrOffsetColumn::<RingLevel>::try_new(vec![0, 3, 7, 12], 12).expect("rings");
        let ring_window = ring_offsets
            .window(polygon_window.values())
            .expect("the selected polygon's ring range");
        assert_eq!(ring_window.values(), 7..12);

        let mut output = CsrOffsetBuilder::new();
        output.push_end(7, 12).expect("existing coordinate base");
        output.extend_rebased(ring_window.rebase(7).expect("rebased ring"));
        assert_eq!(output.finish(12).expect("finished offsets").as_slice(), &[
            0, 7, 12
        ]);
    }

    #[test]
    fn typed_csr_windows_check_row_ranges_and_destination_width() {
        let offsets = CsrOffsetColumn::<()>::try_new(vec![0, 1], 1).expect("offsets");
        let error = offsets
            .window(std::ops::Range { start: 1, end: 0 })
            .expect_err("reversed rows");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::MalformedCsrOffsets)
        ));
        let error = offsets.window(0..2).expect_err("past the terminal offset");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::MalformedCsrOffsets)
        ));

        let window = offsets.window(0..1).expect("valid row");
        let error = window
            .rebase(i32::MAX as usize)
            .expect_err("destination end exceeds i32");
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(GeometryErrorKind::OffsetCapacityExceeded)
        ));
    }
}
