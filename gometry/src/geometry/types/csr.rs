use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::Result;
use crate::geometry::types::{EmptyKind, GeometryErrorKind, Shape};

/// The seven geometry kinds, as a `Copy` discriminant of [`Shape`]. Used to
/// pick the typed Python subclass for a returned geometry without cloning the
/// shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GeometryKind {
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    GeometryCollection,
}

impl GeometryKind {
    pub(crate) const fn of(shape: &Shape) -> Self {
        match shape {
            Shape::Point(_) | Shape::Empty(EmptyKind::Point, _) => Self::Point,
            Shape::MultiPoint(_) => Self::MultiPoint,
            Shape::LineString(_) => Self::LineString,
            Shape::MultiLineString(_) | Shape::Empty(EmptyKind::MultiLineString, _) => {
                Self::MultiLineString
            },
            Shape::Polygon(_) | Shape::Empty(EmptyKind::Polygon, _) => Self::Polygon,
            Shape::MultiPolygon(_) | Shape::Empty(EmptyKind::MultiPolygon, _) => Self::MultiPolygon,
            Shape::GeometryCollection(_) | Shape::Empty(EmptyKind::GeometryCollection, _) => {
                Self::GeometryCollection
            },
        }
    }
}

/// `GeoArrow` list-array vertex offset — `repr(transparent)` over `i32` (0 is
/// valid).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct CsrOffset(i32);

impl CsrOffset {
    fn try_from_vertex_index(index: usize) -> Result<Self> {
        Ok(Self(
            i32::try_from(index).map_err(|_| GeometryErrorKind::OffsetCapacityExceeded)?,
        ))
    }
}

const _: () = {
    assert!(std::mem::size_of::<CsrOffset>() == std::mem::size_of::<i32>());
    assert!(std::mem::align_of::<CsrOffset>() == std::mem::align_of::<i32>());
};

// SAFETY: `CsrOffset` is `repr(transparent)` over `i32` with no extra fields.
unsafe impl bytemuck::TransparentWrapper<i32> for CsrOffset {}

/// Reinterpret `Arc<[CsrOffset]>` as `Arc<[i32]>` without copying.
///
/// # Safety invariant
///
/// `CsrOffset` is `repr(transparent)` over `i32` with identical size and
/// alignment (asserted above).
fn arc_i32_from_csr_offsets(arc: Arc<[CsrOffset]>) -> Arc<[i32]> {
    let raw = Arc::into_raw(arc);
    // SAFETY: transparent layout over `i32` with matching size/alignment.
    unsafe { Arc::from_raw(raw as *const [i32]) }
}

/// Reinterpret `Arc<[i32]>` as `Arc<[CsrOffset]>` without copying.
///
/// # Safety invariant
///
/// `CsrOffset` is `repr(transparent)` over `i32` with identical size and
/// alignment (asserted above).
fn arc_csr_offsets_from_i32(arc: Arc<[i32]>) -> Arc<[CsrOffset]> {
    let raw = Arc::into_raw(arc);
    // SAFETY: transparent layout over `i32` with matching size/align.
    unsafe { Arc::from_raw(raw as *const [CsrOffset]) }
}

/// Amortized CSR offset column builder for hot mutators (simplify, densify, …).
pub(crate) struct CsrOffsetBuilder {
    ends: Vec<usize>,
}

impl Default for CsrOffsetBuilder {
    fn default() -> Self {
        Self { ends: vec![0] }
    }
}

impl CsrOffsetBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Append the end offset of the next row; `vertex_index` must not exceed
    /// `vertex_cap`.
    pub(crate) fn push_end(&mut self, vertex_index: usize, vertex_cap: usize) -> Result<()> {
        if vertex_index > vertex_cap {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        }
        let last = *self
            .ends
            .last()
            .expect("CSR builder starts with a zero offset");
        if vertex_index < last {
            return Err(GeometryErrorKind::MalformedCsrOffsets.into());
        }
        self.ends.push(vertex_index);
        Ok(())
    }

    pub(crate) fn finish(self, vertex_cap: usize) -> Result<CsrOffsetColumn> {
        CsrOffsetColumn::from_builder_ends(self.ends, vertex_cap)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingLevel;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PolygonLevel;

/// Validated frozen CSR offset column for packed lineal / polygonal storage.
#[derive(Clone, Debug)]
pub struct CsrOffsetColumn<Level = ()> {
    offsets: Arc<[CsrOffset]>,
    _level: PhantomData<Level>,
}

impl<Level> std::ops::Deref for CsrOffsetColumn<Level> {
    type Target = [i32];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<Level> CsrOffsetColumn<Level> {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Borrow the offset column as `i32` CSR ends — zero-cost
    /// `repr(transparent)` view.
    pub fn as_slice(&self) -> &[i32] {
        bytemuck::TransparentWrapper::peel_slice(&self.offsets)
    }
    /// Single O(n) validation at freeze: monotonic, starts at 0, last <=
    /// `end_cap`.
    ///
    /// `end_cap` is the maximum allowed value of the last CSR end: vertex
    /// count for line/`ring_offsets` columns, ring count for `polygon_offsets`.
    pub fn try_new(ends: Vec<usize>, end_cap: usize) -> Result<Self> {
        validate_csr_ends(&ends, end_cap)?;
        let offsets: Vec<CsrOffset> = ends
            .into_iter()
            .map(CsrOffset::try_from_vertex_index)
            .collect::<Result<_>>()?;
        Ok(Self {
            offsets: offsets.into(),
            _level: PhantomData,
        })
    }

    /// Trusted finalizer for `CsrOffsetBuilder`: `push_end` already guarantees
    /// `ends[0]==0` and monotonic non-decreasing ends, so only the O(1)
    /// `last <= end_cap` bound (offsets index coord columns — a safety
    /// boundary) is re-checked here; the O(n) monotonicity re-walk of
    /// `validate_csr_ends` is skipped. Untrusted paths (`try_new`,
    /// `try_from_arc_i32`, `rebase_concat`) keep full validation.
    pub(crate) fn from_builder_ends(ends: Vec<usize>, end_cap: usize) -> Result<Self> {
        if ends.last().is_some_and(|&last| last > end_cap) {
            return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
        }
        let offsets: Vec<CsrOffset> = ends
            .into_iter()
            .map(CsrOffset::try_from_vertex_index)
            .collect::<Result<_>>()?;
        Ok(Self {
            offsets: offsets.into(),
            _level: PhantomData,
        })
    }

    /// Concatenate a validated prefix CSR column with a tail column, rebasing
    /// tail ends (skipping its leading zero) by the prefix's last end — single
    /// O(n) `usize` pass, one i32 conversion at freeze.
    pub fn rebase_concat(prefix: &[i32], tail: &[i32], combined_vertex_cap: usize) -> Result<Self> {
        let base = *prefix
            .last()
            .ok_or(GeometryErrorKind::MalformedCsrOffsets)? as usize;
        let mut ends: Vec<usize> = prefix.iter().map(|&offset| offset as usize).collect();
        for &offset in tail.iter().skip(1) {
            ends.push(
                base.checked_add(offset as usize)
                    .ok_or(GeometryErrorKind::OffsetCapacityExceeded)?,
            );
        }
        Self::try_new(ends, combined_vertex_cap)
    }

    /// Fast-path concat of two CSR columns: one exact `i32` allocation, prefix
    /// copy, one rebase pass over the tail (skipping its leading zero).
    ///
    /// Every addition is **checked** — a safe caller cannot drive overflow into
    /// `unchecked_add` UB by supplying naked non-monotonic or near-`i32::MAX`
    /// slices. Monotonicity of the rebased tail is also checked so a non-CSR
    /// input cannot produce a column that only *looks* valid at the ends.
    pub fn rebase_concat_trusted(prefix: &[i32], tail: &[i32], end_cap: usize) -> Result<Self> {
        if prefix.first().copied() != Some(0) {
            return Err(GeometryErrorKind::MalformedCsrOffsets.into());
        }
        let base = *prefix
            .last()
            .ok_or(GeometryErrorKind::MalformedCsrOffsets)?;
        if base < 0 || (base as usize) > end_cap {
            return Err(GeometryErrorKind::MalformedCsrOffsets.into());
        }
        let tail_rest = tail.get(1..).unwrap_or(&[]);
        let len = prefix.len() + tail_rest.len();
        // Fallible i32 materialization: checked_add per tail end so intermediate
        // non-monotonic / overflowing values cannot hit `unchecked_add` UB.
        let mut out = Vec::with_capacity(len);
        out.extend_from_slice(prefix);
        let mut prev = base;
        for &offset in tail_rest {
            let rebased = base
                .checked_add(offset)
                .ok_or(GeometryErrorKind::OffsetCapacityExceeded)?;
            if rebased < prev {
                return Err(GeometryErrorKind::MalformedCsrOffsets.into());
            }
            if (rebased as usize) > end_cap {
                return Err(GeometryErrorKind::OffsetCapacityExceeded.into());
            }
            out.push(rebased);
            prev = rebased;
        }
        Ok(Self {
            offsets: arc_csr_offsets_from_i32(Arc::from(out)),
            _level: PhantomData,
        })
    }

    /// Zero-copy reinterpret for `GeoArrow` export (`Arc<[i32]>` shares the
    /// allocation).
    pub fn as_arc_i32(&self) -> Arc<[i32]> {
        arc_i32_from_csr_offsets(Arc::clone(&self.offsets))
    }

    /// Validate a foreign-owned `i32` offset buffer and wrap without copying.
    ///
    /// `end_cap` is the maximum allowed value of the last CSR end (see
    /// [`Self::try_new`]).
    pub fn try_from_arc_i32(arc: Arc<[i32]>, end_cap: usize) -> Result<Self> {
        validate_csr_offsets_i32(&arc, end_cap)?;
        Ok(Self {
            offsets: arc_csr_offsets_from_i32(arc),
            _level: PhantomData,
        })
    }

    pub fn cast_level<Next>(self) -> CsrOffsetColumn<Next> {
        CsrOffsetColumn {
            offsets: self.offsets,
            _level: PhantomData,
        }
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
