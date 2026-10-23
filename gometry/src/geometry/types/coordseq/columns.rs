use super::{
    CoordSeq, GeometryErrorKind, Point, Range, Result, SharedSlice, VisiblePointRunKey,
    VisibleXYKey,
};

/// One sequence's shared column storage and coordinate row window (see
/// [`CoordSeq::column_arcs`]).
pub(crate) struct SharedColumns {
    pub xs: SharedSlice<f64>,
    pub ys: SharedSlice<f64>,
    pub zs: Option<SharedSlice<f64>>,
    pub ms: Option<MColumn>,
    pub window: Range<usize>,
}

impl SharedColumns {
    /// Return the M owner and its visible window in the same shape consumed by
    /// Arrow primitive-array builders.
    pub(crate) fn m_parts(&self) -> Option<(SharedSlice<f64>, Range<usize>)> {
        self.ms
            .as_ref()
            .map(|column| (column.owner(), column.window(self.window.clone())))
    }
}

/// M storage is either aligned with the shared X/Y/Z columns or has an
/// independent offset into a local owner. The offset advances when a nested
/// coordinate view is made; local views never copy their M payload.
#[derive(Debug)]
pub(crate) enum MColumn {
    Shared(SharedSlice<f64>),
    Local {
        owner: SharedSlice<f64>,
        offset: usize,
    },
}

impl Clone for MColumn {
    fn clone(&self) -> Self {
        match self {
            Self::Shared(owner) => Self::Shared(owner.clone()),
            Self::Local { owner, offset } => Self::Local {
                owner: owner.clone(),
                offset: *offset,
            },
        }
    }
}

impl MColumn {
    fn is_shared(&self) -> bool {
        match self {
            Self::Shared(owner) | Self::Local { owner, .. } => owner.is_shared(),
        }
    }

    fn owner(&self) -> SharedSlice<f64> {
        match self {
            Self::Shared(owner) | Self::Local { owner, .. } => owner.clone(),
        }
    }

    pub(crate) fn values(&self, range: Range<usize>) -> &[f64] {
        let window = self.window(range);
        match self {
            Self::Shared(owner) | Self::Local { owner, .. } => {
                owner.get(window).expect("admitted M window fits its owner")
            },
        }
    }

    const fn window(&self, range: Range<usize>) -> Range<usize> {
        match self {
            Self::Shared(_) => range,
            Self::Local { offset, .. } => {
                *offset..offset.wrapping_add(range.end.wrapping_sub(range.start))
            },
        }
    }

    pub(super) fn view(&self, window: Range<usize>) -> Self {
        match self {
            Self::Shared(owner) => Self::Shared(owner.clone()),
            Self::Local { owner, offset } => Self::Local {
                owner: owner.clone(),
                offset: offset.wrapping_add(window.start),
            },
        }
    }

    fn heap_bytes_saturating(&self) -> usize {
        match self {
            Self::Shared(owner) | Self::Local { owner, .. } => owner.shallow_heap_bytes(),
        }
    }

    fn logical_heap_bytes(&self, range: Range<usize>) -> usize {
        let window = self.window(range);
        match self {
            Self::Shared(owner) | Self::Local { owner, .. } => owner.logical_heap_bytes(window),
        }
    }

    fn is_full_owner(&self, physical_len: usize) -> bool {
        match self {
            Self::Shared(owner) => owner.len() == physical_len,
            Self::Local { owner, offset } => *offset == 0 && owner.len() == physical_len,
        }
    }
}

fn local_m_covers(owner: &SharedSlice<f64>, offset: usize, visible: usize) -> Result<()> {
    let Some(remaining) = owner.len().checked_sub(offset) else {
        return Err(GeometryErrorKind::CoordinateRange {
            start: offset,
            end: offset,
            length: owner.len(),
        }
        .into());
    };
    if visible > remaining {
        return Err(GeometryErrorKind::CoordinateRange {
            start: offset,
            end: offset.wrapping_add(visible),
            length: owner.len(),
        }
        .into());
    }
    Ok(())
}

pub(super) fn admit_m_column(
    ms: Option<&MColumn>,
    xy_physical: usize,
    visible: usize,
) -> Result<()> {
    match ms {
        None => Ok(()),
        Some(MColumn::Shared(owner)) => {
            if owner.len() == xy_physical {
                Ok(())
            } else {
                Err(GeometryErrorKind::CoordinateLength(xy_physical, owner.len()).into())
            }
        },
        Some(MColumn::Local { owner, offset }) => local_m_covers(owner, *offset, visible),
    }
}

pub(super) fn reversed_column(values: &[f64]) -> Vec<f64> {
    values.iter().rev().copied().collect()
}

impl crate::HeapSize for MColumn {
    fn heap_bytes(&self) -> usize {
        self.heap_bytes_saturating()
    }
}

impl crate::HeapSize for CoordSeq {
    fn heap_bytes(&self) -> usize {
        self.heap_bytes_saturating()
    }
}

impl CoordSeq {
    /// Identity of a visible XY window when its backing columns are shared.
    /// Empty runs have no point payload and need no owner key.
    pub(crate) fn shared_xy_key(&self) -> Option<VisibleXYKey> {
        if self.is_empty() || !(self.xs.is_shared() || self.ys.is_shared()) {
            return None;
        }
        Some(VisibleXYKey {
            x: self.xs().as_ptr().addr(),
            y: self.ys().as_ptr().addr(),
            len: self.len(),
        })
    }

    pub(crate) fn shared_z_key(&self) -> Option<(usize, usize)> {
        let owner = self.zs.as_ref()?;
        let values = self.zs()?;
        (owner.is_shared() && !values.is_empty()).then(|| (values.as_ptr().addr(), values.len()))
    }

    pub(crate) fn shared_m_key(&self) -> Option<(usize, usize)> {
        let owner = self.ms.as_ref()?;
        let values = self.ms()?;
        (owner.is_shared() && !values.is_empty()).then(|| (values.as_ptr().addr(), values.len()))
    }

    /// Identity of a visible full-ordinate window when any active column is
    /// shared. The M address comes from the visible slice so local offsets are
    /// part of the key.
    pub(crate) fn shared_point_run_key(&self) -> Option<VisiblePointRunKey> {
        if self.is_empty()
            || !(self.xs.is_shared()
                || self.ys.is_shared()
                || self.zs.as_ref().is_some_and(SharedSlice::is_shared)
                || self.ms.as_ref().is_some_and(MColumn::is_shared))
        {
            return None;
        }
        Some(VisiblePointRunKey {
            x: self.xs().as_ptr().addr(),
            y: self.ys().as_ptr().addr(),
            z: self.zs().map_or(0, |values| values.as_ptr().addr()),
            m: self.ms().map_or(0, |values| values.as_ptr().addr()),
            axes: self.axes().bits(),
            len: self.len(),
        })
    }

    pub(crate) fn heap_bytes_saturating(&self) -> usize {
        self.xs
            .shallow_heap_bytes()
            .saturating_add(self.ys.shallow_heap_bytes())
            .saturating_add(self.zs.as_ref().map_or(0, SharedSlice::shallow_heap_bytes))
            .saturating_add(self.ms.as_ref().map_or(0, MColumn::heap_bytes_saturating))
    }

    /// Fixed owner overhead for a logical view into these shared columns.
    /// Coordinate payload is supplied separately because a packed array can
    /// select fewer rows than the physical sequence owns.
    pub(crate) fn logical_owner_heap_bytes(&self) -> usize {
        let column = |present: bool| {
            if present {
                crate::heap_size::ARC_CONTROL_BYTES.wrapping_add(std::mem::size_of::<Vec<f64>>())
            } else {
                0
            }
        };
        column(!self.xs.is_empty())
            .saturating_add(column(!self.ys.is_empty()))
            .saturating_add(column(
                self.zs.as_ref().is_some_and(|column| !column.is_empty()),
            ))
            .saturating_add(column(self.ms.as_ref().is_some_and(
                |column| match column {
                    MColumn::Shared(owner) | MColumn::Local { owner, .. } => !owner.is_empty(),
                },
            )))
    }

    pub(crate) fn logical_heap_bytes_saturating(&self) -> usize {
        let range = self.range.start as usize..self.range.end as usize;
        self.xs
            .logical_heap_bytes(range.clone())
            .saturating_add(self.ys.logical_heap_bytes(range.clone()))
            .saturating_add(
                self.zs
                    .as_ref()
                    .map_or(0, |column| column.logical_heap_bytes(range.clone())),
            )
            .saturating_add(
                self.ms
                    .as_ref()
                    .map_or(0, |column| column.logical_heap_bytes(range)),
            )
    }

    /// Retained bytes for a packed coordinate owner exposing `count` rows.
    /// This keeps full capacity only when the sequence is the complete owner;
    /// selected/rebased views use their logical payload.
    pub(crate) fn logical_heap_bytes_for_count_saturating(&self, count: usize) -> usize {
        if count == self.len()
            && self.range.start == 0
            && self.range.end as usize == self.xs.len()
            && self
                .zs
                .as_ref()
                .is_none_or(|column| column.len() == self.xs.len())
            && self
                .ms
                .as_ref()
                .is_none_or(|column| column.is_full_owner(self.xs.len()))
        {
            self.heap_bytes_saturating()
        } else {
            self.logical_owner_heap_bytes()
                .saturating_add(count.saturating_mul(self.axes().byte_width(1)))
        }
    }

    /// The window of one shared column (every accessor routes through this).
    fn window<'a>(&self, column: &'a [f64]) -> &'a [f64] {
        column
            .get(self.range.start as usize..self.range.end as usize)
            .expect("coordinate window was admitted against its column")
    }

    /// The shared column storage + this sequence's windows — the zero-copy
    /// hand-off for buffer-protocol exports (Arrow coordinate buffers).
    pub(crate) fn column_arcs(&self) -> SharedColumns {
        SharedColumns {
            xs: self.xs.clone(),
            ys: self.ys.clone(),
            zs: self.zs.clone(),
            ms: self.ms.clone(),
            window: self.range.start as usize..self.range.end as usize,
        }
    }

    /// Raw ordinate-column payload in bytes (`f64` lanes actually stored).
    pub fn coordinate_bytes(&self) -> usize {
        self.axes().byte_width(self.len())
    }

    pub fn xs(&self) -> &[f64] {
        self.window(&self.xs)
    }

    pub fn ys(&self) -> &[f64] {
        self.window(&self.ys)
    }

    pub(crate) fn xy_shared_owner_count(&self) -> usize {
        self.xs.owner_count().min(self.ys.owner_count())
    }

    pub fn zs(&self) -> Option<&[f64]> {
        self.zs.as_deref().map(|column| self.window(column))
    }

    pub fn ms(&self) -> Option<&[f64]> {
        self.ms
            .as_ref()
            .map(|column| column.values(self.range.start as usize..self.range.end as usize))
    }

    /// Whether this sequence's row window spans the whole backing column.
    fn is_full_window(&self, column: &SharedSlice<f64>) -> bool {
        self.range.start == 0 && self.range.end as usize == column.len()
    }

    /// Carry one shared XY/Z column through an op that leaves it untouched:
    /// zero copy when this sequence spans the column's whole backing owner,
    /// otherwise a copy of the visible window.
    fn carried(&self, column: &SharedSlice<f64>) -> SharedSlice<f64> {
        if self.is_full_window(column) {
            column.clone()
        } else {
            SharedSlice::from_vec(self.window(column).to_vec())
        }
    }

    /// The X column carried with zero copy on the full-window common case.
    pub(crate) fn carried_xs(&self) -> SharedSlice<f64> {
        self.carried(&self.xs)
    }

    /// The Y column carried with zero copy on the full-window common case.
    pub(crate) fn carried_ys(&self) -> SharedSlice<f64> {
        self.carried(&self.ys)
    }

    /// The Z column (if present) carried with zero copy on the full-window
    /// common case — for ops that rewrite XY but leave Z untouched.
    pub(crate) fn carried_zs(&self) -> Option<SharedSlice<f64>> {
        self.zs.as_ref().map(|column| self.carried(column))
    }

    /// Carry M into newly computed coordinates, sharing a full owner and
    /// copying only the visible values of a partial owner, like XY/Z carry.
    pub(crate) fn carried_ms(&self) -> Option<MColumn> {
        self.ms.as_ref().map(|column| {
            if column.is_full_owner(self.len()) {
                column.clone()
            } else {
                MColumn::Local {
                    owner: SharedSlice::from_vec(
                        column
                            .values(self.range.start as usize..self.range.end as usize)
                            .to_vec(),
                    ),
                    offset: 0,
                }
            }
        })
    }

    /// Reconstruct the `Point` at `index` (panics out of bounds, like slice
    /// indexing). Prefer column access in hot loops; this is for the by-value
    /// `Point` interchange paths.
    #[expect(
        clippy::indexing_slicing,
        reason = "point_at deliberately keeps slice-like random access and its out-of-range panic"
    )]
    pub fn point_at(&self, index: usize) -> Point {
        // Windowed columns: length equals `self.len()`, so `0..len` iteration
        // proves each access in range (bounds-check elision).
        let xs = self.xs();
        let ys = self.ys();
        debug_assert!(index < xs.len());
        Point {
            x: xs[index],
            y: ys[index],
            z: self.zs().map_or(0.0, |column| column[index]),
            m: self.ms().map_or(0.0, |column| column[index]),
            axes: self.axes(),
        }
    }

    #[cfg(test)]
    pub fn get(&self, index: usize) -> Option<Point> {
        (index < self.len()).then(|| self.point_at(index))
    }

    pub fn first(&self) -> Option<Point> {
        (!self.is_empty()).then(|| self.point_at(0))
    }

    pub fn last(&self) -> Option<Point> {
        self.len().checked_sub(1).map(|index| self.point_at(index))
    }
}

/// Compare two columns by bit pattern (so `NaN` is deterministic and
/// `-0.0`/`0.0` stay distinct), mirroring [`Point`]'s structural identity.
fn columns_bits_eq(left: &[f64], right: &[f64]) -> bool {
    bytemuck::cast_slice::<f64, u8>(left) == bytemuck::cast_slice::<f64, u8>(right)
}

impl PartialEq for CoordSeq {
    fn eq(&self, other: &Self) -> bool {
        // WINDOWED comparison: a view equals any sequence with the same
        // coordinates, independent of the backing buffer it shares.
        self.axes() == other.axes() && coordinate_columns_eq(self, other)
    }
}

pub(crate) fn coordinate_columns_eq(
    left: &(impl crate::geometry::Coordinates + ?Sized),
    right: &(impl crate::geometry::Coordinates + ?Sized),
) -> bool {
    let (Some((left_x, left_y)), Some((right_x, right_y))) =
        (left.xy_columns(), right.xy_columns())
    else {
        return false;
    };
    columns_bits_eq(left_x, right_x)
        && columns_bits_eq(left_y, right_y)
        && match (left.z_column(), right.z_column()) {
            (Some(a), Some(b)) => columns_bits_eq(a, b),
            (None, None) => true,
            _ => false,
        }
        && match (left.m_column(), right.m_column()) {
            (Some(a), Some(b)) => columns_bits_eq(a, b),
            (None, None) => true,
            _ => false,
        }
}

impl Eq for CoordSeq {}

impl std::hash::Hash for CoordSeq {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let hash_column = |column: &[f64], state: &mut H| {
            for value in column {
                value.to_bits().hash(state);
            }
        };
        self.axes().bits().hash(state);
        hash_column(self.xs(), state);
        hash_column(self.ys(), state);
        if let Some(column) = self.zs() {
            hash_column(column, state);
        }
        if let Some(column) = self.ms() {
            hash_column(column, state);
        }
    }
}
