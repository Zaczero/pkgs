use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckedPhysicalRows {
    rows: SharedSlice<usize>,
}

impl CheckedPhysicalRows {
    #[cfg(test)]
    fn checked(rows: Vec<usize>, physical_len: usize) -> Option<Self> {
        if rows.iter().any(|&physical| physical >= physical_len) {
            return None;
        }
        Some(Self {
            rows: SharedSlice::from_vec(rows),
        })
    }

    fn as_slice(&self) -> &[usize] {
        &self.rows
    }
}

impl HeapSize for CheckedPhysicalRows {
    fn heap_bytes(&self) -> usize {
        self.rows.heap_bytes()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RowWindow {
    start: usize,
    len: usize,
}

impl RowWindow {
    pub(crate) const fn start(self) -> usize {
        self.start
    }

    pub(crate) const fn len(self) -> usize {
        self.len
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) enum RowSelection {
    #[default]
    Identity,
    Window(RowWindow),
    Gather(CheckedPhysicalRows),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RowSelectionRef<'a> {
    Identity,
    Window { start: usize, len: usize },
    Gather(&'a [usize]),
}

impl RowSelection {
    /// A gather over `map`, or `None` when a row lies outside `physical_len`.
    #[cfg(test)]
    pub(crate) fn gather_checked_native(map: Vec<usize>, physical_len: usize) -> Option<Self> {
        CheckedPhysicalRows::checked(map, physical_len).map(Self::Gather)
    }

    pub(crate) fn as_deref(&self) -> RowSelectionRef<'_> {
        match self {
            Self::Identity => RowSelectionRef::Identity,
            Self::Window(window) => RowSelectionRef::Window {
                start: window.start(),
                len: window.len(),
            },
            Self::Gather(map) => RowSelectionRef::Gather(map.as_slice()),
        }
    }

    pub(crate) const fn is_identity(&self) -> bool {
        matches!(self, Self::Identity)
    }

    pub(crate) const fn reorders(&self) -> bool {
        !self.is_identity()
    }

    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }

    pub(crate) fn explicit_indices(&self) -> Option<std::borrow::Cow<'_, [usize]>> {
        match self {
            Self::Identity | Self::Window(_) => None,
            Self::Gather(map) => Some(std::borrow::Cow::Borrowed(map.as_slice())),
        }
    }
}

impl HeapSize for RowSelection {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Identity | Self::Window(_) => 0,
            Self::Gather(map) => map.heap_bytes(),
        }
    }
}

impl<'a> From<Option<&'a [usize]>> for RowSelectionRef<'a> {
    fn from(value: Option<&'a [usize]>) -> Self {
        value.map_or(Self::Identity, Self::Gather)
    }
}

/// Expand adjacent admitted CSR offsets at each hot row-window consumer.
macro_rules! admitted_csr_window {
    ($offsets:expr, $physical:expr $(,)?) => {{
        let __offsets = $offsets;
        let __physical = $physical;
        #[expect(
            clippy::indexing_slicing,
            reason = "typed CSR offsets and the selected physical row are admitted"
        )]
        let __window =
            __offsets[__physical] as usize..__offsets[__physical.wrapping_add(1)] as usize;
        __window
    }};
}

impl RowSelectionRef<'_> {
    pub(crate) const fn len(self, physical_len: usize) -> usize {
        match self {
            Self::Identity => physical_len,
            Self::Window { len, .. } => len,
            Self::Gather(map) => map.len(),
        }
    }

    pub(crate) const fn physical(self, logical: usize) -> usize {
        match self {
            Self::Identity => logical,
            Self::Window { start, .. } => start.wrapping_add(logical),
            #[expect(
                clippy::indexing_slicing,
                reason = "the private gathered selector is admitted against its backing rows; retain O(1) random access"
            )]
            Self::Gather(map) => map[logical],
        }
    }

    pub(crate) const fn csr_window(
        self,
        offsets: &[i32],
        logical: usize,
    ) -> std::ops::Range<usize> {
        admitted_csr_window!(offsets, self.physical(logical))
    }

    pub(crate) const fn csr_rows(
        self,
        logical: std::ops::Range<usize>,
    ) -> Option<std::ops::Range<usize>> {
        match self {
            Self::Identity => Some(logical),
            Self::Window { start, .. } => {
                Some(start.wrapping_add(logical.start)..start.wrapping_add(logical.end))
            },
            Self::Gather(_) => None,
        }
    }

    pub(crate) const fn is_identity(self) -> bool {
        matches!(self, Self::Identity)
    }
}

/// Values covered by contiguous CSR rows. Gather selections have no single
/// endpoint range and return `None`.
pub(crate) fn csr_value_range(
    offsets: &[i32],
    row_map: RowSelectionRef<'_>,
    logical_rows: std::ops::Range<usize>,
) -> Option<std::ops::Range<usize>> {
    let physical_rows = row_map.csr_rows(logical_rows)?;
    let endpoints = offsets
        .get(physical_rows.start..=physical_rows.end)
        .expect("admitted CSR rows include both endpoint offsets");
    let start = usize::try_from(
        *endpoints
            .first()
            .expect("CSR endpoint range contains the first offset"),
    )
    .expect("admitted CSR offsets are non-negative");
    let end = usize::try_from(
        *endpoints
            .last()
            .expect("CSR endpoint range contains the final offset"),
    )
    .expect("admitted CSR offsets are non-negative");
    Some(start..end)
}

/// Vertex interval covered by a contiguous range of admitted CSR rows.
pub(crate) fn csr_value_window(
    offsets: &[i32],
    rows: std::ops::Range<usize>,
) -> std::ops::Range<usize> {
    let endpoints = offsets
        .get(rows.start..=rows.end)
        .expect("CSR row range is bounded by its admitted offset column");
    let start = endpoints
        .first()
        .expect("CSR row range includes its start boundary");
    let end = endpoints
        .last()
        .expect("CSR row range includes its end boundary");
    (*start as usize)..(*end as usize)
}

pub(crate) fn physical_row<'a>(row_map: impl Into<RowSelectionRef<'a>>, logical: usize) -> usize {
    row_map.into().physical(logical)
}

pub(crate) fn line_logical_len<'a>(
    offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    row_map.into().len(offsets.len().saturating_sub(1))
}

pub(crate) fn row_map_is_identity<'a>(row_map: impl Into<RowSelectionRef<'a>>) -> bool {
    row_map.into().is_identity()
}

pub(crate) fn polygon_logical_len<'a>(
    polygon_offsets: &[i32],
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    row_map.into().len(polygon_offsets.len().saturating_sub(1))
}

pub(crate) fn point_logical_len<'a>(
    coords: &CoordSeq,
    row_map: impl Into<RowSelectionRef<'a>>,
) -> usize {
    row_map.into().len(coords.len())
}

#[derive(Default)]
struct RowSelectionBuild {
    state: RowSelectionBuildState,
}

#[derive(Default)]
enum RowSelectionBuildState {
    #[default]
    Empty,
    Contiguous(std::ops::Range<usize>),
    Gathered(Vec<usize>),
}

impl RowSelectionBuild {
    fn push(&mut self, index: usize) {
        match &mut self.state {
            RowSelectionBuildState::Empty => {
                self.state = RowSelectionBuildState::Contiguous(index..index.wrapping_add(1));
            },
            RowSelectionBuildState::Contiguous(window) => {
                if index == window.end {
                    window.end = index.wrapping_add(1);
                } else {
                    let mut gathered = Vec::with_capacity(window.len().wrapping_add(1));
                    gathered.extend(window.clone());
                    gathered.push(index);
                    self.state = RowSelectionBuildState::Gathered(gathered);
                }
            },
            RowSelectionBuildState::Gathered(indices) => indices.push(index),
        }
    }

    fn finish(self) -> RowSelection {
        match self.state {
            RowSelectionBuildState::Empty => RowSelection::Gather(CheckedPhysicalRows {
                rows: SharedSlice::empty(),
            }),
            RowSelectionBuildState::Contiguous(window) => RowSelection::Window(RowWindow {
                start: window.start,
                len: window.len(),
            }),
            RowSelectionBuildState::Gathered(indices) => {
                RowSelection::Gather(CheckedPhysicalRows {
                    rows: SharedSlice::from_vec(indices),
                })
            },
        }
    }
}

pub(crate) fn row_selection_from_logical_rows(
    row_map: RowSelectionRef<'_>,
    physical_len: usize,
    rows: impl IntoIterator<Item = usize>,
) -> RowSelection {
    let mut build = RowSelectionBuild::default();
    for logical in rows {
        assert!(
            logical < row_map.len(physical_len),
            "logical row is outside selected row map"
        );
        let physical = physical_row(row_map, logical);
        assert!(
            physical < physical_len,
            "physical row is outside backing storage"
        );
        build.push(physical);
    }
    build.finish()
}

pub(crate) fn contiguous_physical_range(
    row_map: RowSelectionRef<'_>,
    rows: impl IntoIterator<Item = usize>,
) -> Option<std::ops::Range<usize>> {
    let mut rows = rows.into_iter();
    let start = physical_row(row_map, rows.next()?);
    let mut end = start.checked_add(1)?;
    for logical in rows {
        if physical_row(row_map, logical) != end {
            return None;
        }
        end = end.checked_add(1)?;
    }
    Some(start..end)
}
