//! Canonical logical row ownership and admission for geometry arrays.

use std::ops::Range;
use std::sync::Arc;

use super::builder::PointColumnBuilder;
use super::packed_columns::PackedColumns;
use super::validity::ValidityCursor;
use super::{
    GeometryArrayStorage, PackedRow, PackedStorage, RowSelection, RowSelectionRef, RowsIter,
    ValidityMask, admit_packed_coord_count, contiguous_physical_range,
    materialized_packed_lines_len, materialized_packed_polygons_len, physical_row,
    point_logical_len, row_selection_from_logical_rows,
};
use crate::error::Result;
use crate::geometry::{
    CoordSeq, CoordinateAxes, CsrOffsetBuilder, GeometryErrorKind, GeometryKind, PolygonLevel,
    RingLevel, Shape, ShapeCountQuery, Work,
};

/// How a nullable payload is aligned when it crosses the owner boundary.
///
/// This is ingress intent. Equal payload and mask lengths do not establish
/// whether rows are logical or present-only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StorageLayout {
    /// The payload has one row or window for every logical mask position.
    LogicalRows,
    /// The payload contains only rows selected by present mask bits.
    PresentOnly,
}

/// A logical row together with its source row id.
#[derive(Clone, Copy)]
pub(crate) enum LogicalRow<'a> {
    Missing {
        logical: usize,
    },
    Present {
        logical: usize,
        row: super::ShapeRow<'a>,
    },
}

enum LogicalRowsState<'a> {
    Dense(RowsIter<'a>),
    NullableLogical {
        rows: RowsIter<'a>,
        validity: ValidityCursor<'a>,
    },
    CompactPoints {
        rows: RowsIter<'a>,
        validity: ValidityCursor<'a>,
    },
}

pub(crate) struct LogicalRows<'a> {
    state: LogicalRowsState<'a>,
    logical: usize,
    remaining: usize,
}

impl<'a> Iterator for LogicalRows<'a> {
    type Item = LogicalRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let logical = self.logical;
        self.logical = self.logical.wrapping_add(1);
        self.remaining = self.remaining.wrapping_sub(1);
        Some(match &mut self.state {
            LogicalRowsState::Dense(rows) => LogicalRow::Present {
                logical,
                row: rows.next().expect("dense storage length is exact"),
            },
            LogicalRowsState::NullableLogical { rows, validity } => {
                let (_, present) = validity.next().expect("validity length is exact");
                let row = rows.next().expect("logical storage length is exact");
                present.map_or(LogicalRow::Missing { logical }, |_| LogicalRow::Present {
                    logical,
                    row,
                })
            },
            LogicalRowsState::CompactPoints { rows, validity } => {
                let (_, present) = validity.next().expect("validity length is exact");
                present.map_or(LogicalRow::Missing { logical }, |_| LogicalRow::Present {
                    logical,
                    row: rows.next().expect("present point payload count is exact"),
                })
            },
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for LogicalRows<'_> {}

/// The canonical owner of logical geometry rows and their optional validity.
///
/// Packed nullable points contain one physical point per present bit. Packed
/// nullable lines and polygons contain one logical CSR window per bit. Mixed
/// nullable storage retains one logical shape slot per bit.
#[derive(Clone, Debug)]
pub(crate) struct ArrayRows {
    storage: Arc<GeometryArrayStorage>,
    missing: Option<ValidityMask>,
}

impl ArrayRows {
    const fn dense(storage: Arc<GeometryArrayStorage>) -> Self {
        Self {
            storage,
            missing: None,
        }
    }

    const fn nullable(storage: Arc<GeometryArrayStorage>, missing: ValidityMask) -> Self {
        Self {
            storage,
            missing: Some(missing),
        }
    }

    /// Admit raw payload and optional validity into the canonical row layout.
    pub(crate) fn admit(
        storage: Arc<GeometryArrayStorage>,
        missing: Option<ValidityMask>,
        layout: StorageLayout,
    ) -> Result<Self> {
        let Some(mask) = missing else {
            return Ok(Self::dense(storage));
        };

        match layout {
            StorageLayout::LogicalRows => {
                if storage.len() != mask.len() {
                    return Err(GeometryErrorKind::message(
                        "logical nullable payload length must match the missing mask",
                    ));
                }
                match storage.as_ref() {
                    GeometryArrayStorage::Mixed(_) => {
                        if mask.any() {
                            Ok(Self::nullable(storage, mask))
                        } else {
                            Ok(Self::dense(storage))
                        }
                    },
                    GeometryArrayStorage::Packed(PackedStorage::Points { .. }) => {
                        let compact =
                            Self::compact_point_storage_from_logical(storage.as_ref(), &mask)?;
                        Ok(Self::nullable(Arc::new(compact), mask))
                    },
                    GeometryArrayStorage::Packed(
                        PackedStorage::Lines { .. } | PackedStorage::Polygons { .. },
                    ) => {
                        if Self::is_compact_logical_storage(storage.as_ref(), &mask) {
                            Ok(Self::nullable(storage, mask))
                        } else {
                            let compact =
                                Self::compact_storage_from_logical(storage.as_ref(), &mask)?;
                            Ok(Self::nullable(Arc::new(compact), mask))
                        }
                    },
                }
            },
            StorageLayout::PresentOnly => match storage.as_ref() {
                GeometryArrayStorage::Mixed(_) => {
                    let scattered = Self::scatter_mixed_storage(storage.as_ref(), &mask)?;
                    if mask.any() {
                        Ok(Self::nullable(Arc::new(scattered), mask))
                    } else {
                        Ok(Self::dense(Arc::new(scattered)))
                    }
                },
                GeometryArrayStorage::Packed(_) => {
                    if storage.len() != mask.present_count() {
                        return Err(GeometryErrorKind::message(
                            "present-only payload length must match the missing mask present count",
                        ));
                    }
                    if matches!(
                        storage.as_ref(),
                        GeometryArrayStorage::Packed(PackedStorage::Points {
                            row_map,
                            ..
                        }) if row_map.is_identity()
                    ) {
                        Ok(Self::nullable(storage, mask))
                    } else {
                        let compact = Self::compact_storage_from_present(storage.as_ref(), &mask)?;
                        Ok(Self::nullable(Arc::new(compact), mask))
                    }
                },
            },
        }
    }

    /// Admit a final writer's row map with the payload, preserving its logical
    /// row identity through present-only compaction or scattering.
    #[expect(
        dead_code,
        reason = "geographic final-storage receipts admit their writer map with the row owner"
    )]
    pub(crate) fn admit_with_row_map<T, W: Work>(
        storage: Arc<GeometryArrayStorage>,
        missing: Option<ValidityMask>,
        layout: StorageLayout,
        row_map: Vec<Option<T>>,
        work: &mut W,
    ) -> Result<(Self, Vec<Option<T>>)> {
        work.charge(
            u64::try_from(storage.len()).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let mut count = ShapeCountQuery::default();
        let mut coordinates = 0_usize;
        for row_index in 0..storage.len() {
            work.charge(1)?;
            coordinates = coordinates
                .checked_add(storage.row(row_index).coordinates(&mut count)?)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        work.charge(
            u64::try_from(coordinates).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let logical_rows = match missing.as_ref() {
            Some(mask) => mask.len(),
            None => storage.len(),
        };
        let row_map = match (missing.as_ref(), layout) {
            (Some(mask), StorageLayout::PresentOnly) => {
                if row_map.len() != mask.present_count() {
                    return Err(GeometryErrorKind::message(
                        "present-only writer map must match the payload's present row count",
                    ));
                }
                let mut present_rows = row_map.into_iter();
                let mut logical_map = Vec::with_capacity(mask.len());
                for logical in 0..mask.len() {
                    if mask.is_missing(logical) {
                        logical_map.push(None);
                    } else {
                        logical_map.push(Some(present_rows.next().flatten().ok_or_else(|| {
                            GeometryErrorKind::message("present writer row has no literal map")
                        })?));
                    }
                }
                if present_rows.next().is_some() {
                    return Err(GeometryErrorKind::message(
                        "present-only writer map contains an unused row",
                    ));
                }
                logical_map
            },
            (Some(mask), StorageLayout::LogicalRows) => {
                if row_map.len() != mask.len() {
                    return Err(GeometryErrorKind::message(
                        "logical writer map must match the missing mask",
                    ));
                }
                if row_map
                    .iter()
                    .enumerate()
                    .any(|(logical, row)| mask.is_missing(logical) != row.is_none())
                {
                    return Err(GeometryErrorKind::message(
                        "logical writer map does not match row validity",
                    ));
                }
                row_map
            },
            (None, _) => {
                if row_map.len() != logical_rows || row_map.iter().any(Option::is_none) {
                    return Err(GeometryErrorKind::message(
                        "dense writer map must cover every payload row",
                    ));
                }
                row_map
            },
        };
        let rows = Self::admit(storage, missing, layout)?;
        if row_map.len() != rows.len() {
            return Err(GeometryErrorKind::message(
                "admitted row map does not cover every logical row",
            ));
        }
        Ok((rows, row_map))
    }

    pub(crate) fn storage(&self) -> &GeometryArrayStorage {
        self.storage.as_ref()
    }

    pub(crate) const fn storage_arc(&self) -> &Arc<GeometryArrayStorage> {
        &self.storage
    }

    pub(crate) const fn missing(&self) -> Option<&ValidityMask> {
        self.missing.as_ref()
    }

    pub(crate) fn len(&self) -> usize {
        self.missing
            .as_ref()
            .map_or_else(|| self.storage.len(), ValidityMask::len)
    }

    /// Whether validity accompanies packed storage; Mixed nullable rows remain
    /// logically backed and use the per-shape path.
    pub(crate) fn is_compact_nullable(&self) -> bool {
        self.missing.is_some() && matches!(self.storage.as_ref(), GeometryArrayStorage::Packed(_))
    }

    pub(crate) fn packed_columns(&self) -> Option<PackedColumns<'_>> {
        super::packed_columns::resolve_packed_columns(self)
    }

    /// Materialize this owner's selected packed points into one coordinate column.
    pub(crate) fn gather_point_coords(&self) -> Result<CoordSeq> {
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
            self.storage.as_ref()
        else {
            unreachable!("point coordinate gathering requires packed points");
        };
        Self::gather_point_selection(coords, row_map.as_deref())
    }

    pub(crate) fn logical_row(&self, logical: usize) -> Option<LogicalRow<'_>> {
        if logical >= self.len() {
            return None;
        }
        let Some(missing) = self.missing.as_ref() else {
            return Some(LogicalRow::Present {
                logical,
                row: self.storage.row(logical),
            });
        };
        if missing.is_missing(logical) {
            return Some(LogicalRow::Missing { logical });
        }
        let payload = if matches!(
            self.storage.as_ref(),
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
        ) {
            missing.present_before(logical)
        } else {
            logical
        };
        Some(LogicalRow::Present {
            logical,
            row: self.storage.row(payload),
        })
    }

    pub(crate) fn logical_rows(&self) -> LogicalRows<'_> {
        let remaining = self.len();
        let state = match (&self.missing, self.storage.as_ref()) {
            (None, _) => LogicalRowsState::Dense(self.storage.iter_rows()),
            (Some(missing), GeometryArrayStorage::Packed(PackedStorage::Points { .. })) => {
                LogicalRowsState::CompactPoints {
                    rows: self.storage.iter_rows(),
                    validity: missing.cursor(),
                }
            },
            (Some(missing), _) => LogicalRowsState::NullableLogical {
                rows: self.storage.iter_rows(),
                validity: missing.cursor(),
            },
        };
        LogicalRows {
            state,
            logical: 0,
            remaining,
        }
    }

    /// Select logical source rows in output order, retaining missing rows.
    pub(crate) fn select<I>(&self, source_rows: I) -> Result<Self>
    where
        I: ExactSizeIterator<Item = usize> + Clone,
    {
        let missing = self.selected_missing(source_rows.clone());
        let storage = self.selected_storage(source_rows);

        if self.is_compact_nullable()
            && let Some(mask) = missing.as_ref()
            && matches!(
                &storage,
                GeometryArrayStorage::Packed(
                    PackedStorage::Lines { .. } | PackedStorage::Polygons { .. }
                )
            )
        {
            // Source-owned selection preserves canonical nullable CSR windows,
            // including nonzero windows and duplicate gathers.
            return Ok(Self::nullable(Arc::new(storage), mask.clone()));
        }

        let layout = if missing.is_some()
            && matches!(
                &storage,
                GeometryArrayStorage::Packed(PackedStorage::Points { .. })
            ) {
            StorageLayout::PresentOnly
        } else {
            StorageLayout::LogicalRows
        };
        Self::admit(Arc::new(storage), missing, layout)
    }

    /// Select a contiguous logical source range without allocating row ids.
    pub(crate) fn select_range(&self, source_rows: Range<usize>) -> Result<Self> {
        let missing = self
            .missing
            .as_ref()
            .and_then(|mask| mask.window(source_rows.start, source_rows.len()));
        let storage = if self.is_compact_nullable()
            && matches!(
                self.storage.as_ref(),
                GeometryArrayStorage::Packed(PackedStorage::Points { .. })
            ) {
            let mask = self
                .missing
                .as_ref()
                .expect("nullable packed points have a validity mask");
            let present =
                mask.present_before(source_rows.start)..mask.present_before(source_rows.end);
            self.selected_point_present_rows(present)
        } else {
            self.selected_storage(source_rows)
        };
        if self.is_compact_nullable()
            && let Some(mask) = missing.as_ref()
            && matches!(
                &storage,
                GeometryArrayStorage::Packed(
                    PackedStorage::Lines { .. } | PackedStorage::Polygons { .. }
                )
            )
        {
            return Ok(Self::nullable(Arc::new(storage), mask.clone()));
        }
        let layout = if missing.is_some()
            && matches!(
                &storage,
                GeometryArrayStorage::Packed(PackedStorage::Points { .. })
            ) {
            StorageLayout::PresentOnly
        } else {
            StorageLayout::LogicalRows
        };
        Self::admit(Arc::new(storage), missing, layout)
    }

    /// Select source rows known to be present and publish a dense owner.
    pub(crate) fn select_present<I>(&self, source_rows: I) -> Result<Self>
    where
        I: Iterator<Item = usize> + Clone,
    {
        debug_assert!(source_rows.clone().all(|row| {
            !self
                .missing
                .as_ref()
                .is_some_and(|mask| mask.is_missing(row))
        }));
        let storage = self.selected_storage(source_rows);
        Self::admit(Arc::new(storage), None, StorageLayout::LogicalRows)
    }

    /// Apply a new logical mask through this owner's source rows.
    pub(crate) fn remask(&self, missing: Option<ValidityMask>) -> Result<Self> {
        let Some(mask) = missing else {
            if self.missing.is_some() {
                return Err(GeometryErrorKind::message(
                    "cannot clear an existing GeometryArray missing mask",
                ));
            }
            return Ok(self.clone());
        };
        if mask.len() != self.len() {
            return Err(GeometryErrorKind::message(
                "missing mask length must match array length",
            ));
        }
        if self.missing.as_ref() == Some(&mask) {
            return Ok(self.clone());
        }
        if self.is_compact_nullable() {
            for (row, present) in mask.cursor() {
                if present.is_some()
                    && self
                        .missing
                        .as_ref()
                        .is_some_and(|source| source.is_missing(row))
                {
                    return Err(GeometryErrorKind::message(
                        "a compact nullable array cannot unmask an existing row",
                    ));
                }
            }
            let storage =
                self.selected_storage((0..mask.len()).filter(|&logical| mask.is_valid(logical)));
            return Self::admit(Arc::new(storage), Some(mask), StorageLayout::PresentOnly);
        }
        Self::admit(
            Arc::clone(&self.storage),
            Some(mask),
            StorageLayout::LogicalRows,
        )
    }

    /// Return present source rows as a dense owner.
    pub(crate) fn drop_missing(&self) -> Result<Self> {
        if self.missing.is_none() {
            return Ok(self.clone());
        }
        if matches!(
            self.storage.as_ref(),
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
        ) {
            // Every nullable point admission rebases its present payload to identity.
            return Ok(Self::dense(Arc::clone(&self.storage)));
        }
        self.select_present(self.present_logical_indices())
    }

    /// Repeatable logical source ids for present rows.
    pub(crate) fn present_logical_indices(&self) -> impl Iterator<Item = usize> + Clone + '_ {
        (0..self.len()).filter(|&logical| {
            self.missing
                .as_ref()
                .is_none_or(|mask| mask.is_valid(logical))
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "an exact-size source iterator keeps each output index within its pre-sized validity bitmap"
    )]
    fn selected_missing<I>(&self, source_rows: I) -> Option<ValidityMask>
    where
        I: ExactSizeIterator<Item = usize> + Clone,
    {
        let mask = self.missing.as_ref()?;
        let row_count = source_rows.len();
        let mut bytes = vec![0; row_count.div_ceil(8)];
        for (out, row) in source_rows.enumerate() {
            if mask.is_valid(row) {
                bytes[out / 8] |= 1 << (out % 8);
            }
        }
        ValidityMask::from_bitmap(crate::geometry::SharedSlice::from_vec(bytes), 0, row_count)
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "admitted packed line and polygon offset columns always contain their initial sentinel"
    )]
    fn selected_storage<I>(&self, source_rows: I) -> GeometryArrayStorage
    where
        I: Iterator<Item = usize> + Clone,
    {
        match self.storage.as_ref() {
            GeometryArrayStorage::Packed(PackedStorage::Points { .. }) => {
                let rank = self
                    .missing
                    .as_ref()
                    .filter(|_| self.is_compact_nullable())
                    .map(|mask| mask.rank_for_rows(source_rows.clone()));
                let present = source_rows.filter_map(|logical| {
                    rank.as_ref()
                        .map_or(Some(logical), |rank| rank.resolve(logical))
                });
                self.selected_point_present_rows(present)
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: Arc::clone(coords),
                offsets: offsets.clone(),
                row_map: row_selection_from_logical_rows(
                    row_map.as_deref(),
                    offsets.len() - 1,
                    source_rows,
                ),
            }),
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: Arc::clone(coords),
                ring_offsets: ring_offsets.clone(),
                polygon_offsets: polygon_offsets.clone(),
                row_map: row_selection_from_logical_rows(
                    row_map.as_deref(),
                    polygon_offsets.len() - 1,
                    source_rows,
                ),
            }),
            GeometryArrayStorage::Mixed(shapes) => GeometryArrayStorage::Mixed(
                source_rows
                    .map(|row| {
                        shapes
                            .get(row)
                            .expect("selected mixed row is within the backing array")
                            .clone()
                    })
                    .collect(),
            ),
        }
    }

    fn selected_point_present_rows<I>(&self, present: I) -> GeometryArrayStorage
    where
        I: Iterator<Item = usize> + Clone,
    {
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
            self.storage.as_ref()
        else {
            unreachable!("point row selection requires packed points");
        };
        if present.clone().next().is_none() {
            return GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(CoordSeq::empty(coords.axes())),
                row_map: RowSelection::Identity,
            });
        }
        if let Some(window) = contiguous_physical_range(row_map.as_deref(), present.clone()) {
            return GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(coords.view(crate::geometry::CoordWindow::from_range(
                    window,
                    coords.len(),
                ))),
                row_map: RowSelection::Identity,
            });
        }
        GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::clone(coords),
            row_map: row_selection_from_logical_rows(row_map.as_deref(), coords.len(), present),
        })
    }

    fn compact_point_storage_from_logical(
        storage: &GeometryArrayStorage,
        mask: &ValidityMask,
    ) -> Result<GeometryArrayStorage> {
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) = storage
        else {
            return Err(GeometryErrorKind::message(
                "logical nullable point storage requires packed points",
            ));
        };
        if storage.len() != mask.len() {
            return Err(GeometryErrorKind::message(
                "logical nullable payload length must match the missing mask",
            ));
        }
        let rows = mask.present_count();
        admit_packed_coord_count(rows)?;
        let mut builder = PointColumnBuilder::with_capacity(coords.axes(), rows);
        for (logical, present) in mask.cursor() {
            if present.is_some() {
                builder.push_at(coords, physical_row(row_map.as_deref(), logical));
            }
        }
        Ok(GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::new(builder.finish()?),
            row_map: RowSelection::Identity,
        }))
    }

    fn compact_storage_from_logical(
        storage: &GeometryArrayStorage,
        mask: &ValidityMask,
    ) -> Result<GeometryArrayStorage> {
        if storage.len() != mask.len() {
            return Err(GeometryErrorKind::message(
                "logical nullable payload length must match the missing mask",
            ));
        }
        let present_rows = (0..mask.len()).filter(|&logical| !mask.is_missing(logical));
        match storage {
            GeometryArrayStorage::Packed(PackedStorage::Points { .. }) => {
                Self::compact_point_storage_from_logical(storage, mask)
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let selected = GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords: Arc::clone(coords),
                    offsets: offsets.clone(),
                    row_map: row_selection_from_logical_rows(
                        row_map.as_deref(),
                        offsets.len().saturating_sub(1),
                        present_rows,
                    ),
                });
                Self::compact_storage_from_present(&selected, mask)
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let selected = GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords: Arc::clone(coords),
                    ring_offsets: ring_offsets.clone(),
                    polygon_offsets: polygon_offsets.clone(),
                    row_map: row_selection_from_logical_rows(
                        row_map.as_deref(),
                        polygon_offsets.len().saturating_sub(1),
                        present_rows,
                    ),
                });
                Self::compact_storage_from_present(&selected, mask)
            },
            GeometryArrayStorage::Mixed(_) => Err(GeometryErrorKind::message(
                "logical nullable mixed storage cannot use packed compaction",
            )),
        }
    }

    /// Convert an explicitly present-only packed payload to canonical nullable
    /// points or logical CSR rows.
    fn compact_storage_from_present(
        storage: &GeometryArrayStorage,
        mask: &ValidityMask,
    ) -> Result<GeometryArrayStorage> {
        if storage.len() != mask.present_count() {
            return Err(GeometryErrorKind::message(
                "present-only payload length must match the mask present count",
            ));
        }
        match storage {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                Ok(GeometryArrayStorage::Packed(PackedStorage::Points {
                    coords: if row_map.is_identity() {
                        Arc::clone(coords)
                    } else {
                        Arc::new(Self::gather_point_selection(coords, row_map.as_deref())?)
                    },
                    row_map: RowSelection::Identity,
                }))
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                if row_map.is_identity() {
                    let mut logical_offsets = CsrOffsetBuilder::with_capacity(mask.len());
                    let mut present_end = 0_i32;
                    let mut source_ends = offsets.as_slice().iter().copied().skip(1);
                    for (_, present) in mask.cursor() {
                        if present.is_some() {
                            present_end = source_ends
                                .next()
                                .expect("present line count matches its source CSR rows");
                        }
                        logical_offsets.push_end(present_end as usize, i32::MAX as usize)?;
                    }
                    return Ok(GeometryArrayStorage::Packed(PackedStorage::Lines {
                        coords: Arc::clone(coords),
                        offsets: logical_offsets.finish(coords.len())?,
                        row_map: RowSelection::Identity,
                    }));
                }
                let vertices = materialized_packed_lines_len(offsets.as_slice(), map)?;
                let mut builder = PointColumnBuilder::with_capacity(coords.axes(), vertices);
                let mut logical_offsets = CsrOffsetBuilder::with_capacity(mask.len());
                let mut present_row = 0;
                for (_, present_index) in mask.cursor() {
                    if present_index.is_some() {
                        let row = PackedRow::line(present_row, coords, offsets, map);
                        for vertex in row
                            .vertex_window()
                            .expect("present line row has a coordinate window")
                        {
                            builder.push_at(coords, vertex);
                        }
                        present_row = present_row.wrapping_add(1);
                    }
                    logical_offsets.push_end(builder.len(), i32::MAX as usize)?;
                }
                let coords = builder.finish()?;
                Ok(GeometryArrayStorage::Packed(PackedStorage::Lines {
                    offsets: logical_offsets.finish(coords.len())?,
                    coords: Arc::new(coords),
                    row_map: RowSelection::Identity,
                }))
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                if row_map.is_identity() {
                    let mut logical_polygons = CsrOffsetBuilder::with_capacity(mask.len());
                    let mut present_end = 0_i32;
                    let mut source_ends = polygon_offsets.as_slice().iter().copied().skip(1);
                    for (_, present) in mask.cursor() {
                        if present.is_some() {
                            present_end = source_ends
                                .next()
                                .expect("present polygon count matches its source CSR rows");
                        }
                        logical_polygons.push_end(present_end as usize, i32::MAX as usize)?;
                    }
                    return Ok(GeometryArrayStorage::Packed(PackedStorage::Polygons {
                        coords: Arc::clone(coords),
                        ring_offsets: ring_offsets.clone(),
                        polygon_offsets: logical_polygons
                            .finish(ring_offsets.len().saturating_sub(1))?
                            .cast_level::<PolygonLevel>(),
                        row_map: RowSelection::Identity,
                    }));
                }
                let (vertices, ring_capacity) = materialized_packed_polygons_len(
                    ring_offsets.as_slice(),
                    polygon_offsets.as_slice(),
                    map,
                )?;
                let mut builder = PointColumnBuilder::with_capacity(coords.axes(), vertices);
                let mut out_rings = CsrOffsetBuilder::with_capacity(ring_capacity);
                let mut out_polygons = CsrOffsetBuilder::with_capacity(mask.len());
                let mut present_row = 0;
                let mut rings = 0_usize;
                for (_, present_index) in mask.cursor() {
                    if present_index.is_some() {
                        let row = PackedRow::polygon(
                            present_row,
                            coords,
                            ring_offsets,
                            polygon_offsets,
                            map,
                        );
                        let row_rings = row
                            .polygon_rings()
                            .expect("present polygon row has a ring window");
                        for ring in row_rings.clone() {
                            let vertices = row
                                .polygon_ring_window(ring)
                                .expect("ring lies in its admitted polygon row");
                            for vertex in vertices {
                                builder.push_at(coords, vertex);
                            }
                            out_rings.push_end(builder.len(), i32::MAX as usize)?;
                        }
                        rings = rings.wrapping_add(row_rings.len());
                        present_row = present_row.wrapping_add(1);
                    }
                    out_polygons.push_end(rings, i32::MAX as usize)?;
                }
                let coords = builder.finish()?;
                let coord_len = coords.len();
                Ok(GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords: Arc::new(coords),
                    ring_offsets: out_rings.finish(coord_len)?.cast_level::<RingLevel>(),
                    polygon_offsets: out_polygons.finish(rings)?.cast_level::<PolygonLevel>(),
                    row_map: RowSelection::Identity,
                }))
            },
            GeometryArrayStorage::Mixed(_) => Err(GeometryErrorKind::message(
                "present-only mixed storage requires row scattering",
            )),
        }
    }

    /// Whether logical CSR rows already have canonical zero-width missing windows.
    fn is_compact_logical_storage(storage: &GeometryArrayStorage, mask: &ValidityMask) -> bool {
        if mask.present_count() == mask.len() {
            return false;
        }
        match storage {
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) if row_map.is_identity() => {
                let ends = offsets.as_slice();
                ends.len() == mask.len().saturating_add(1)
                    && ends
                        .last()
                        .is_some_and(|&end| end >= 0 && end as usize == coords.len())
                    && mask.iter().enumerate().all(|(row, missing)| {
                        !missing || RowSelectionRef::Identity.csr_window(ends, row).is_empty()
                    })
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) if row_map.is_identity() => {
                let polygons = polygon_offsets.as_slice();
                let rings = ring_offsets.as_slice();
                polygons.len() == mask.len().saturating_add(1)
                    && rings
                        .last()
                        .is_some_and(|&end| end >= 0 && end as usize == coords.len())
                    && mask.iter().enumerate().all(|(row, missing)| {
                        !missing
                            || RowSelectionRef::Identity
                                .csr_window(polygons, row)
                                .is_empty()
                    })
            },
            GeometryArrayStorage::Packed(
                PackedStorage::Points { .. }
                | PackedStorage::Lines { .. }
                | PackedStorage::Polygons { .. },
            )
            | GeometryArrayStorage::Mixed(_) => false,
        }
    }

    fn scatter_mixed_storage(
        storage: &GeometryArrayStorage,
        mask: &ValidityMask,
    ) -> Result<GeometryArrayStorage> {
        let GeometryArrayStorage::Mixed(shapes) = storage else {
            return Err(GeometryErrorKind::message(
                "mixed row scattering requires mixed storage",
            ));
        };
        if shapes.len() != mask.present_count() {
            return Err(GeometryErrorKind::message(
                "present-only payload length must match the mask present count",
            ));
        }
        let mut rows = Vec::with_capacity(mask.len());
        let mut present = shapes.iter();
        for missing in mask.iter() {
            if missing {
                rows.push(Self::missing_placeholder());
            } else {
                let shape = present
                    .next()
                    .expect("present count matches the admitted mask");
                rows.push(shape.clone());
            }
        }
        Ok(GeometryArrayStorage::Mixed(rows))
    }

    fn gather_point_selection(coords: &CoordSeq, row_map: RowSelectionRef<'_>) -> Result<CoordSeq> {
        let rows = point_logical_len(coords, row_map);
        admit_packed_coord_count(rows)?;
        let mut builder = PointColumnBuilder::with_capacity(coords.axes(), rows);
        for logical in 0..rows {
            builder.push_at(coords, physical_row(row_map, logical));
        }
        builder.finish()
    }

    /// Coordinate-free sentinel used only by heterogeneous Mixed storage.
    pub(crate) const fn missing_placeholder() -> Shape {
        Shape::Empty(GeometryKind::GeometryCollection, CoordinateAxes::XY)
    }
}
