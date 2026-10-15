use std::sync::Arc;

#[cfg(test)]
use super::literals::LiteralRowLayout;
use super::literals::{FlatLiteralLayout, LiteralAddress, ShapeLiteralLayout};
use super::{ResourceError, Result, Work};
use crate::geometry::publication::StorageTarget;
use crate::geometry::storage::{ArrayRows, GeometryArrayStorage, StorageLayout, ValidityMask};
use crate::geometry::{CoordinateAxes, GeometryErrorKind, Point, Shape};

#[derive(Clone)]
pub(in crate::geometry) struct CheckedLiteralRecord<I> {
    pub(in crate::geometry) address: LiteralAddress,
    pub(in crate::geometry) image: I,
    pub(in crate::geometry) record: Point,
}

/// Owns the actual finalized sink and its independent logical-use map.
pub(in crate::geometry) struct FrozenLiteralBuffers<I> {
    pub(super) storage: FrozenStorage<I>,
}

/// Created only by the actual-buffer binder; it moves the same sink onward.
pub(in crate::geometry) struct SealedOutput<I> {
    pub(super) buffers: FrozenLiteralBuffers<I>,
    pub(super) checked: Vec<CheckedLiteralRecord<I>>,
}

pub(super) enum FrozenStorage<I> {
    Scalar {
        shape: Shape,
        layout: ShapeLiteralLayout<I>,
    },
    FlatScalar {
        shape: Shape,
        layout: Box<FlatLiteralLayout<I>>,
    },
    Rows {
        rows: ArrayRows,
        layout: Arc<FlatLiteralLayout<I>>,
        visit_rows: std::ops::Range<usize>,
    },
    Groups {
        groups: FinalGroupRows,
        layout: Box<FlatLiteralLayout<I>>,
        expected_present: Vec<bool>,
    },
}

pub(in crate::geometry) enum LiteralStorage {
    Scalar(Shape),
    Rows(ArrayRows),
    Groups(FinalGroupRows),
}

pub(crate) struct FinalGroupRows {
    pub(crate) values: ArrayRows,
    pub(crate) offsets: Vec<i64>,
    pub(crate) missing: Option<ValidityMask>,
}

pub(in crate::geometry) enum LiteralCheck<I> {
    Pass(SealedOutput<I>),
    Reject(Vec<I>),
}

pub(in crate::geometry) trait LiteralReceiver<I> {
    fn accepts_point<W: Work>(
        &mut self,
        address: LiteralAddress,
        image: I,
        record: Point,
        expected_axes: CoordinateAxes,
        stored_axes: CoordinateAxes,
        work: &mut W,
    ) -> Result<bool>;
}

impl<I> FrozenLiteralBuffers<I> {
    pub(in crate::geometry) const fn shapes(shape: Shape, layout: ShapeLiteralLayout<I>) -> Self {
        Self {
            storage: FrozenStorage::Scalar { shape, layout },
        }
    }

    pub(in crate::geometry) fn flat_shape(shape: Shape, layout: FlatLiteralLayout<I>) -> Self {
        Self {
            storage: FrozenStorage::FlatScalar {
                shape,
                layout: Box::new(layout),
            },
        }
    }

    #[cfg(test)]
    pub(in crate::geometry) fn rows<W: Work>(
        storage: Arc<GeometryArrayStorage>,
        missing: Option<ValidityMask>,
        storage_layout: StorageLayout,
        row_map: Vec<Option<LiteralRowLayout<I>>>,
        work: &mut W,
    ) -> Result<Self> {
        let mut layout = FlatLiteralLayout::with_capacity(row_map.len());
        for row in row_map {
            if let Some(row) = row {
                layout.push_layout(row)?;
            } else {
                layout.rows.push(None);
            }
        }
        Self::flat_rows(storage, missing, storage_layout, layout, work)
    }

    pub(in crate::geometry) fn flat_rows<W: Work>(
        storage: Arc<GeometryArrayStorage>,
        missing: Option<ValidityMask>,
        storage_layout: StorageLayout,
        mut layout: FlatLiteralLayout<I>,
        work: &mut W,
    ) -> Result<Self> {
        let rows = admit_flat_rows(storage, missing, storage_layout, &mut layout, work)?;
        Ok(Self {
            storage: FrozenStorage::Rows {
                visit_rows: 0..rows.len(),
                rows,
                layout: Arc::new(layout),
            },
        })
    }

    #[cfg(test)]
    pub(in crate::geometry) fn groups<W: Work>(
        storage: Arc<GeometryArrayStorage>,
        value_missing: Option<ValidityMask>,
        value_layout: StorageLayout,
        value_map: Vec<Option<LiteralRowLayout<I>>>,
        offsets: Vec<i64>,
        missing: Option<ValidityMask>,
        work: &mut W,
    ) -> Result<Self> {
        let mut layout = FlatLiteralLayout::with_capacity(value_map.len());
        for row in value_map {
            if let Some(row) = row {
                layout.push_layout(row)?;
            } else {
                layout.rows.push(None);
            }
        }
        let group_count = offsets.len().checked_sub(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("group domain has no offset origin".into())
        })?;
        if missing
            .as_ref()
            .is_some_and(|mask| mask.len() != group_count)
        {
            return Err(GeometryErrorKind::Invalid(
                "group validity does not match the declared group count".into(),
            )
            .into());
        }
        let expected_present = (0..group_count)
            .map(|group| !missing.as_ref().is_some_and(|mask| mask.is_missing(group)))
            .collect();
        Self::flat_groups(
            storage,
            value_missing,
            value_layout,
            layout,
            offsets,
            missing,
            expected_present,
            work,
        )
    }

    pub(in crate::geometry) fn flat_groups<W: Work>(
        storage: Arc<GeometryArrayStorage>,
        value_missing: Option<ValidityMask>,
        value_layout: StorageLayout,
        mut layout: FlatLiteralLayout<I>,
        offsets: Vec<i64>,
        missing: Option<ValidityMask>,
        expected_present: Vec<bool>,
        work: &mut W,
    ) -> Result<Self> {
        let values = admit_flat_rows(storage, value_missing, value_layout, &mut layout, work)?;
        Ok(Self {
            storage: FrozenStorage::Groups {
                groups: FinalGroupRows {
                    values,
                    offsets,
                    missing,
                },
                layout: Box::new(layout),
                expected_present,
            },
        })
    }

    pub(in crate::geometry) fn admitted_row_view(
        rows: ArrayRows,
        layout: Arc<FlatLiteralLayout<I>>,
        logical_row: usize,
    ) -> Result<Self> {
        let end = logical_row
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        if end > rows.len() || layout.rows.len() != rows.len() {
            return Err(GeometryErrorKind::Invalid(
                "final array row view is outside its admitted owner".into(),
            )
            .into());
        }
        Ok(Self {
            storage: FrozenStorage::Rows {
                rows,
                layout,
                visit_rows: logical_row..end,
            },
        })
    }

    pub(in crate::geometry) fn admit_target(self, target: StorageTarget) -> Result<Self> {
        let matches = matches!(
            (&self.storage, target),
            (
                FrozenStorage::Scalar { .. } | FrozenStorage::FlatScalar { .. },
                StorageTarget::Scalar
            ) | (FrozenStorage::Rows { .. }, StorageTarget::Array)
                | (FrozenStorage::Groups { .. }, StorageTarget::Groups)
        );
        if !matches {
            return Err(GeometryErrorKind::Invalid(
                "storage target does not match the producer's final admitted storage".into(),
            )
            .into());
        }
        Ok(self)
    }
}

pub(in crate::geometry) fn admit_flat_rows<I, W: Work>(
    storage: Arc<GeometryArrayStorage>,
    missing: Option<ValidityMask>,
    storage_layout: StorageLayout,
    layout: &mut FlatLiteralLayout<I>,
    work: &mut W,
) -> Result<ArrayRows> {
    work.charge_into_error(
        u64::try_from(layout.rows.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let (rows, row_map) = ArrayRows::admit_with_row_map(
        storage,
        missing,
        storage_layout,
        std::mem::take(&mut layout.rows),
        work,
    )?;
    layout.rows = row_map;
    Ok(rows)
}

impl<I> SealedOutput<I> {
    pub(in crate::geometry) const fn shape_layout(&self) -> Option<&ShapeLiteralLayout<I>> {
        match &self.buffers.storage {
            FrozenStorage::Scalar { layout, .. } => Some(layout),
            FrozenStorage::FlatScalar { .. }
            | FrozenStorage::Rows { .. }
            | FrozenStorage::Groups { .. } => None,
        }
    }

    pub(in crate::geometry) fn into_publication_parts(
        self,
    ) -> (
        LiteralStorage,
        Option<ShapeLiteralLayout<I>>,
        Vec<CheckedLiteralRecord<I>>,
    ) {
        let (storage, layout) = match self.buffers.storage {
            FrozenStorage::Scalar { shape, layout } => {
                (LiteralStorage::Scalar(shape), Some(layout))
            },
            FrozenStorage::FlatScalar { shape, .. } => (LiteralStorage::Scalar(shape), None),
            FrozenStorage::Rows { rows, .. } => (LiteralStorage::Rows(rows), None),
            FrozenStorage::Groups { groups, .. } => (LiteralStorage::Groups(groups), None),
        };
        (storage, layout, self.checked)
    }

    #[cfg(test)]
    pub(in crate::geometry) fn into_shape(self) -> Result<Shape> {
        match self.buffers.storage {
            FrozenStorage::Scalar { shape, .. } | FrozenStorage::FlatScalar { shape, .. } => {
                Ok(shape)
            },
            FrozenStorage::Rows { .. } | FrozenStorage::Groups { .. } => {
                Err(GeometryErrorKind::Invalid(
                    "non-scalar sealed geometry cannot be adopted as a scalar shape".into(),
                )
                .into())
            },
        }
    }
}
