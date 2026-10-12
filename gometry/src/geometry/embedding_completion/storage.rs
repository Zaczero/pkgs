use std::sync::Arc;

use super::{
    ArrayRows, CoordinateAxes, ExactChartPoint, GeometryArrayStorage, GeometryErrorKind,
    LiteralAddress, LiteralRowLayout, Point, ResourceError, Result, Shape, ShapeLiteralLayout,
    StorageLayout, StorageTarget, ValidityMask, Work,
};

#[derive(Clone)]
pub(in crate::geometry) struct CheckedLiteralRecord<I> {
    pub(in crate::geometry) address: LiteralAddress,
    pub(in crate::geometry) image: I,
    pub(in crate::geometry) record: Point,
    pub(in crate::geometry) chart: ExactChartPoint,
}

pub(in crate::geometry) enum LiteralRowBinding<I> {
    Checked(LiteralRowLayout<I>),
    OtherProducer,
}

/// Owns the real finalized sink and its physical-slot-to-logical-use map.
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
    Rows {
        rows: ArrayRows,
        row_map: Arc<[Option<LiteralRowBinding<I>>]>,
        visit_rows: std::ops::Range<usize>,
    },
    Groups {
        groups: FinalGroupRows,
        row_map: Vec<Option<LiteralRowBinding<I>>>,
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

pub(in crate::geometry) trait CheckedImages<I> {
    fn image(&self, address: LiteralAddress, image: I) -> Result<&ExactChartPoint>;
}

pub(in crate::geometry) trait LiteralChart<I> {
    fn read_point<W: Work>(
        &mut self,
        address: LiteralAddress,
        image: I,
        record: Point,
        expected_axes: CoordinateAxes,
        stored_axes: CoordinateAxes,
        work: &mut W,
    ) -> Result<Option<ExactChartPoint>>;
}

pub(in crate::geometry) trait LiteralReceiver<I>:
    CheckedImages<I> + LiteralChart<I>
{
}

impl<I, R> LiteralReceiver<I> for R where R: CheckedImages<I> + LiteralChart<I> {}

impl<I> FrozenLiteralBuffers<I> {
    pub(in crate::geometry) const fn shapes(
        shape: Shape,
        locations: ShapeLiteralLayout<I>,
    ) -> Self {
        Self {
            storage: FrozenStorage::Scalar {
                shape,
                layout: locations,
            },
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "array receipt producers provide their final admitted storage and row map"
        )
    )]
    pub(in crate::geometry) fn rows<W: Work>(
        storage: Arc<GeometryArrayStorage>,
        missing: Option<ValidityMask>,
        layout: StorageLayout,
        row_map: Vec<Option<LiteralRowLayout<I>>>,
        work: &mut W,
    ) -> Result<Self> {
        let row_map = row_map
            .into_iter()
            .map(|row| row.map(LiteralRowBinding::Checked))
            .collect();
        Self::rows_with_producer_bindings(storage, missing, layout, row_map, work)
    }

    pub(in crate::geometry) fn rows_with_producer_bindings<W: Work>(
        storage: Arc<GeometryArrayStorage>,
        missing: Option<ValidityMask>,
        layout: StorageLayout,
        row_map: Vec<Option<LiteralRowBinding<I>>>,
        work: &mut W,
    ) -> Result<Self> {
        work.charge(u64::try_from(row_map.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let (rows, row_map) =
            ArrayRows::admit_with_row_map(storage, missing, layout, row_map, work)?;
        Ok(Self {
            storage: FrozenStorage::Rows {
                visit_rows: 0..rows.len(),
                rows,
                row_map: row_map.into(),
            },
        })
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Groups receipt producers provide final values, group CSR, validity, and row maps"
        )
    )]
    pub(in crate::geometry) fn groups<W: Work>(
        storage: Arc<GeometryArrayStorage>,
        value_missing: Option<ValidityMask>,
        value_layout: StorageLayout,
        value_map: Vec<Option<LiteralRowLayout<I>>>,
        offsets: Vec<i64>,
        missing: Option<ValidityMask>,
        work: &mut W,
    ) -> Result<Self> {
        let value_map = value_map
            .into_iter()
            .map(|row| row.map(LiteralRowBinding::Checked))
            .collect();
        Self::groups_with_producer_bindings(
            storage,
            value_missing,
            value_layout,
            value_map,
            offsets,
            missing,
            work,
        )
    }

    pub(in crate::geometry) fn groups_with_producer_bindings<W: Work>(
        storage: Arc<GeometryArrayStorage>,
        value_missing: Option<ValidityMask>,
        value_layout: StorageLayout,
        value_map: Vec<Option<LiteralRowBinding<I>>>,
        offsets: Vec<i64>,
        missing: Option<ValidityMask>,
        work: &mut W,
    ) -> Result<Self> {
        work.charge(u64::try_from(value_map.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let (values, value_map) =
            ArrayRows::admit_with_row_map(storage, value_missing, value_layout, value_map, work)?;
        Ok(Self {
            storage: FrozenStorage::Groups {
                groups: FinalGroupRows {
                    values,
                    offsets,
                    missing,
                },
                row_map: value_map,
            },
        })
    }

    pub(in crate::geometry) fn admitted_row_view(
        rows: ArrayRows,
        row_map: Arc<[Option<LiteralRowBinding<I>>]>,
        logical_row: usize,
    ) -> Result<Self> {
        let end = logical_row
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        if end > rows.len() || row_map.len() != rows.len() {
            return Err(GeometryErrorKind::Invalid(
                "final array row view is outside its admitted owner".into(),
            )
            .into());
        }
        Ok(Self {
            storage: FrozenStorage::Rows {
                rows,
                row_map,
                visit_rows: logical_row..end,
            },
        })
    }

    pub(in crate::geometry) fn admit_target(self, target: StorageTarget) -> Result<Self> {
        let Self { storage } = self;
        match (target, storage) {
            (StorageTarget::Scalar, FrozenStorage::Scalar { shape, layout }) => Ok(Self {
                storage: FrozenStorage::Scalar { shape, layout },
            }),
            (
                StorageTarget::Array,
                FrozenStorage::Rows {
                    rows,
                    row_map,
                    visit_rows,
                },
            ) => Ok(Self {
                storage: FrozenStorage::Rows {
                    rows,
                    row_map,
                    visit_rows,
                },
            }),
            (StorageTarget::Groups, FrozenStorage::Groups { groups, row_map }) => Ok(Self {
                storage: FrozenStorage::Groups { groups, row_map },
            }),
            (StorageTarget::Scalar | StorageTarget::Array | StorageTarget::Groups, _) => {
                Err(GeometryErrorKind::Invalid(
                    "storage target does not match the producer's final admitted storage".into(),
                )
                .into())
            },
        }
    }
}

impl<I> SealedOutput<I> {
    pub(in crate::geometry) const fn shape_layout(&self) -> Option<&ShapeLiteralLayout<I>> {
        match &self.buffers.storage {
            FrozenStorage::Scalar { layout, .. } => Some(layout),
            FrozenStorage::Rows { .. } | FrozenStorage::Groups { .. } => None,
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
            FrozenStorage::Rows { rows, .. } => (LiteralStorage::Rows(rows), None),
            FrozenStorage::Groups { groups, .. } => (LiteralStorage::Groups(groups), None),
        };
        (storage, layout, self.checked)
    }

    #[cfg(test)]
    pub(in crate::geometry) fn into_shape(self) -> Result<Shape> {
        match self.buffers.storage {
            FrozenStorage::Scalar { shape, .. } => Ok(shape),
            FrozenStorage::Rows { .. } | FrozenStorage::Groups { .. } => {
                Err(GeometryErrorKind::Invalid(
                    "non-scalar sealed geometry cannot be adopted as a scalar shape".into(),
                )
                .into())
            },
        }
    }
}
