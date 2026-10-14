use super::receipt::{ProducerImageId, ProducerLiteral, PublicationLiteral};
use crate::error::Result;
use crate::geometry::embedding_completion::literals::LiteralAddress;
use crate::geometry::embedding_completion::storage::LiteralReceiver;
use crate::geometry::{CoordinateAxes, GeometryErrorKind, Point};
use crate::work::Work;

pub(super) struct CheckedLiteralSnapshot<I> {
    records: Vec<PublicationLiteral<I>>,
}

impl<I: Copy + Ord> CheckedLiteralSnapshot<I> {
    pub(super) fn new(mut records: Vec<PublicationLiteral<I>>) -> Result<Self> {
        let key = |literal: &PublicationLiteral<I>| (literal.address, literal.image);
        if records
            .array_windows::<2>()
            .any(|[left, right]| key(left) > key(right))
        {
            records.sort_unstable_by_key(key);
        }
        for [left, right] in records.array_windows::<2>() {
            if key(left) == key(right) && left.record != right.record {
                return Err(GeometryErrorKind::Invalid(
                    "checked publication occurrences disagree on one literal identity".into(),
                )
                .into());
            }
        }
        records.dedup_by(|current, previous| key(current) == key(previous));
        Ok(Self { records })
    }
}

impl CheckedLiteralSnapshot<ProducerImageId> {
    pub(super) fn into_producer_literals(self) -> Vec<ProducerLiteral> {
        self.records
    }
}

impl<I: Copy + Ord> LiteralReceiver<I> for CheckedLiteralSnapshot<I> {
    fn accepts_point<W: Work>(
        &mut self,
        address: LiteralAddress,
        image: I,
        record: Point,
        expected_axes: CoordinateAxes,
        stored_axes: CoordinateAxes,
        _work: &mut W,
    ) -> Result<bool> {
        let Ok(index) = self
            .records
            .binary_search_by_key(&(address, image), |literal| {
                (literal.address, literal.image)
            })
        else {
            return Ok(false);
        };
        Ok(self.records.get(index).is_some_and(|literal| {
            expected_axes == stored_axes
                && CoordinateAxes::from_point(record) == stored_axes
                && record == literal.record
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::receipt::{CheckedPublicationLiteral, PublicationImage};
    use super::*;
    use crate::geometry::topology::{EmissionPortId, QSourceImage};
    use crate::geometry::{MOrdinate, ZOrdinate};
    use crate::work::Unmetered;

    fn address(
        operation: u32,
        logical_row: usize,
        group_row: Option<usize>,
        value_row: Option<usize>,
        output: u32,
    ) -> LiteralAddress {
        LiteralAddress {
            operation,
            logical_row,
            group_row,
            value_row,
            output,
        }
    }

    fn literal(
        address: LiteralAddress,
        image: PublicationImage,
        record: Point,
    ) -> CheckedPublicationLiteral {
        CheckedPublicationLiteral {
            address,
            image,
            record,
        }
    }

    #[test]
    fn snapshot_sorts_and_resolves_complete_keys() {
        let base = address(0, 0, None, None, 0);
        let large_image = PublicationImage::ProducerDrawn(ProducerImageId::new(usize::MAX));
        let base_point = Point::new_raw_xy(1.0, -0.0);
        let logical_point = Point::new_raw_xy(2.0, 3.0);
        let group_point = Point::new_raw_xy(4.0, 5.0);
        let value_point = Point::new_raw_xy(6.0, 7.0);
        let output_point = Point::new_raw_xy(8.0, 9.0);
        let operation_point = Point::new_raw_xy(10.0, 11.0);
        let native_point = Point::new_raw_xy(12.0, 13.0);
        let source_point = Point::new_raw_xy(14.0, 15.0);

        let mut snapshot = CheckedLiteralSnapshot::new(vec![
            literal(address(5, 0, None, None, 0), large_image, operation_point),
            literal(
                base,
                PublicationImage::SourceOnly(QSourceImage::PublicationSample(0)),
                source_point,
            ),
            literal(address(0, 0, None, None, 4), large_image, output_point),
            literal(
                base,
                PublicationImage::Native(EmissionPortId(0)),
                native_point,
            ),
            literal(address(0, 0, None, Some(3), 0), large_image, value_point),
            literal(base, large_image, base_point),
            literal(address(0, 1, None, None, 0), large_image, logical_point),
            literal(base, large_image, base_point),
            literal(address(0, 0, Some(2), None, 0), large_image, group_point),
        ])
        .unwrap();

        assert_eq!(snapshot.records.len(), 8);
        assert!(
            snapshot.records.array_windows::<2>().all(|[left, right]| {
                (left.address, left.image) <= (right.address, right.image)
            }),
            "the immutable snapshot retains complete-key order"
        );
        let mut work = Unmetered;
        assert!(
            LiteralReceiver::accepts_point(
                &mut snapshot,
                address(0, 1, None, None, 0),
                large_image,
                logical_point,
                CoordinateAxes::XY,
                CoordinateAxes::XY,
                &mut work,
            )
            .unwrap()
        );
        for (key, image, point) in [
            (address(0, 0, Some(2), None, 0), large_image, group_point),
            (address(0, 0, None, Some(3), 0), large_image, value_point),
            (address(0, 0, None, None, 4), large_image, output_point),
            (address(5, 0, None, None, 0), large_image, operation_point),
            (
                base,
                PublicationImage::Native(EmissionPortId(0)),
                native_point,
            ),
            (
                base,
                PublicationImage::SourceOnly(QSourceImage::PublicationSample(0)),
                source_point,
            ),
            (base, large_image, base_point),
        ] {
            assert!(
                LiteralReceiver::accepts_point(
                    &mut snapshot,
                    key,
                    image,
                    point,
                    CoordinateAxes::XY,
                    CoordinateAxes::XY,
                    &mut work,
                )
                .unwrap()
            );
        }
        assert!(
            !LiteralReceiver::accepts_point(
                &mut snapshot,
                base,
                PublicationImage::ProducerDrawn(ProducerImageId::new(17)),
                base_point,
                CoordinateAxes::XY,
                CoordinateAxes::XY,
                &mut work,
            )
            .unwrap(),
            "readback rejects a record for an absent full key"
        );
        assert!(
            !LiteralReceiver::accepts_point(
                &mut snapshot,
                base,
                large_image,
                Point::new_raw_xy(1.0, 0.0),
                CoordinateAxes::XY,
                CoordinateAxes::XY,
                &mut work,
            )
            .unwrap(),
            "readback rejects a changed signed-zero bit"
        );
    }

    #[test]
    fn snapshot_rejects_conflicting_duplicate_records_and_active_m() {
        let address = address(0, 3, Some(1), Some(2), 4);
        let image = PublicationImage::ProducerDrawn(ProducerImageId::new(0));
        let original = Point::new_raw_axes(1.0, 2.0, ZOrdinate(Some(3.0)), MOrdinate(Some(4.0)));
        let changed_m = Point::new_raw_axes(1.0, 2.0, ZOrdinate(Some(3.0)), MOrdinate(Some(5.0)));

        assert!(
            CheckedLiteralSnapshot::new(vec![
                literal(address, image, original),
                literal(address, image, changed_m),
            ])
            .is_err(),
            "the complete producer record includes active Z and M ordinates"
        );
        assert!(
            CheckedLiteralSnapshot::new(vec![
                literal(address, image, Point::new_raw_xy(0.0, 1.0)),
                literal(address, image, Point::new_raw_xy(-0.0, 1.0)),
            ])
            .is_err(),
            "bitwise point identity does not merge signed-zero occurrences"
        );
    }
}
