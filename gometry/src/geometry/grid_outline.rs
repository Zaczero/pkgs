use crate::Frame;
use crate::error::Result;
use crate::geometry::publication::{
    FinishedGeometry, ProducerDrawingReceipt, ProducerReceipt, PublicationCall, PublicationOutcome,
    PublicationRejection, StorageTarget, finish_geographic,
};
use crate::geometry::{GeometryErrorKind, Shape};
use crate::work::Unmetered;

pub(crate) fn finish_grid_outline(shape: Shape, frame: Frame) -> Result<FinishedGeometry> {
    if !matches!(&shape, Shape::Polygon(_) | Shape::MultiPolygon(_)) {
        return Err(GeometryErrorKind::Invalid(
            "grid outline producer returned a non-polygonal shape".into(),
        )
        .into());
    }
    let receipt = ProducerDrawingReceipt::drawing(shape)?;
    let mut work = Unmetered;
    match finish_geographic(
        PublicationCall::new(ProducerReceipt::ProducerDrawing(receipt), frame, &mut work),
        StorageTarget::Scalar,
    )? {
        PublicationOutcome::Pass(finished) => Ok(finished),
        PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
            Err(GeometryErrorKind::Invalid(
                format!(
                    "final grid-outline storage changed {} checked output records",
                    images.len()
                )
                .into(),
            )
            .into())
        },
        PublicationOutcome::Reject(_) => Err(GeometryErrorKind::Invalid(
            "grid-outline finisher returned a different producer rejection family".into(),
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::finish_grid_outline;
    use crate::Frame;
    use crate::geometry::Shape;
    use crate::geometry::embedding_completion::literals::{FlatLiteralLayout, LiteralAddress};
    use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
    use crate::geometry::publication::{
        ProducerDrawingReceipt, ProducerReceipt, PublicationCall, PublicationOutcome,
        PublicationRejection, StorageTarget, finish_geographic,
    };
    use crate::work::Unmetered;

    #[test]
    fn producer_drawing_rejects_grid_outline_storage_readback_mismatch() {
        let expected = crate::io::parse_wkt("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))").unwrap();
        let corrupted = crate::io::parse_wkt("POLYGON ((0 0, 5 0, 4 4, 0 4, 0 0))").unwrap();
        let mut layout = FlatLiteralLayout::with_capacity(1);
        let mut literals = Vec::new();
        ProducerDrawingReceipt::capture_shape(
            &mut layout,
            &mut literals,
            &expected,
            LiteralAddress {
                operation: 0,
                logical_row: 0,
                group_row: None,
                value_row: None,
                output: 0,
            },
        )
        .unwrap();
        let buffers = FrozenLiteralBuffers::flat_shape(corrupted, layout);
        let receipt = ProducerDrawingReceipt::new(buffers, literals);
        let mut work = Unmetered;
        match finish_geographic(
            PublicationCall::new(
                ProducerReceipt::ProducerDrawing(receipt),
                Frame::Crs(crate::wgs84_crs()),
                &mut work,
            ),
            StorageTarget::Scalar,
        )
        .unwrap()
        {
            PublicationOutcome::Reject(PublicationRejection::ProducerDrawing(images)) => {
                assert!(!images.is_empty());
            },
            PublicationOutcome::Pass(finished) => {
                let (actual, _) = finished.into_scalar().unwrap();
                assert_eq!(
                    actual, expected,
                    "omitting the producer-drawn binder must not publish changed coordinates"
                );
            },
            PublicationOutcome::Reject(_) => {
                panic!("grid-outline storage returns its producer-drawn rejection family")
            },
        }
    }

    #[test]
    fn finish_grid_outline_reads_back_the_frozen_polygon() {
        let expected = crate::io::parse_wkt("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))").unwrap();
        let finished =
            finish_grid_outline(expected.clone(), Frame::Crs(crate::wgs84_crs())).unwrap();
        let (actual, frame) = finished.into_scalar().unwrap();
        assert_eq!(actual, expected);
        assert_eq!(frame, Frame::Crs(crate::wgs84_crs()));
        assert!(matches!(actual, Shape::Polygon(_)));
    }
}
