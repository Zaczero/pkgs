use crate::Frame;
use crate::error::{ResourceError, Result};
use crate::geometry::embedding_completion::{
    FrozenLiteralBuffers, LiteralAddress, LiteralPolygon, LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::publication::{
    FinishedGeometry, ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, ProducerReceipt,
    PublicationCall, PublicationOutcome, PublicationRejection, StorageTarget, finish_geographic,
};
use crate::geometry::{GeometryErrorKind, Polygon, Ring, Shape, Unmetered};

pub(crate) fn finish_grid_outline(shape: Shape, frame: Frame) -> Result<FinishedGeometry> {
    let (layout, literals) = scalar_polygon_literals(&shape)?;
    let buffers = FrozenLiteralBuffers::shapes(shape, layout);
    let receipt = ProducerDrawingReceipt::new(buffers, literals, Vec::new(), Vec::new());
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

fn scalar_polygon_literals(
    shape: &Shape,
) -> Result<(ShapeLiteralLayout<ProducerImageId>, Vec<ProducerLiteral>)> {
    let mut polygons = Vec::new();
    let mut literals = Vec::new();
    let mut next_image = 0;

    match shape {
        Shape::Polygon(polygon) => {
            push_polygon_literals(polygon, &mut next_image, &mut literals, &mut polygons)?;
        },
        Shape::MultiPolygon(multi) => {
            for polygon in multi {
                push_polygon_literals(polygon, &mut next_image, &mut literals, &mut polygons)?;
            }
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => {
            return Err(GeometryErrorKind::Invalid(
                "grid outline producer returned a non-polygonal shape".into(),
            )
            .into());
        },
    }

    Ok((
        ShapeLiteralLayout {
            points: Vec::new(),
            lines: Vec::new(),
            polygons,
        },
        literals,
    ))
}

fn push_polygon_literals(
    polygon: &Polygon,
    next_image: &mut usize,
    literals: &mut Vec<ProducerLiteral>,
    output_layout: &mut Vec<LiteralPolygon<ProducerImageId>>,
) -> Result<()> {
    let output = u32::try_from(output_layout.len()).map_err(|_| ResourceError::SizeOverflow)?;
    let axes = polygon.shell.coords().axes();
    let shell = ring_literals(&polygon.shell, output, next_image, literals)?;
    let mut holes = Vec::with_capacity(polygon.holes.len());
    for hole in polygon.holes.iter() {
        holes.push(ring_literals(hole, output, next_image, literals)?);
    }
    output_layout.push(LiteralPolygon {
        output,
        axes,
        shell,
        holes,
    });
    Ok(())
}

fn ring_literals(
    ring: &Ring,
    output: u32,
    next_image: &mut usize,
    literals: &mut Vec<ProducerLiteral>,
) -> Result<LiteralSequence<ProducerImageId>> {
    let coords = ring.coords();
    let axes = coords.axes();
    let mut images = Vec::with_capacity(coords.len());
    for record in coords {
        let image_index = *next_image;
        *next_image = image_index
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let image = ProducerImageId::new(image_index);
        let address = LiteralAddress {
            operation: 0,
            logical_row: 0,
            group_row: None,
            value_row: None,
            output,
        };
        images.push(image);
        literals.push(ProducerLiteral::from_point(address, image, record));
    }
    Ok(LiteralSequence {
        output,
        axes,
        images,
    })
}

#[cfg(test)]
mod tests {
    use super::{finish_grid_outline, scalar_polygon_literals};
    use crate::Frame;
    use crate::geometry::embedding_completion::FrozenLiteralBuffers;
    use crate::geometry::publication::{
        ProducerDrawingReceipt, ProducerReceipt, PublicationCall, PublicationOutcome,
        PublicationRejection, StorageTarget, finish_geographic,
    };
    use crate::geometry::{Shape, Unmetered};

    #[test]
    fn producer_drawing_rejects_grid_outline_storage_readback_mismatch() {
        let expected = crate::io::parse_wkt("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))").unwrap();
        let corrupted = crate::io::parse_wkt("POLYGON ((0 0, 5 0, 4 4, 0 4, 0 0))").unwrap();
        let (layout, literals) = scalar_polygon_literals(&expected).unwrap();
        let buffers = FrozenLiteralBuffers::shapes(corrupted, layout);
        let receipt = ProducerDrawingReceipt::new(buffers, literals, Vec::new(), Vec::new());
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
