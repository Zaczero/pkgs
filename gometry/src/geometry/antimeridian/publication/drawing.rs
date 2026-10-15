use crate::Frame;
use crate::error::Result;
use crate::geometry::Shape;
use crate::geometry::publication::{
    FinishedGeometry, ProducerDrawingReceipt, PublicationRows, StorageTarget,
    finish_producer_drawing,
};

pub(crate) struct SplitDrawing {
    pub(super) shape: Shape,
}

impl SplitDrawing {
    pub(crate) fn orient_geojson(mut self) -> Result<Self> {
        if matches!(
            &self.shape,
            Shape::Polygon(_) | Shape::MultiPolygon(_) | Shape::GeometryCollection(_)
        ) {
            self.shape = self.shape.orient_polygons(false)?;
        }
        Ok(self)
    }

    pub(crate) fn finish_scalar(self, frame: Frame) -> Result<FinishedGeometry> {
        finish_producer_drawing(
            ProducerDrawingReceipt::drawing(self.shape)?,
            frame,
            StorageTarget::Scalar,
        )
    }

    pub(crate) fn append_to(self, rows: &mut PublicationRows, logical_row: usize) -> Result<()> {
        rows.try_push_drawing(self.shape, logical_row, "split_antimeridian")
    }
}
