use std::sync::Arc;

use crate::Frame;
use crate::array::StreamingShapes;
use crate::error::Result;
use crate::geometry::Shape;
use crate::geometry::embedding_completion::literals::LiteralRowLayout;
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::publication::{
    FinishedGeometry, ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, StorageTarget,
    finish_producer_drawing,
};
use crate::work::Unmetered;

pub(crate) struct SplitDrawing {
    pub(super) shape: Shape,
}

pub(super) struct SplitRowDrawing {
    pub(super) shape: Shape,
    pub(super) layout: LiteralRowLayout<ProducerImageId>,
    pub(super) literals: Vec<ProducerLiteral>,
    pub(super) next_image: usize,
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
        let row = self.into_row(0, 0)?;
        let buffers = FrozenLiteralBuffers::shapes(row.shape, row.layout.layout);
        finish_producer_drawing(
            ProducerDrawingReceipt::new(buffers, row.literals),
            frame,
            StorageTarget::Scalar,
        )
    }

    pub(super) fn into_row(
        self,
        logical_row: usize,
        image_start: usize,
    ) -> Result<SplitRowDrawing> {
        let mut next_image = image_start;
        let mut outputs = (0, 0, 0);
        let (shape_layout, literals) = ProducerDrawingReceipt::drawing_layout(
            &self.shape,
            logical_row,
            None,
            None,
            &mut next_image,
            &mut outputs,
        )?;
        Ok(SplitRowDrawing {
            shape: self.shape,
            layout: LiteralRowLayout {
                operation: 0,
                logical_row,
                group_row: None,
                value_row: None,
                layout: shape_layout,
            },
            literals,
            next_image,
        })
    }
}

pub(crate) struct SplitArrayPublisher {
    writer: StreamingShapes,
    row_map: Vec<Option<LiteralRowLayout<ProducerImageId>>>,
    literals: Vec<ProducerLiteral>,
    next_image: usize,
}

impl SplitArrayPublisher {
    pub(crate) fn with_capacity(rows: usize) -> Self {
        Self {
            writer: StreamingShapes::with_capacity(0, rows),
            row_map: Vec::with_capacity(rows),
            literals: Vec::new(),
            next_image: 0,
        }
    }

    pub(crate) fn push_missing(&mut self) -> Result<()> {
        self.writer.try_push_missing()?;
        self.row_map.push(None);
        Ok(())
    }

    pub(crate) fn push(&mut self, logical_row: usize, drawing: SplitDrawing) -> Result<()> {
        let row = drawing.into_row(logical_row, self.next_image)?;
        self.writer.try_push(row.shape)?;
        self.row_map.push(Some(row.layout));
        self.literals.extend(row.literals);
        self.next_image = row.next_image;
        Ok(())
    }

    pub(crate) fn finish(self, frame: Frame) -> Result<FinishedGeometry> {
        let (storage, missing, layout) = self.writer.finish_parts()?;
        let mut work = Unmetered;
        let buffers = FrozenLiteralBuffers::rows(
            Arc::new(storage),
            missing,
            layout,
            self.row_map,
            &mut work,
        )?;
        finish_producer_drawing(
            ProducerDrawingReceipt::new(buffers, self.literals),
            frame,
            StorageTarget::Array,
        )
    }
}
