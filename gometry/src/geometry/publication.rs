//! Shared receipt and final-storage boundary for geographic publication.

mod binding;
mod chart_restriction;
mod finish;
mod receipt;
mod routes;
mod storage;
#[cfg(test)]
mod tests;

pub(crate) use chart_restriction::{
    ChartCutSide, ChartRestriction, DirectedChartRestrictionUse, SyntheticChartVertex,
};
pub(crate) use finish::FinishedGeometry;
pub(in crate::geometry) use finish::{finish_geographic, finish_producer_drawing};
pub(crate) use receipt::{GeometryOutput, PendingPublicationRow};
pub(in crate::geometry) use receipt::{
    ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, ProducerReceipt, PublicationCall,
    PublicationOutcome, PublicationRejection, StorageTarget,
};
pub(crate) use routes::{
    finish_geographic_line, finish_geographic_nearest_point,
    finish_geographic_nearest_point_columns, finish_geographic_point_selection,
    finish_geographic_snap, geographic_line_pending_row, geographic_point_selection_pending_row,
    geographic_snap_pending_row, publish_split_groups, publish_split_parts_array,
};
pub(crate) use storage::PublicationRows;
