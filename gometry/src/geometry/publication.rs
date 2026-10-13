//! Shared receipt and final-storage boundary for geographic publication.

mod binding;
mod chart_restriction;
mod finish;
mod receipt;
mod storage;
#[cfg(test)]
mod tests;

pub(crate) use chart_restriction::{
    ChartCutSide, ChartRestriction, DirectedChartRestrictionUse, SyntheticChartVertex,
};
pub(crate) use finish::FinishedGeometry;
pub(in crate::geometry) use finish::finish_geographic;
pub(crate) use receipt::{GeometryOutput, PendingPublicationRow};
pub(in crate::geometry) use receipt::{
    ProducerDrawingReceipt, ProducerImageId, ProducerLiteral, ProducerReceipt,
    ProducerSourceRootUse, PublicationCall, PublicationOutcome, PublicationRejection,
    StorageTarget,
};
pub(crate) use storage::PublicationArrayRows;
