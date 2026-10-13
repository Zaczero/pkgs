use super::q::ExactGeographicTopology;
use crate::error::Result;
use crate::geometry::Shape;
use crate::geometry::publication::ProducerDrawingReceipt;
use crate::geometry::storage::ValidityMask;
use crate::work::Work;

mod analysis;
mod faces;
mod gaps;
mod invalid_edges;
mod regions;
mod regions_emission;
#[cfg(test)]
mod tests;

pub(crate) use analysis::{analyze_geographic_coverage, analyze_geographic_coverage_with_work};

pub(crate) struct GeographicCoverageAnalysis {
    pub(crate) topology: ExactGeographicTopology,
    pub(crate) face_rows: Vec<Vec<usize>>,
    pub(crate) span_rows: Vec<Vec<usize>>,
    invalid_source_edge_flags: Vec<bool>,
    invalid_source_owners: Vec<super::q::QSpanOwner>,
}

impl GeographicCoverageAnalysis {
    pub(crate) fn is_valid(&self) -> bool {
        self.invalid_source_edge_flags
            .iter()
            .all(|invalid| !invalid)
            && self.face_rows.iter().all(|rows| rows.len() <= 1)
    }

    pub(crate) fn capture_invalid_source_owners(&mut self, work: &mut impl Work) -> Result<()> {
        self.invalid_source_owners =
            invalid_edges::capture(&self.topology, &self.invalid_source_edge_flags, work)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn invalid_source_shapes(&self, work: &mut impl Work) -> Result<Vec<Shape>> {
        invalid_edges::project_shapes(
            &self.topology,
            &self.invalid_source_edge_flags,
            &self.invalid_source_owners,
            work,
        )
    }

    pub(in crate::geometry) fn invalid_edges_drawing_receipt(
        &self,
        missing: Option<ValidityMask>,
        work: &mut impl Work,
    ) -> Result<ProducerDrawingReceipt> {
        invalid_edges::producer_drawing_receipt(self, missing, work)
    }

    pub(crate) fn has_shared_boundary(&self) -> bool {
        self.topology.sources.row_count > 1 && self.span_rows.iter().any(|rows| rows.len() > 1)
    }

    pub(crate) fn source_edges_match_shortest_chart(&self) -> Result<bool> {
        let frame = self.topology.sources.frame;
        for edge in &self.topology.sources.edges {
            let [start, end] = edge.source;
            if !edge.source_interval_reads_as(frame, false, start, end)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn source_rings_are_ordinary(&self) -> bool {
        self.topology.sources.rings.iter().all(|ring| {
            ring.summary.chart_turn == 0
                && ring.summary.domain == crate::geographic::RingDomain::Ordinary
                && !ring.summary.exact_axis_band
        })
    }
}

pub(crate) struct GeographicCoverageFaceAssignment {
    pub(crate) owners: Vec<Option<usize>>,
    pub(crate) gap_assignments: Vec<bool>,
}
