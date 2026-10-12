use std::collections::BTreeSet;

use super::{
    Candidate, ExactChartPoint, ExactGeographicTopology, IssueKind, SourcePolygonId, SourceRingId,
    compare_witness, offer, ring_witness,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum FaceIssue {
    OutsideHole(SourcePolygonId, SourceRingId),
    HoleOverlap(SourcePolygonId, SourceRingId, SourceRingId),
    PolygonOverlap(SourcePolygonId, SourcePolygonId),
}

pub(super) fn face_issues(
    topology: &ExactGeographicTopology,
    best: &mut Option<Candidate>,
) -> Result<()> {
    let mut issues = BTreeSet::new();
    for face in &topology.q.face_validity {
        if let Some((polygon, ring)) = face.outside_hole {
            issues.insert(FaceIssue::OutsideHole(polygon, ring));
        }
        if let Some((polygon, first, second)) = face.hole_overlap {
            issues.insert(FaceIssue::HoleOverlap(polygon, first, second));
        }
        if let Some((first, second)) = face.polygon_overlap {
            issues.insert(FaceIssue::PolygonOverlap(first, second));
        }
    }
    for issue in issues {
        match issue {
            FaceIssue::OutsideHole(polygon, ring) => offer(
                best,
                Candidate {
                    kind: IssueKind::HoleOutsideShell,
                    polygon,
                    other_polygon: None,
                    rings: Some((ring, None)),
                    witness: ring_witness(topology, ring)?,
                },
                &topology.sources,
            )?,
            FaceIssue::HoleOverlap(polygon, first, second) => offer(
                best,
                Candidate {
                    kind: IssueKind::HoleOverlap,
                    polygon,
                    other_polygon: None,
                    rings: Some((first, Some(second))),
                    witness: minimum_witness(
                        ring_witness(topology, first)?,
                        ring_witness(topology, second)?,
                    )?,
                },
                &topology.sources,
            )?,
            FaceIssue::PolygonOverlap(first, second) => {
                let first_shell = topology
                    .sources
                    .polygons
                    .get(first.0 as usize)
                    .and_then(|polygon| topology.sources.rings.get(polygon.rings.start))
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q overlap has no first polygon shell".into())
                    })?;
                let second_shell = topology
                    .sources
                    .polygons
                    .get(second.0 as usize)
                    .and_then(|polygon| topology.sources.rings.get(polygon.rings.start))
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q overlap has no second polygon shell".into())
                    })?;
                offer(
                    best,
                    Candidate {
                        kind: IssueKind::PolygonOverlap,
                        polygon: first.min(second),
                        other_polygon: Some(first.max(second)),
                        rings: None,
                        witness: minimum_witness(
                            ring_witness(topology, first_shell.id)?,
                            ring_witness(topology, second_shell.id)?,
                        )?,
                    },
                    &topology.sources,
                )?;
            },
        }
    }
    Ok(())
}

fn minimum_witness(left: ExactChartPoint, right: ExactChartPoint) -> Result<ExactChartPoint> {
    if compare_witness(&left, &right)?.is_le() {
        Ok(left)
    } else {
        Ok(right)
    }
}
