//! Source-frame vertex provenance for operations whose output vertices are
//! selected from, or interpolated on, the original input geometry.

use crate::error::Result;
use crate::geometry::{Point, SegmentProjection};

/// How one output vertex derives from the operation's source-frame input.
///
/// This type deliberately cannot hold a coordinate computed in another frame:
/// the only representable outputs are an exact source vertex copy or an
/// interpolation along a source segment. Local-projection kernels may use a
/// projected frame to decide this plan, but final coordinates are emitted by
/// [`emit_from_original`] from the original input bits.
#[derive(Clone, Debug)]
pub(crate) enum VertexProvenance {
    Input(usize),
    OnSegment {
        i: usize,
        j: usize,
        projection: SegmentProjection,
    },
}

/// Per-edge subdivision counts for one original line or ring chain.
///
/// The chain ordinal is in the geometry's logical line/ring traversal order.
/// Counts preserve which generated support segments belong to each original
/// edge without treating inserted coordinates as new vertex sites.
#[derive(Clone, Debug)]
pub(crate) struct RefinedChainProvenance {
    pub(crate) chain_index: usize,
    /// End offsets of original edges in the refined chain's segment sequence.
    pub(crate) piece_ends: Vec<usize>,
}

/// Materialize a vertex-provenance plan in the original source frame.
///
/// `Input(i)` is a bit-exact copy of `original[i]`. `OnSegment` interpolates
/// directly on the original segment, so projected coordinates can never leak
/// into a copy/partial-output operation.
pub(crate) fn emit_from_original(
    original: &[Point],
    plan: &[VertexProvenance],
) -> Result<Vec<Point>> {
    let mut output = Vec::with_capacity(plan.len());
    for vertex in plan {
        let point = match vertex {
            VertexProvenance::Input(index) => *original
                .get(*index)
                .expect("input provenance index is within the source geometry"),
            VertexProvenance::OnSegment { i, j, projection } => projection.interpolate_point(
                *original
                    .get(*i)
                    .expect("segment provenance start is within the source geometry"),
                *original
                    .get(*j)
                    .expect("segment provenance end is within the source geometry"),
            )?,
        };
        output.push(point);
    }
    Ok(output)
}
