//! Source-frame vertex provenance for operations whose output vertices are
//! selected from, or interpolated on, the original input geometry.

use crate::geometry::{HashSet, Point, PointKey, SegmentProjection};

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

/// Materialize a vertex-provenance plan in the original source frame.
///
/// `Input(i)` is a bit-exact copy of `original[i]`. `OnSegment` interpolates
/// directly on the original segment, so projected coordinates can never leak
/// into a copy/partial-output operation.
pub(crate) fn emit_from_original(original: &[Point], plan: &[VertexProvenance]) -> Vec<Point> {
    let input_xy: HashSet<PointKey> = original.iter().map(|&point| PointKey::new(point)).collect();
    let mut output = Vec::with_capacity(plan.len());
    for vertex in plan {
        let point = match vertex {
            VertexProvenance::Input(index) => original[*index],
            VertexProvenance::OnSegment { i, j, projection } => {
                projection.interpolate_point(original[*i], original[*j])
            },
        };
        if matches!(vertex, VertexProvenance::Input(_)) {
            debug_assert!(input_xy.contains(&PointKey::new(point)));
        }
        output.push(point);
    }
    output
}
