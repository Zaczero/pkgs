//! Exact DE-9IM matrices derived from domain-owned topology.
//!
//! Cartesian pairs use the exact partition; geographic pairs use one exact
//! source-chart Q with global faces and source-row occupancy. Named predicates
//! and pattern matching project their answers from the matrix owned here.
//!
//! A `MultiLineString` follows the OGC Simple Feature Access mod-2 boundary
//! rule: an endpoint belongs to the collection boundary when it occurs in an
//! odd number of member `LineString` boundaries. An even-incidence shared node
//! is therefore part of the collection interior. This is the rule specified
//! for `MultiCurve` in OGC 06-103r4, §§6.1.8–6.1.9; the DE-9IM cells are the
//! interior/boundary/exterior intersections described in §6.1.14.2:
//! <https://docs.ogc.org/is/06-103r4/06-103r4.pdf>.

use crate::geometry::Dimension;

mod de9im;
#[cfg(test)]
mod evaluator;
mod graph_matrix;
pub(in crate::geometry) use graph_matrix::exact_cartesian_relate;
mod interval;
mod matrix_predicates;
mod operands;

pub(crate) use de9im::{De9im, Loc};
pub(crate) use graph_matrix::{
    GeographicPrepared, graph_intersects_data_geographic, graph_relate_data,
    graph_relate_data_geographic, graph_relate_shapes, is_axis_aligned_rectangle,
    line_scalar_prefers_direct_polygon_relate,
};
pub(crate) use interval::{De9imInterval, OperandShape};
#[cfg(test)]
pub(crate) use operands::polygon_has_nondegenerate_area;
pub(crate) use operands::{line_has_nonzero_segment, multiline_has_nonzero_segment};

pub(crate) const fn dimension_entry(dimension: Dimension) -> u8 {
    match dimension {
        Dimension::Point => b'0',
        Dimension::Curve => b'1',
        Dimension::Surface => b'2',
    }
}
