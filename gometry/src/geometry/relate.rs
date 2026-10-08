//! Exact DE-9IM matrices derived from domain-owned topology.
//!
//! Cartesian pairs use exact source-contact witnesses; geographic pairs use
//! their domain-specific exact chart relation. Named predicates and pattern
//! matching share the same DE-9IM evidence model.
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
mod evaluator;
mod graph_matrix;
mod interval;
mod matrix_predicates;
mod operands;

pub(crate) use de9im::{De9im, Loc};
pub(crate) use evaluator::{
    pattern as evaluate_cartesian_pattern, predicate as evaluate_cartesian_predicate,
    predicate_prepared_row as evaluate_cartesian_prepared_row, predicate_rows,
};
pub(crate) use graph_matrix::{
    GeographicPrepared, graph_intersects_data_geographic, graph_relate_data,
    graph_relate_data_geographic, graph_relate_shapes, is_axis_aligned_rectangle,
    is_axis_aligned_rectangle_row,
};
pub(crate) use interval::{De9imInterval, OperandShape};
pub(crate) use operands::{line_has_nonzero_segment, multiline_has_nonzero_segment};

pub(crate) const fn dimension_entry(dimension: Dimension) -> u8 {
    match dimension {
        Dimension::Point => b'0',
        Dimension::Curve => b'1',
        Dimension::Surface => b'2',
    }
}
