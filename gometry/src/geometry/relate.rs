//! Exact DE-9IM matrices derived from source-owned topology graphs.
//!
//! The graph builder preserves declared parts, mod-2 line boundaries, polygon
//! holes, and geographic quotient coordinates. Named predicates and pattern
//! matching project their answers from the matrix owned here.
//!
//! A `MultiLineString` follows the OGC Simple Feature Access mod-2 boundary
//! rule: an endpoint belongs to the collection boundary when it occurs in an
//! odd number of member `LineString` boundaries. An even-incidence shared node
//! is therefore part of the collection interior. This is the rule specified
//! for `MultiCurve` in OGC 06-103r4, §§6.1.8–6.1.9; the DE-9IM cells are the
//! interior/boundary/exterior intersections described in §6.1.14.2:
//! <https://docs.ogc.org/is/06-103r4/06-103r4.pdf>.

mod de9im;
mod direct;
mod graph_matrix;
mod interval;
mod operands;

pub(crate) use de9im::{De9im, Loc};
pub(crate) use interval::{De9imInterval, OperandShape};
pub(crate) use direct::{
    StagedLinework, StagedRings, direct_areal_linework_predicate, direct_areal_linework_relate,
    direct_areal_pattern, direct_areal_linework_pattern, direct_linework_pattern,
    direct_areal_predicate, direct_areal_relate, direct_linework_predicate,
    direct_linework_relate, stage_linework, stage_rings,
};
pub(crate) use graph_matrix::{
    GeographicPrepared, collect_polygons, graph_intersects_rectangle_data, graph_relate_data,
    ring_has_area, ring_is_sound,
    graph_relate_data_geographic, graph_relate_data_geographic_with_left_graph,
    graph_relate_data_geographic_with_right_graph, graph_relate_data_with_left_graph,
    graph_relate_data_with_right_graph, graph_relate_shapes, is_axis_aligned_rectangle,
    line_scalar_prefers_direct_polygon_relate,
};
pub(crate) use operands::{
    line_has_nonzero_segment, multiline_has_nonzero_segment, polygon_has_nondegenerate_area,
};
