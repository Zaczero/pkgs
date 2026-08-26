//! Exact DE-9IM matrices derived from source-owned topology graphs.
//!
//! The graph builder preserves declared parts, mod-2 line boundaries, polygon
//! holes, and geographic quotient coordinates. Named predicates and pattern
//! matching project their answers from the matrix owned here.

mod de9im;
mod graph_matrix;
mod operands;

pub(crate) use de9im::{De9im, Loc};
pub(crate) use graph_matrix::{
    graph_intersects_rectangle_data, graph_relate_data, graph_relate_data_geographic,
    graph_relate_shapes, graph_relate_shapes_geographic,
};
pub(crate) use operands::{
    line_has_nonzero_segment, multiline_has_nonzero_segment, polygon_has_nondegenerate_area,
};
