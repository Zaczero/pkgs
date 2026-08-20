//! Native DE-9IM relate for areal × areal operands, derived from the
//! joint per-operand winding arrangement — no geo-rs intersection-matrix
//! machinery.
//!
//! Every matrix entry falls out of the arrangement structure:
//! - The INTERIOR row/column entries are face properties: a face with winding
//!   `>= 1` on a side lies in that operand's interior, so `II`/`IE`/`EI` are
//!   2-dimensional wherever such faces exist (open sets intersect openly), and
//!   `EE` is always `2` (the unbounded face).
//! - The BOUNDARY entries are edge-piece properties. Noding splits every input
//!   edge at crossings, so each atomic piece lies wholly in the other operand's
//!   interior, exterior, or along its boundary: a piece carrying BOTH operands'
//!   weights is shared boundary (`BB = 1`); a one-operand piece classifies by
//!   the other operand's winding on its side faces (constant across a
//!   one-operand edge). A boundary crossing or corner touch without a shared
//!   run is a shared NODE (`BB = 0`); a boundary point inside the other's OPEN
//!   interior always extends to a 1-dimensional piece, so `IB`/`BI`/`BE`/`EB`
//!   are `1` or `F`, never `0`.

mod areal;
mod de9im;
mod lineal;
mod mixed;
mod native;
mod operands;
mod topo;

#[cfg(test)]
pub(crate) use areal::areal_relate_arrangement_oracle;
pub(crate) use areal::{areal_relate_data, areal_relate_pattern_shapes, areal_relate_shapes};
pub(crate) use de9im::{De9im, Loc};
pub(crate) use lineal::lineal_relate_shapes;
pub(crate) use mixed::{
    fully_covered, group_intervals_by_index, mixed_relate_data, mixed_relate_shapes,
    projection_interval,
};
pub(crate) use native::{
    collection_has_overlapping_lineal_members, native_relate_data, native_relate_data_for,
    native_relate_pattern_shapes, native_relate_shapes, shape_has_polygonal_members,
    shape_has_puntal_support,
};
pub(crate) use operands::{
    LinealOperand, PuntalOperand, effective_dimension, empty_relate, line_has_nonzero_segment,
    line_is_collapsed, multiline_has_nonzero_segment, multiline_is_collapsed,
    polygon_has_nondegenerate_area, polygon_parts, puntal_relate,
};
pub(crate) use topo::{PairEdgeLabel, RelateTopology, merge_topo_loc};

#[cfg(test)]
mod tests;
