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

pub(crate) use areal::*;
pub(crate) use de9im::*;
pub(crate) use lineal::*;
pub(crate) use mixed::*;
pub(crate) use native::*;
pub(crate) use operands::*;
pub(crate) use topo::*;

#[cfg(test)]
mod tests;
