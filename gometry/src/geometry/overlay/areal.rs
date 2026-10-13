#[cfg(test)]
use super::nary;
#[cfg(test)]
use crate::error::Result;
#[cfg(test)]
use crate::geometry::Polygon;
#[cfg(test)]
use crate::geometry::overlay::{ExactSourceInventory, OverlayOp};
use crate::geometry::{Segment, XY, ray_crossing_is_right};

/// One segment's signed contribution to the rightward-ray winding number at
/// `probe`; closed loops sum these contributions to their winding.
pub(crate) fn ray_crossing(segment: Segment, probe: XY) -> i32 {
    let (ay, by) = (segment.start.y, segment.end.y);
    if (ay > probe.y) != (by > probe.y)
        && ray_crossing_is_right(segment.start.x, ay, segment.end.x, by, probe.x, probe.y)
    {
        return if by > ay { 1 } else { -1 };
    }
    0
}

#[cfg(test)]
pub(in crate::geometry) fn binary_areal_overlay<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
    left: &[P],
    right: &[Q],
    op: OverlayOp,
) -> Result<Vec<Polygon>> {
    let inventory = ExactSourceInventory::from_polygon_operands(left, right)?;
    let mut budget = crate::geometry::ExpansionBudget::new(op.name(), "exact partition");
    let partition = inventory.partition(&mut budget)?;
    let (_, _, polygons) = inventory.overlay_parts(&partition, op)?;
    Ok(polygons)
}

#[cfg(test)]
#[path = "areal/tests/axes.rs"]
mod axes_tests;
#[cfg(test)]
#[path = "areal/tests/contacts.rs"]
mod contact_tests;
#[cfg(test)]
#[path = "areal/tests/disjoint.rs"]
mod disjoint_tests;
#[cfg(test)]
#[path = "areal/tests/folds.rs"]
mod fold_tests;
#[cfg(test)]
#[path = "areal/tests/lower_dimensional.rs"]
mod lower_dimensional_tests;
#[cfg(test)]
#[path = "areal/tests/partition.rs"]
mod partition_tests;
#[cfg(test)]
#[path = "areal/tests/points.rs"]
mod point_tests;
#[cfg(test)]
#[path = "areal/tests/support.rs"]
mod test_support;
