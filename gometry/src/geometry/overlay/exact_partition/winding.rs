use std::ops::Range;

use super::{
    Axis, Bounds, ExactDyadic, ExactPartition, ExactSign, ExactSourceInventory, GeometryErrorKind,
    NodeId, Ordering, Result, RingKind, Segment,
};

impl ExactSourceInventory {
    #[expect(
        clippy::indexing_slicing,
        reason = "each staged source ring ID indexes the admitted ring-kind table"
    )]
    pub(super) fn winding_source_rings(&self) -> Vec<Option<u32>> {
        self.source_rings
            .iter()
            .map(|&ring| ring.filter(|&ring| self.ring_kinds[ring as usize] == RingKind::Winding))
            .collect()
    }
}

pub(super) fn select_operation(operation: crate::geometry::OverlayOp, membership: &[bool]) -> bool {
    match operation {
        crate::geometry::OverlayOp::Union => membership.iter().any(|&value| value),
        crate::geometry::OverlayOp::Intersection => membership.iter().all(|&value| value),
        crate::geometry::OverlayOp::Difference => {
            membership.first().copied().unwrap_or(false)
                && membership.iter().skip(1).all(|&value| !value)
        },
        crate::geometry::OverlayOp::SymmetricDifference => {
            membership.iter().filter(|&&value| value).count() % 2 == 1
        },
    }
}

pub(super) const fn ring_occupied(winding: i64, odd: bool) -> bool {
    if odd { winding % 2 != 0 } else { winding != 0 }
}

pub(super) fn rectangle_side_outward(
    partition: &ExactPartition<'_>,
    start: NodeId,
    end: NodeId,
    bounds: Bounds,
) -> Result<Option<[i8; 2]>> {
    for (axis, value, direction) in [
        (Axis::X, bounds.minx(), [-1, 0]),
        (Axis::X, bounds.maxx(), [1, 0]),
        (Axis::Y, bounds.miny(), [0, -1]),
        (Axis::Y, bounds.maxy(), [0, 1]),
    ] {
        if partition.compare_axis_to(start, axis, value)? == Ordering::Equal
            && partition.compare_axis_to(end, axis, value)? == Ordering::Equal
        {
            return Ok(Some(direction));
        }
    }
    Ok(None)
}

pub(in crate::geometry) fn homogeneous_midpoint(
    left: &[ExactDyadic; 3],
    right: &[ExactDyadic; 3],
) -> Result<[ExactDyadic; 3]> {
    let denominator = left[2]
        .try_product(&right[2])?
        .try_product(&ExactDyadic::from_f64(2.0))?;
    let mut x = left[0]
        .try_product(&right[2])?
        .try_add(right[0].try_product(&left[2])?)?;
    let mut y = left[1]
        .try_product(&right[2])?
        .try_add(right[1].try_product(&left[2])?)?;
    let mut denominator = denominator;
    if denominator.sign() == ExactSign::Negative {
        x = x.neg();
        y = y.neg();
        denominator = denominator.neg();
    }
    Ok([x, y, denominator])
}

pub(super) fn symbolic_ray_crossing(
    segment: Segment,
    point: &[ExactDyadic; 3],
    direction: [i8; 2],
) -> Result<i32> {
    let (start, end) = (segment.start, segment.end);
    let above = |ordinate: f64| -> Result<bool> {
        let relative = ExactDyadic::from_f64(ordinate)
            .try_product(&point[2])?
            .try_subtract(point[1].clone())?;
        Ok(match relative.sign() {
            ExactSign::Positive => true,
            ExactSign::Negative => false,
            ExactSign::Zero => direction[1] < 0,
        })
    };
    let (start_above, end_above) = (above(start.y)?, above(end.y)?);
    if start_above == end_above {
        return Ok(0);
    }

    let dx = ExactDyadic::from_f64(end.x).try_subtract(ExactDyadic::from_f64(start.x))?;
    let dy = ExactDyadic::from_f64(end.y).try_subtract(ExactDyadic::from_f64(start.y))?;
    let px = point[0]
        .clone()
        .try_subtract(ExactDyadic::from_f64(start.x).try_product(&point[2])?)?;
    let py = point[1]
        .clone()
        .try_subtract(ExactDyadic::from_f64(start.y).try_product(&point[2])?)?;
    let orientation = dx.try_product(&py)?.try_subtract(dy.try_product(&px)?)?;
    let orientation = if orientation.sign() == ExactSign::Zero {
        let dx_sign = exact_sign_i8(dx.sign());
        let dy_sign = exact_sign_i8(dy.sign());
        (dx_sign
            .wrapping_mul(direction[1])
            .wrapping_sub(dy_sign.wrapping_mul(direction[0])))
        .signum()
    } else {
        exact_sign_i8(orientation.sign())
    };
    let right = if end.y > start.y {
        orientation > 0
    } else {
        orientation < 0
    };
    if right {
        Ok(if end.y > start.y { 1 } else { -1 })
    } else {
        Ok(0)
    }
}

pub(super) const fn exact_sign_i8(sign: ExactSign) -> i8 {
    match sign {
        ExactSign::Negative => -1,
        ExactSign::Zero => 0,
        ExactSign::Positive => 1,
    }
}

/// Which polygons the face walk currently stands in, kept up to date as it
/// crosses one ring edge at a time: a polygon is occupied when its shell ring
/// is filled and none of its holes is, and every operand counts its occupied
/// polygons. A face then reads each operand in O(1) instead of rescanning
/// every polygon.
pub(super) struct PolygonOccupancy<'source> {
    pub(super) odd: bool,
    /// Admitted source identity remains authoritative even when operand staging
    /// order differs from operand order.
    pub(super) ring_polygons: &'source [u32],
    pub(super) ring_holes: &'source [bool],
    pub(super) polygon_operands: &'source [u32],
    pub(super) ring_count: usize,
    pub(super) shell_filled: Vec<bool>,
    pub(super) filled_holes: Vec<u32>,
    pub(super) occupied: Vec<u32>,
}

impl<'source> PolygonOccupancy<'source> {
    pub(super) fn new(source: &'source ExactSourceInventory, odd: bool) -> Self {
        let polygons = source.polygon_operands.len();
        let mut shell_filled = Vec::with_capacity(polygons);
        shell_filled.resize(polygons, false);
        let filled_holes = vec![0; polygons];
        let occupied = vec![0; source.operand_count()];
        Self {
            odd,
            ring_polygons: &source.ring_polygons,
            ring_holes: &source.ring_holes,
            polygon_operands: &source.polygon_operands,
            ring_count: source.ring_count,
            shell_filled,
            filled_holes,
            occupied,
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "source-staged ring IDs and polygon IDs index the borrowed canonical owner columns"
    )]
    pub(super) fn seed(&mut self, winding: &[i64]) -> Result<()> {
        if winding.len() != self.ring_count {
            return Err(GeometryErrorKind::Invalid(
                "polygon occupancy seed has the wrong ring count".into(),
            )
            .into());
        }
        for ((&polygon, &hole), &winding) in
            self.ring_polygons.iter().zip(self.ring_holes).zip(winding)
        {
            let filled = ring_occupied(winding, self.odd);
            let polygon = polygon as usize;
            if !hole {
                self.shell_filled[polygon] = filled;
            } else if filled {
                self.filled_holes[polygon] = self.filled_holes[polygon].wrapping_add(1);
            }
        }
        for (polygon, &filled_shell) in self.shell_filled.iter().enumerate() {
            if filled_shell && self.filled_holes[polygon] == 0 {
                let operand = self.polygon_operands[polygon] as usize;
                self.occupied[operand] = self.occupied[operand].wrapping_add(1);
            }
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ring and polygon IDs were admitted by source staging"
    )]
    pub(super) fn cross(&mut self, winding: &mut [i64], ring: u32, direction: i64) {
        let ring = ring as usize;
        let before = ring_occupied(winding[ring], self.odd);
        winding[ring] = winding[ring].wrapping_add(direction);
        let after = ring_occupied(winding[ring], self.odd);
        if before == after {
            return;
        }
        let Some((&polygon, &hole)) = self.ring_polygons.get(ring).zip(self.ring_holes.get(ring))
        else {
            return;
        };
        let polygon = polygon as usize;
        let was = self.shell_filled[polygon] && self.filled_holes[polygon] == 0;
        if !hole {
            self.shell_filled[polygon] = after;
        } else if after {
            self.filled_holes[polygon] = self.filled_holes[polygon].wrapping_add(1);
        } else {
            self.filled_holes[polygon] = self.filled_holes[polygon].wrapping_sub(1);
        }
        let now = self.shell_filled[polygon] && self.filled_holes[polygon] == 0;
        if was != now {
            let count = &mut self.occupied[self.polygon_operands[polygon] as usize];
            if now {
                *count = count.wrapping_add(1);
            } else {
                *count = count.wrapping_sub(1);
            }
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "operand indexes are admitted from ExactSourceInventory::polygon_rings"
    )]
    pub(super) fn operand_occupied(&self, operand: usize) -> bool {
        self.occupied[operand] != 0
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "polygon IDs are admitted by the source inventory used to construct occupancy"
    )]
    pub(super) fn polygon_holes_occupied(&self, polygon: usize) -> bool {
        self.filled_holes[polygon] != 0
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "ring IDs in each stored polygon range were admitted with the source ring inventory"
)]
pub(super) fn rings_occupied(winding: &[i64], mut rings: Range<usize>, odd: bool) -> bool {
    rings
        .next()
        .is_some_and(|ring| ring_occupied(winding[ring], odd))
        && rings.all(|ring| !ring_occupied(winding[ring], odd))
}
