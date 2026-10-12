#[cfg(test)]
use std::cell::Cell;

use super::contact::{certified_degree_vector, strict_sign};
use super::h3_boundary::{MIN_H3_BOUNDARY_ARCS, exact_h3_bbox, h3_ordered_arcs};
use super::h3_projection::project_h3_seed;
use super::{
    Ball, Bound, CellVertexKey, CertSign, DegreeWindowResult, H3ArcSet, H3CellPlan, H3Fan,
    H3FanPlan, H3FanPointClass, H3FanTriangle, H3FanVertex, MAX_H3_BOUNDARY_ARCS,
    PhysicalEndpointKey, Resolution, Vec3Ball, Vec3Bound,
};

#[cfg(test)]
thread_local! {
    static H3_FAN_PLAN_CONSTRUCTIONS: Cell<usize> = const { Cell::new(0) };
}

/// A certified open hemisphere containing this cell boundary.  The later A3
/// descendant-cap plan supplies the traversal radius; the fan only needs its
/// stricter prerequisite that every one of its *own* ordered minor pieces is
/// inside a cap of radius strictly below pi/2.  Positive center-dot bounds at
/// both endpoints prove that for each minor piece by positive normalized
/// linear interpolation.
#[derive(Clone, Copy, Debug)]
struct CertifiedH3BoundaryCap {
    center: Vec3Bound,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FanSide {
    Exterior,
    Open,
    Uncertain,
}
#[derive(Clone, Copy, Debug)]
struct FanInputVertex {
    key: CellVertexKey,
    endpoint: PhysicalEndpointKey,
    ray: Vec3Bound,
    projected: TangentPoint,
}
#[derive(Clone, Copy, Debug)]
struct TangentPoint {
    x: Bound,
    y: Bound,
}
impl H3FanPlan {
    /// Locate a stored degree point in this cell's certified fan.  This is a
    /// point witness primitive, not a replacement for source/cell arc
    /// contact: it proves `Outside` only when every real fan triangle has a
    /// strict exterior constraint.
    pub(in crate::grid) fn point_class(
        &self,
        longitude: Bound,
        latitude: Bound,
    ) -> H3FanPointClass {
        let Self::Certified(plan) = self else {
            return H3FanPointClass::Uncertain;
        };
        let Some(point) = certified_degree_vector(longitude, latitude) else {
            return H3FanPointClass::Uncertain;
        };
        let mut all_exterior = true;
        for index in 0..usize::from(plan.len) {
            let Some(triangle) = plan.triangle_at(index) else {
                return H3FanPointClass::Uncertain;
            };
            match triangle_point_class(&triangle, point) {
                H3FanPointClass::Open => return H3FanPointClass::Open,
                H3FanPointClass::Outside => {},
                H3FanPointClass::Uncertain => all_exterior = false,
            }
        }
        if all_exterior {
            H3FanPointClass::Outside
        } else {
            H3FanPointClass::Uncertain
        }
    }

    /// A cell-center candidate has a stronger, distinct certificate than a
    /// general source witness: strict positivity against every *real* ordered
    /// boundary edge places it in the fan's proved star kernel.  Artificial
    /// center diagonals deliberately do not participate here—the native
    /// center lies on all of them, and treating their zero as open would be
    /// unsound for an arbitrary source witness.
    pub(in crate::grid) fn kernel_point_class(
        &self,
        longitude: Bound,
        latitude: Bound,
    ) -> H3FanPointClass {
        let Self::Certified(plan) = self else {
            return H3FanPointClass::Uncertain;
        };
        let Some(point) = certified_degree_vector(longitude, latitude) else {
            return H3FanPointClass::Uncertain;
        };
        for vertex in plan.vertices() {
            let Some(side) = vertex.next.real_edge.dot(point) else {
                return H3FanPointClass::Uncertain;
            };
            if !side.strictly_positive() {
                return H3FanPointClass::Uncertain;
            }
        }
        H3FanPointClass::Open
    }

    pub(super) fn matches_cell(&self, cell: h3o::CellIndex) -> bool {
        matches!(self, Self::Certified(H3Fan { cell: plan_cell, .. }) if *plan_cell == cell)
    }

    fn side_at_vertex(&self, key: CellVertexKey, tangent: Vec3Ball) -> FanSide {
        let Self::Certified(plan) = self else {
            return FanSide::Uncertain;
        };
        let Some(vertex) = plan.vertices().find(|vertex| vertex.key == key) else {
            return FanSide::Uncertain;
        };
        let previous = triangle_side(&vertex.previous, tangent);
        let next = triangle_side(&vertex.next, tangent);
        if previous == FanSide::Open || next == FanSide::Open {
            FanSide::Open
        } else if previous == FanSide::Exterior && next == FanSide::Exterior {
            FanSide::Exterior
        } else {
            FanSide::Uncertain
        }
    }

    pub(super) fn side_at_physical_endpoint(
        &self,
        endpoint: PhysicalEndpointKey,
        tangent: Vec3Ball,
    ) -> FanSide {
        let Self::Certified(plan) = self else {
            return FanSide::Uncertain;
        };
        let Some(vertex) = plan.vertices().find(|vertex| vertex.endpoint == endpoint) else {
            return FanSide::Uncertain;
        };
        self.side_at_vertex(vertex.key, tangent)
    }
}
fn triangle_point_class(triangle: &[Vec3Bound; 3], point: Vec3Bound) -> H3FanPointClass {
    let [Some(first), Some(second), Some(third)] = triangle.map(|plane| plane.dot(point)) else {
        return H3FanPointClass::Uncertain;
    };
    if first.strictly_positive() && second.strictly_positive() && third.strictly_positive() {
        H3FanPointClass::Open
    } else if first.strictly_negative() || second.strictly_negative() || third.strictly_negative() {
        H3FanPointClass::Outside
    } else {
        H3FanPointClass::Uncertain
    }
}
impl H3Fan {
    fn vertices(&self) -> impl ExactSizeIterator<Item = &H3FanVertex> {
        self.vertices
            .get(..usize::from(self.len))
            .expect("H3 fan prefix fits its fixed array")
            .iter()
            .map(|vertex| vertex.as_ref().expect("H3 fan prefix is initialized"))
    }

    /// Triangle `(center, v_i, v_{i+1})` has its real outer edge and BOTH
    /// artificial diagonals.  The vertex-side relation needs only one
    /// diagonal locally; general fan point location must retain both or a
    /// single wedge would falsely cover the exterior half of the cell.
    fn triangle_at(&self, index: usize) -> Option<[Vec3Bound; 3]> {
        let next_index = next_h3_boundary_index(index, usize::from(self.len))?;
        let current = self.vertices.get(index)?.as_ref()?;
        let next = self.vertices.get(next_index)?.as_ref()?;
        Some([
            current.next.real_edge,
            current.next.diagonal,
            next.previous.diagonal,
        ])
    }
}
fn next_h3_boundary_index(index: usize, count: usize) -> Option<usize> {
    if index >= count {
        return None;
    }
    let next = index.checked_add(1)?;
    Some(if next == count { 0 } else { next })
}
/// Builds the per-cell certificate required before any H3 vertex side can
/// establish a negative.  A failed projection, orientation, or fixed-size
/// segment proof yields `Uncertain`; it is never repaired by a regular-hexagon
/// assumption.
#[cfg(test)]
pub(in crate::grid) fn h3_fan_plan(cell: h3o::CellIndex) -> H3FanPlan {
    h3_cell_plan(cell, cell.resolution()).fan()
}
#[cfg(test)]
pub(in crate::grid) fn reset_h3_fan_plan_construction_count() {
    H3_FAN_PLAN_CONSTRUCTIONS.with(|count| count.set(0));
}
#[cfg(test)]
pub(in crate::grid) fn h3_fan_plan_construction_count() -> usize {
    H3_FAN_PLAN_CONSTRUCTIONS.with(Cell::get)
}
pub(in crate::grid) fn h3_cell_plan(cell: h3o::CellIndex, target: Resolution) -> H3CellPlan {
    let arcs = (target == cell.resolution())
        .then(|| h3_ordered_arcs(cell))
        .flatten();
    let bbox = arcs
        .as_ref()
        .map_or(DegreeWindowResult::Boundary, exact_h3_bbox);
    H3CellPlan { cell, arcs, bbox }
}
pub(super) fn h3_fan_plan_from_arcs(cell: h3o::CellIndex, arcs: &H3ArcSet) -> H3FanPlan {
    #[cfg(test)]
    H3_FAN_PLAN_CONSTRUCTIONS.with(|count| {
        count.set(
            count
                .get()
                .checked_add(1)
                .expect("H3 fan-plan test counter fits usize"),
        );
    });

    let Some(center) = cell
        .target_substrate_center_seed(cell.resolution())
        .and_then(|seed| project_h3_seed(&seed))
    else {
        return H3FanPlan::Uncertain;
    };
    let Some(cap) = certify_h3_boundary_cap(center, arcs) else {
        return H3FanPlan::Uncertain;
    };
    let Some((north, east)) = tangent_basis(cap.center) else {
        return H3FanPlan::Uncertain;
    };
    let count = arcs.len();
    if !(MIN_H3_BOUNDARY_ARCS..=MAX_H3_BOUNDARY_ARCS).contains(&count) {
        return H3FanPlan::Uncertain;
    }

    let mut inputs = [None; MAX_H3_BOUNDARY_ARCS];
    let next_arcs = arcs.iter().skip(1).chain(arcs.iter().take(1));
    for ((arc, next), slot) in arcs.iter().zip(next_arcs).zip(inputs.iter_mut()) {
        if arc.vertex_keys[1] != next.vertex_keys[0] {
            return H3FanPlan::Uncertain;
        }
        if arc.supplier != cell {
            return H3FanPlan::Uncertain;
        }
        let Some(projected) = cap.gnomonic_project(north, east, arc.start) else {
            return H3FanPlan::Uncertain;
        };
        if !tangent_nonzero(projected) {
            return H3FanPlan::Uncertain;
        }
        *slot = Some(FanInputVertex {
            key: arc.vertex_keys[0],
            endpoint: arc.endpoints[0],
            ray: arc.start,
            projected,
        });
    }
    let Some(input_prefix) = inputs.get(..count) else {
        return H3FanPlan::Uncertain;
    };

    let input = |index: usize| inputs.get(index).copied().flatten();

    let origin = TangentPoint {
        x: Bound::exact(0.0).expect("finite origin"),
        y: Bound::exact(0.0).expect("finite origin"),
    };
    let (Some(first), Some(second)) = (input(0), input(1)) else {
        return H3FanPlan::Uncertain;
    };
    let Some(ring_sign) =
        orientation(first.projected, second.projected, origin).and_then(strict_sign)
    else {
        return H3FanPlan::Uncertain;
    };
    let next_inputs = input_prefix
        .iter()
        .skip(1)
        .chain(input_prefix.iter().take(1));
    for (current, next) in input_prefix.iter().zip(next_inputs) {
        let (Some(current), Some(next)) = (*current, *next) else {
            return H3FanPlan::Uncertain;
        };
        if orientation(current.projected, next.projected, origin).and_then(strict_sign)
            != Some(ring_sign)
        {
            return H3FanPlan::Uncertain;
        }
    }
    let last_index = count
        .checked_sub(1)
        .expect("a certified H3 fan has boundary vertices");
    for first in 0..count {
        let Some(first_next) = next_h3_boundary_index(first, count) else {
            return H3FanPlan::Uncertain;
        };
        let second_start = first
            .checked_add(1)
            .expect("H3 fan index is below the boundary count");
        for second in second_start..count {
            if second == first_next || (first == 0 && second == last_index) {
                continue;
            }
            let Some(second_next) = next_h3_boundary_index(second, count) else {
                return H3FanPlan::Uncertain;
            };
            let (Some(first_start), Some(first_end), Some(second_start), Some(second_end)) = (
                input(first),
                input(first_next),
                input(second),
                input(second_next),
            ) else {
                return H3FanPlan::Uncertain;
            };
            if !matches!(
                segments_strictly_disjoint(
                    first_start.projected,
                    first_end.projected,
                    second_start.projected,
                    second_end.projected,
                ),
                Some(true)
            ) {
                return H3FanPlan::Uncertain;
            }
        }
    }
    for diagonal in 0..count {
        for edge in 0..count {
            let Some(edge_next) = next_h3_boundary_index(edge, count) else {
                return H3FanPlan::Uncertain;
            };
            if edge == diagonal || edge_next == diagonal {
                continue;
            }
            let (Some(diagonal_input), Some(edge_start), Some(edge_end)) =
                (input(diagonal), input(edge), input(edge_next))
            else {
                return H3FanPlan::Uncertain;
            };
            if !matches!(
                segments_strictly_disjoint(
                    origin,
                    diagonal_input.projected,
                    edge_start.projected,
                    edge_end.projected,
                ),
                Some(true)
            ) {
                return H3FanPlan::Uncertain;
            }
        }
        let other_start = diagonal
            .checked_add(1)
            .expect("H3 fan index is below the boundary count");
        for other in other_start..count {
            let (Some(diagonal_input), Some(other_input)) = (input(diagonal), input(other)) else {
                return H3FanPlan::Uncertain;
            };
            if !matches!(
                radial_open_interiors_strictly_disjoint(
                    origin,
                    diagonal_input.projected,
                    other_input.projected,
                ),
                Some(true)
            ) {
                return H3FanPlan::Uncertain;
            }
        }
    }

    let mut vertices = [None; MAX_H3_BOUNDARY_ARCS];
    let Some(first_input_slot) = input_prefix.first() else {
        return H3FanPlan::Uncertain;
    };
    let Some(mut previous_input) = input_prefix.last().copied().flatten() else {
        return H3FanPlan::Uncertain;
    };
    let mut current_inputs = input_prefix.iter().peekable();
    for slot in vertices.iter_mut().take(count) {
        let Some(current_slot) = current_inputs.next() else {
            return H3FanPlan::Uncertain;
        };
        let Some(current) = *current_slot else {
            return H3FanPlan::Uncertain;
        };
        let next_slot = current_inputs.peek().copied().unwrap_or(first_input_slot);
        let Some(next_input) = *next_slot else {
            return H3FanPlan::Uncertain;
        };
        let Some(previous_real) = previous_input
            .ray
            .cross(current.ray)
            .and_then(|plane| orient_inward(plane, center))
        else {
            return H3FanPlan::Uncertain;
        };
        let Some(previous_diagonal) = current
            .ray
            .cross(center)
            .and_then(|plane| orient_inward(plane, previous_input.ray))
        else {
            return H3FanPlan::Uncertain;
        };
        let Some(next_real) = current
            .ray
            .cross(next_input.ray)
            .and_then(|plane| orient_inward(plane, center))
        else {
            return H3FanPlan::Uncertain;
        };
        let Some(next_diagonal) = center
            .cross(current.ray)
            .and_then(|plane| orient_inward(plane, next_input.ray))
        else {
            return H3FanPlan::Uncertain;
        };
        *slot = Some(H3FanVertex {
            key: current.key,
            endpoint: current.endpoint,
            previous: H3FanTriangle {
                real_edge: previous_real,
                diagonal: previous_diagonal,
            },
            next: H3FanTriangle {
                real_edge: next_real,
                diagonal: next_diagonal,
            },
        });
        previous_input = current;
    }
    H3FanPlan::Certified(H3Fan {
        cell,
        vertices,
        len: u8::try_from(count).expect("bounded H3 fan length"),
    })
}
fn certify_h3_boundary_cap(center: Vec3Bound, arcs: &H3ArcSet) -> Option<CertifiedH3BoundaryCap> {
    if arcs.is_empty() {
        return None;
    }
    for arc in arcs.iter() {
        if !center.dot(arc.start)?.strictly_positive() || !center.dot(arc.end)?.strictly_positive()
        {
            return None;
        }
    }
    Some(CertifiedH3BoundaryCap { center })
}
impl CertifiedH3BoundaryCap {
    fn gnomonic_project(
        self,
        north: Vec3Bound,
        east: Vec3Bound,
        point: Vec3Bound,
    ) -> Option<TangentPoint> {
        // The constructor proves this denominator strictly positive for every
        // ordered boundary endpoint.  Keep the assertion as a checked
        // defense against a future caller feeding a non-cap point.
        gnomonic_project(self.center, north, east, point)
    }
}
fn tangent_basis(center: Vec3Bound) -> Option<(Vec3Bound, Vec3Bound)> {
    let north_axis = Vec3Bound::exact([0.0, 0.0, 1.0])?;
    let north = north_axis.sub(center.scale(center.z)?)?.normalized()?;
    Some((north, north.cross(center)?))
}
fn gnomonic_project(
    center: Vec3Bound,
    north: Vec3Bound,
    east: Vec3Bound,
    point: Vec3Bound,
) -> Option<TangentPoint> {
    let denominator = center.dot(point)?;
    if !denominator.strictly_positive() {
        return None;
    }
    let inverse = Bound::exact(1.0)?.div(denominator)?;
    let offset = point.scale(inverse)?.sub(center)?;
    Some(TangentPoint {
        x: offset.dot(north)?,
        y: offset.dot(east)?,
    })
}
fn tangent_nonzero(point: TangentPoint) -> bool {
    point
        .x
        .mul(point.x)
        .and_then(|x| point.y.mul(point.y).and_then(|y| x.add(y)))
        .is_some_and(Bound::strictly_positive)
}
fn orientation(first: TangentPoint, second: TangentPoint, third: TangentPoint) -> Option<Bound> {
    second
        .x
        .sub(first.x)?
        .mul(third.y.sub(first.y)?)?
        .sub(second.y.sub(first.y)?.mul(third.x.sub(first.x)?)?)
}
fn segments_strictly_disjoint(
    first: TangentPoint,
    second: TangentPoint,
    third: TangentPoint,
    fourth: TangentPoint,
) -> Option<bool> {
    // A strict separator from EITHER segment's supporting line proves closed
    // disjointness.  Do not require all four orientations: a nonincident H3
    // edge can meet a center diagonal's line extension at an opposite vertex
    // while the finite segments remain strictly disjoint.
    let first_separator = matches!(
        (
            orientation(first, second, third).and_then(strict_sign),
            orientation(first, second, fourth).and_then(strict_sign),
        ),
        (Some(left), Some(right)) if left == right
    );
    let second_separator = matches!(
        (
            orientation(third, fourth, first).and_then(strict_sign),
            orientation(third, fourth, second).and_then(strict_sign),
        ),
        (Some(left), Some(right)) if left == right
    );
    (first_separator || second_separator).then_some(true)
}
/// Two fan diagonals share only the tangent-plane origin.  A strict angular
/// separation proves their open interiors disjoint; opposite collinear rays
/// are also disjoint and need a dot proof instead of an orientation sign.
fn radial_open_interiors_strictly_disjoint(
    origin: TangentPoint,
    first: TangentPoint,
    second: TangentPoint,
) -> Option<bool> {
    if orientation(origin, first, second)
        .and_then(strict_sign)
        .is_some()
    {
        return Some(true);
    }
    let first_x = first.x.sub(origin.x)?;
    let first_y = first.y.sub(origin.y)?;
    let second_x = second.x.sub(origin.x)?;
    let second_y = second.y.sub(origin.y)?;
    first_x
        .mul(second_x)?
        .add(first_y.mul(second_y)?)?
        .strictly_negative()
        .then_some(true)
}
fn orient_inward(plane: Vec3Bound, witness: Vec3Bound) -> Option<Vec3Bound> {
    match plane.dot(witness)? {
        value if value.strictly_positive() => Some(plane),
        value if value.strictly_negative() => Some(plane.neg()),
        _ => None,
    }
}
fn triangle_side(triangle: &H3FanTriangle, tangent: Vec3Ball) -> FanSide {
    let (Some(real), Some(diagonal)) = (
        tangent.dot(triangle.real_edge).map(Ball::sign),
        tangent.dot(triangle.diagonal).map(Ball::sign),
    ) else {
        return FanSide::Uncertain;
    };
    if real == CertSign::Positive && diagonal == CertSign::Positive {
        FanSide::Open
    } else if real == CertSign::Negative || diagonal == CertSign::Negative {
        FanSide::Exterior
    } else {
        FanSide::Uncertain
    }
}

#[cfg(test)]
mod tests;
