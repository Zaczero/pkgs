#[cfg(test)]
use h3o::{CapLatticeProfile, Resolution};

use super::trig::sin_cos;
use super::{Bound, Hex2Int, ProjectionSeed, Vec3Bound};

/// Replays a vendor H3 seed with certified primitive arithmetic.  It follows
/// the native projection algebra directly and never converts the public
/// `LatLng` back into a predicate carrier.
pub(super) fn project_h3_seed(seed: &ProjectionSeed) -> Option<Vec3Bound> {
    project_h3_seed_unnormalized(seed)?.normalized()
}
/// Certified native H3 projection before the final unit-vector normalization.
/// A3's seam proof compares this affine carrier in its paired face frames;
/// normalizing before subtraction would erase precisely the chart discrepancy
/// the cap must charge.
pub(super) fn project_h3_seed_unnormalized(seed: &ProjectionSeed) -> Option<Vec3Bound> {
    let frame = seed.projection_frame();
    let (resolution, substrate) = match seed {
        ProjectionSeed::Point {
            resolution,
            substrate,
            ..
        }
        | ProjectionSeed::Intersection {
            resolution,
            substrate,
            ..
        } => (resolution, substrate),
    };
    let [x, y] = seed_native_point(seed, frame.sqrt3_2)?;
    let center = Vec3Bound::exact(frame.center)?;
    let north_axis = Vec3Bound::exact([0.0, 0.0, 1.0])?;
    let north = north_axis.sub(center.scale(center.z)?)?.normalized()?;
    let east = north.cross(center)?;

    let angle = if !substrate && resolution % 2 == 1 {
        Bound::exact(frame.axis_az_rads_cii)?.sub(Bound::exact(frame.ap7_rot_rads)?)?
    } else {
        Bound::exact(frame.axis_az_rads_cii)?
    };
    let (sin_axis, cos_axis) = sin_cos(angle)?;
    let sin_axis = sin_axis.bound()?;
    let cos_axis = cos_axis.bound()?;
    let north_coeff = cos_axis.mul(x)?.add(sin_axis.mul(y)?)?;
    let east_coeff = sin_axis.mul(x)?.sub(cos_axis.mul(y)?)?;
    let basis = north.scale(north_coeff)?.add(east.scale(east_coeff)?)?;
    let mut scale = Bound::exact(frame.res0_u_gnomonic)?.mul(Bound::exact(frame.inv_sqrt7)?)?;
    if *substrate {
        scale = scale.mul(Bound::exact(frame.one_third)?)?;
    }
    center.add(basis.scale(scale)?)
}
fn seed_native_point(seed: &ProjectionSeed, sqrt3_2: f64) -> Option<[Bound; 2]> {
    let (x2, y_sqrt3) = match seed {
        ProjectionSeed::Point { point, .. } => (
            Bound::from_i128(i128::from(point.x2))?,
            Bound::from_i128(i128::from(point.y_sqrt3))?,
        ),
        ProjectionSeed::Intersection {
            segment, face_edge, ..
        } => intersection_native_point(*segment, *face_edge)?,
    };
    Some([
        x2.mul(Bound::exact(0.5)?)?,
        y_sqrt3.mul(Bound::exact(sqrt3_2)?)?,
    ])
}
fn intersection_native_point(
    segment: [Hex2Int; 2],
    face_edge: [Hex2Int; 2],
) -> Option<(Bound, Bound)> {
    let sx = i128::from(segment[0].x2);
    let sy = i128::from(segment[0].y_sqrt3);
    let dx = i128::from(segment[1].x2).checked_sub(sx)?;
    let dy = i128::from(segment[1].y_sqrt3).checked_sub(sy)?;
    let ex = i128::from(face_edge[0].x2);
    let ey = i128::from(face_edge[0].y_sqrt3);
    let edx = i128::from(face_edge[1].x2).checked_sub(ex)?;
    let edy = i128::from(face_edge[1].y_sqrt3).checked_sub(ey)?;
    let (numerator, denominator) = rational_intersection_parameter(
        edx,
        edy,
        dx,
        dy,
        ex.checked_sub(sx)?,
        ey.checked_sub(sy)?,
    )?;
    let t = Bound::from_i128(numerator)?.div(Bound::from_i128(denominator)?)?;
    let x2 = Bound::from_i128(sx)?.add(Bound::from_i128(dx)?.mul(t)?)?;
    let y_sqrt3 = Bound::from_i128(sy)?.add(Bound::from_i128(dy)?.mul(t)?)?;
    Some((x2, y_sqrt3))
}
/// Exact raw-seed segment parameter with a positive denominator.  An
/// arithmetic failure is an undecidable carrier, not wrapped lattice data.
fn rational_intersection_parameter(
    edge_dx: i128,
    edge_dy: i128,
    segment_dx: i128,
    segment_dy: i128,
    start_dx: i128,
    start_dy: i128,
) -> Option<(i128, i128)> {
    let mut denominator = cross2(edge_dx, edge_dy, segment_dx, segment_dy)?;
    if denominator == 0 {
        return None;
    }
    let mut numerator = cross2(edge_dx, edge_dy, start_dx, start_dy)?;
    if denominator < 0 {
        numerator = numerator.checked_neg()?;
        denominator = denominator.checked_neg()?;
    }
    let divisor = gcd_i128(numerator, denominator);
    Some((
        numerator.checked_div(divisor)?,
        denominator.checked_div(divisor)?,
    ))
}
fn gcd_i128(numerator: i128, denominator: i128) -> i128 {
    debug_assert!(denominator > 0);
    let mut left = numerator.unsigned_abs();
    let mut right = denominator as u128;
    while right != 0 {
        let remainder = left.checked_rem(right).expect("gcd divisor is nonzero");
        (left, right) = (right, remainder);
    }
    // The denominator is positive i128, therefore its divisor is representable
    // as i128 even when the numerator is i128::MIN.
    left as i128
}
fn cross2(ax: i128, ay: i128, bx: i128, by: i128) -> Option<i128> {
    ax.checked_mul(by)?.checked_sub(ay.checked_mul(bx)?)
}

#[cfg(test)]
mod tests;
