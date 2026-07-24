use super::*;
pub(crate) fn line_length_3d<C: Coordinates + ?Sized>(points: &C) -> Result<f64> {
    // SoA-backed sequences run the wide column kernel (column presence IS
    // the per-vertex Z truth, so the missing-Z check happens once).
    if let Some((xs, ys)) = points.xy_columns() {
        if xs.len() < 2 {
            return Ok(0.0);
        }
        let zs = points.z_column().ok_or(GeometryErrorKind::MissingZ)?;
        return Ok(line_length_3d_columns(xs, ys, zs));
    }
    points.segment_pairs().try_fold(0.0, |total, [start, end]| {
        let z0 = start.z().ok_or(GeometryErrorKind::MissingZ)?;
        let z1 = end.z().ok_or(GeometryErrorKind::MissingZ)?;
        let dx = end.x - start.x;
        let dy = end.y - start.y;
        let dz = z1 - z0;
        // Guarded sqrt: `hypot` is a libm call per segment; fall back to the
        // chained exact form only on overflow/underflow.
        let squared = dx * dx + dy * dy + dz * dz;
        let length =
            if squared.is_finite() && (squared != 0.0 || (dx == 0.0 && dy == 0.0 && dz == 0.0)) {
                squared.sqrt()
            } else {
                dx.hypot(dy).hypot(dz)
            };
        Ok(total + length)
    })
}

pub(crate) fn bounds_distance_squared(left: Bounds, right: Bounds) -> f64 {
    let dx = if left.maxx() < right.minx() {
        right.minx() - left.maxx()
    } else if right.maxx() < left.minx() {
        left.minx() - right.maxx()
    } else {
        0.0
    };
    let dy = if left.maxy() < right.miny() {
        right.miny() - left.maxy()
    } else if right.maxy() < left.miny() {
        left.miny() - right.maxy()
    } else {
        0.0
    };
    // Plain ops: scalar `mul_add` is a libm call below x86-64-v3.
    dx * dx + dy * dy
}
