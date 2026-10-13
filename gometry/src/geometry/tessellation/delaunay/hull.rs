use super::*;

pub(crate) fn delaunay_triangulation(points: &[Point]) -> Result<Triangulation> {
    let mut sites = Vec::with_capacity(points.len());
    for (id, &point) in points.iter().enumerate() {
        sites.push(Site::raw(id, point, id));
    }
    certified_delaunay(&sites)?.into_topology()
}

/// The exact convex hull as a counter-clockwise cycle rotated to its smallest
/// site index. With `corners` only strict corners survive, which is the seed
/// the engine fans from; without it collinear boundary sites stay on the
/// cycle, and a wholly collinear input yields its collinear order instead.
#[expect(
    clippy::indexing_slicing,
    reason = "site IDs in order come from the provider, and lower/upper last-two reads follow length checks"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "lower and upper last-two reads follow len >= 2, so subtracting one or two cannot underflow"
)]
pub(super) fn hull_cycle<C: exact_delaunay::MeshCoordinates>(
    coordinates: &C,
    site_count: usize,
    corners: bool,
) -> Result<Vec<usize>> {
    let mut order = Vec::with_capacity(site_count);
    order.extend(0..site_count);
    order
        .sort_unstable_by(|&a, &b| coordinates.compare(coordinates.query(a), coordinates.query(b)));
    if !corners {
        let Some(&last) = order.last() else {
            return Ok(Vec::new());
        };
        let mut all_collinear = order.len() < 3;
        if !all_collinear {
            let first = *order.first().expect("nonempty hull order");
            all_collinear = true;
            for &site in &order {
                if coordinates.orient([
                    coordinates.query(first),
                    coordinates.query(last),
                    coordinates.query(site),
                ])? != ExactSign::Zero
                {
                    all_collinear = false;
                    break;
                }
            }
        }
        if all_collinear {
            return collinear_order(coordinates, order);
        }
    }
    let pops = |sign: ExactSign| {
        if corners {
            sign != ExactSign::Positive
        } else {
            sign == ExactSign::Negative
        }
    };
    let mut lower = Vec::with_capacity(order.len());
    for &site in &order {
        while lower.len() >= 2 {
            let sign = coordinates.orient([
                coordinates.query(lower[lower.len() - 2]),
                coordinates.query(lower[lower.len() - 1]),
                coordinates.query(site),
            ])?;
            if !pops(sign) {
                break;
            }
            lower.pop();
        }
        lower.push(site);
    }
    let mut upper = Vec::with_capacity(order.len());
    for &site in order.iter().rev() {
        while upper.len() >= 2 {
            let sign = coordinates.orient([
                coordinates.query(upper[upper.len() - 2]),
                coordinates.query(upper[upper.len() - 1]),
                coordinates.query(site),
            ])?;
            if !pops(sign) {
                break;
            }
            upper.pop();
        }
        upper.push(site);
    }
    lower.pop();
    upper.pop();
    lower.reserve(upper.len());
    lower.extend(upper);
    if let Some(start) = lower
        .iter()
        .enumerate()
        .min_by_key(|(_, value)| **value)
        .map(|(index, _)| index)
    {
        lower.rotate_left(start);
    }
    Ok(lower)
}

#[cfg(test)]
pub(in crate::geometry::tessellation) fn exact_corner_hull_cycle(points: &[Point]) -> Vec<usize> {
    let xy = |site: usize| points[site].xy();
    exact_corner_hull_cycle_with(&xy, points.len()).expect("stored XY orientation is infallible")
}

#[cfg(test)]
pub(in crate::geometry::tessellation) fn exact_hull_cycle(points: &[Point]) -> Vec<usize> {
    let xy = |site: usize| points[site].xy();
    exact_hull_cycle_with(&xy, points.len()).expect("stored XY hull arithmetic is infallible")
}

pub(in crate::geometry::tessellation) fn exact_corner_hull_cycle_with<
    C: exact_delaunay::MeshCoordinates,
>(
    coordinates: &C,
    site_count: usize,
) -> Result<Vec<usize>> {
    hull_cycle(coordinates, site_count, true)
}

pub(in crate::geometry::tessellation) fn exact_hull_cycle_with<
    C: exact_delaunay::MeshCoordinates,
>(
    coordinates: &C,
    site_count: usize,
) -> Result<Vec<usize>> {
    hull_cycle(coordinates, site_count, false)
}

/// Collinear sites in order along their longer extent.
pub(super) fn collinear_order<C: exact_delaunay::MeshCoordinates>(
    coordinates: &C,
    mut order: Vec<usize>,
) -> Result<Vec<usize>> {
    let Some(&first) = order.first() else {
        return Ok(order);
    };
    let (mut min_x, mut max_x, mut min_y, mut max_y) = (first, first, first, first);
    for &site in order.iter().skip(1) {
        let query = coordinates.query(site);
        if coordinates
            .compare_x(query, coordinates.query(min_x))
            .is_lt()
        {
            min_x = site;
        }
        if coordinates
            .compare_x(query, coordinates.query(max_x))
            .is_gt()
        {
            max_x = site;
        }
        if coordinates
            .compare_y(query, coordinates.query(min_y))
            .is_lt()
        {
            min_y = site;
        }
        if coordinates
            .compare_y(query, coordinates.query(max_y))
            .is_gt()
        {
            max_y = site;
        }
    }
    let x_is_longer = !coordinates
        .compare_axis_spans([coordinates.query(min_x), coordinates.query(max_x)], [
            coordinates.query(min_y),
            coordinates.query(max_y),
        ])?
        .is_lt();
    if x_is_longer {
        order.sort_unstable_by(|&left, &right| {
            coordinates
                .compare_x(coordinates.query(left), coordinates.query(right))
                .then_with(|| {
                    coordinates.compare_y(coordinates.query(left), coordinates.query(right))
                })
        });
    } else {
        order.sort_unstable_by(|&left, &right| {
            coordinates
                .compare_y(coordinates.query(left), coordinates.query(right))
                .then_with(|| {
                    coordinates.compare_x(coordinates.query(left), coordinates.query(right))
                })
        });
    }
    Ok(order)
}

/// Twin slots for a flat triangle list by directed-edge lookup.
#[expect(
    clippy::indexing_slicing,
    reason = "triangle and halfedge columns have one entry per stable flat slot, and reverse slots are values inserted from this same range"
)]
pub(super) fn delaunay_halfedges(triangles: &[usize]) -> Result<Vec<usize>> {
    let mut halfedges = vec![EMPTY; triangles.len()];
    let mut directed = crate::collections::try_hash_map_with_capacity(triangles.len())?;
    for index in 0..triangles.len() {
        let start = triangles[index];
        let end = triangles[next_halfedge(index)];
        if let Some(reverse) = directed.remove(&(end, start)) {
            halfedges[index] = reverse;
            halfedges[reverse] = index;
        } else {
            directed.insert((start, end), index);
        }
    }
    Ok(halfedges)
}

/// Planar triangulation Euler identity: `T = 2n - 2 - h` for `n` vertices
/// and `h` hull vertices of a connected triangulation of a point set.
#[cfg(test)]
pub(super) fn delaunay_euler_complete(n: usize, triangulation: &Triangulation) -> bool {
    if n < 3 {
        return triangulation.triangles.is_empty();
    }
    let t = triangulation.triangles.len() / 3;
    if t == 0 {
        return false;
    }
    let h = triangulation
        .halfedges
        .iter()
        .filter(|&&edge| edge == EMPTY)
        .count();
    h >= 3 && t == n.saturating_mul(2).saturating_sub(2).saturating_sub(h)
}
