#[cfg(test)]
use h3o::{CapLatticeProfile, Resolution};

#[cfg(test)]
use super::arcs::certified_arc_latitude_bounds;
use super::arcs::certified_arc_latitude_bounds_from_endpoints;
use super::contact::{certified_degree_vector, degree_bound_to_radians};
#[cfg(test)]
use super::h3_fan::h3_cell_plan;
#[cfg(test)]
use super::h3_fan::h3_fan_plan;
use super::h3_fan::h3_fan_plan_from_arcs;
use super::h3_projection::project_h3_seed;
use super::trig::{down, upward_sum};
use super::vectors::{
    certified_vector_latitude, certified_vector_longitude, directed_arc_longitude_from_endpoints,
};
use super::{
    Bound, CellVertexKey, CertifiedCircularBoundsRad, CertifiedDegreeWindows,
    CertifiedLongitudeRad, DegreeWindowResult, ExactArcKind, H3ArcSet, H3BboxWitnesses,
    H3BoundingWindow, H3CellPlan, H3FanPlan, HALF_PI_HI, LongitudeSpan, MAX_H3_BOUNDARY_ARCS,
    Midpoint as _, OrderedMinorArc, PI_HI, PI_LO, PhysicalEndpointKey, ProjectionSeed, Vec3Bound,
};

/// The native H3 boundary holds at most five original pentagon vertices plus
/// five face-crossing insertions.  Keep that vendor bound structural: arc
/// construction must not make allocation failure a numeric uncertainty.
pub(super) const MIN_H3_BOUNDARY_ARCS: usize = 3;
fn push_fixed_prefix<T: Copy, const N: usize>(
    values: &mut [Option<T>; N],
    len: &mut u8,
    value: T,
) -> Option<()> {
    let index = usize::from(*len);
    let next_len = len.checked_add(1)?;
    *values.get_mut(index)? = Some(value);
    *len = next_len;
    Some(())
}
impl H3BboxWitnesses {
    const fn empty() -> Self {
        Self {
            values: [None; MAX_H3_BOUNDARY_ARCS + 1],
            len: 0,
        }
    }

    fn push(&mut self, value: CertifiedDegreeWindows) -> Option<()> {
        push_fixed_prefix(&mut self.values, &mut self.len, value)
    }

    pub(in crate::grid) fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = CertifiedDegreeWindows> + '_ {
        self.values
            .get(..usize::from(self.len))
            .expect("H3 witness prefix fits its fixed array")
            .iter()
            .map(|value| *value.as_ref().expect("H3 witness prefix is initialized"))
    }
}
impl H3CellPlan {
    pub(in crate::grid) const fn cell(&self) -> h3o::CellIndex {
        self.cell
    }

    pub(in crate::grid) const fn arcs(&self) -> Option<&H3ArcSet> {
        self.arcs.as_ref()
    }

    pub(in crate::grid) const fn bbox(&self) -> DegreeWindowResult {
        self.bbox
    }

    /// Derive the exact fan from the shared raw boundary.  The returned fan
    /// belongs to the caller's operation; it is intentionally not retained in
    /// the cell plan because bbox and cap exclusions do not need it.
    pub(in crate::grid) fn fan(&self) -> H3FanPlan {
        self.arcs.as_ref().map_or(H3FanPlan::Uncertain, |arcs| {
            h3_fan_plan_from_arcs(self.cell, arcs)
        })
    }
}
impl H3ArcSet {
    const fn empty() -> Self {
        Self {
            values: [None; MAX_H3_BOUNDARY_ARCS],
            len: 0,
        }
    }

    pub(super) const fn len(&self) -> usize {
        self.len as usize
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[cfg(test)]
    pub(super) fn get(&self, index: usize) -> Option<&OrderedMinorArc> {
        if index >= self.len() {
            return None;
        }
        self.values.get(index)?.as_ref()
    }

    fn push(&mut self, arc: &OrderedMinorArc) -> Option<()> {
        push_fixed_prefix(&mut self.values, &mut self.len, *arc)
    }

    pub(in crate::grid) fn iter(&self) -> impl ExactSizeIterator<Item = &OrderedMinorArc> {
        self.values
            .get(..self.len())
            .expect("H3 arc prefix fits its fixed array")
            .iter()
            .map(|arc| arc.as_ref().expect("H3 arc set prefix is initialized"))
    }
}
/// Builds every H3 boundary piece in the vendor's supplied order, including
/// Class-III face-crossing insertions.  The final point is joined back to the
/// first; `ProjectedBoundary` guarantees the public/seed counts agree.
pub(super) fn h3_ordered_arcs(cell: h3o::CellIndex) -> Option<H3ArcSet> {
    let boundary = cell.boundary_with_seeds();
    let count = boundary.public.len();
    if !(MIN_H3_BOUNDARY_ARCS..=MAX_H3_BOUNDARY_ARCS).contains(&count)
        || count != boundary.projection_seeds().len()
        || count != boundary.projection_seed_edges().len()
    {
        return None;
    }

    let mut points = [None; MAX_H3_BOUNDARY_ARCS];
    for (ordinal, (seed, source_edge)) in boundary
        .projection_seeds()
        .copied()
        .zip(boundary.projection_seed_edges())
        .enumerate()
    {
        let ordinal = u8::try_from(ordinal).ok()?;
        let key = match seed {
            ProjectionSeed::Point { .. } => CellVertexKey::H3 { cell, ordinal },
            ProjectionSeed::Intersection { .. } => CellVertexKey::H3Insertion {
                cell,
                edge: source_edge,
                ordinal,
            },
        };
        let point = project_h3_seed(&seed)?;
        *points.get_mut(usize::from(ordinal))? = Some((
            key,
            PhysicalEndpointKey::CellVertex(key),
            point,
            certified_vector_longitude(point)?,
        ));
    }
    let point_prefix = points.get(..count)?;
    let next_points = point_prefix
        .iter()
        .skip(1)
        .chain(point_prefix.iter().take(1));
    let mut arcs = H3ArcSet::empty();
    for (start, end) in point_prefix.iter().zip(next_points) {
        let (start_vertex, start_key, start, start_longitude) = (*start)?;
        let (end_vertex, end_key, end, end_longitude) = (*end)?;
        let normal = start.cross(end)?;
        let normal_norm = normal.dot(normal)?.sqrt()?;
        if normal_norm.contains_zero() || start.dot(end)?.lo <= -1.0 {
            return None;
        }
        let kind = if normal.z.strictly_positive() || normal.z.strictly_negative() {
            ExactArcKind::General
        } else {
            ExactArcKind::UnresolvedMeridian
        };
        let longitude = if kind == ExactArcKind::General {
            Some(directed_arc_longitude_from_endpoints(
                start_longitude,
                end_longitude,
            )?)
        } else {
            None
        };
        arcs.push(&OrderedMinorArc::from_keyed_points(
            cell,
            [start_vertex, end_vertex],
            [start_key, end_key],
            start,
            end,
            kind,
            normal,
            longitude,
        ))?;
    }
    Some(arcs)
}
/// Certified point witnesses for a target H3 cell.  The target-substrate
/// centre and every native projected boundary seed are positive-only aids for
/// bbox traversal; all ambiguity is returned as `None` so the caller uses the
/// exact ordered-arc bbox instead.
pub(in crate::grid) fn h3_bbox_positive_witnesses(cell: h3o::CellIndex) -> Option<H3BboxWitnesses> {
    let boundary = cell.boundary_with_seeds();
    let count = boundary.public.len();
    if !(MIN_H3_BOUNDARY_ARCS..=MAX_H3_BOUNDARY_ARCS).contains(&count)
        || count != boundary.projection_seeds().len()
    {
        return None;
    }
    let mut witnesses = H3BboxWitnesses::empty();
    let center = cell.target_substrate_center_seed(cell.resolution())?;
    witnesses.push(certified_h3_point_degree_window(project_h3_seed(&center)?)?)?;
    for seed in boundary.projection_seeds() {
        witnesses.push(certified_h3_point_degree_window(project_h3_seed(seed)?)?)?;
    }
    Some(witnesses)
}
fn certified_h3_point_degree_window(point: Vec3Bound) -> Option<CertifiedDegreeWindows> {
    let longitude = certified_vector_longitude(point)?;
    let result = CertifiedCircularBoundsRad {
        latitude: certified_vector_latitude(point)?,
        longitude: CertifiedLongitudeRad::Span(LongitudeSpan::ordered(longitude, longitude)?),
    }
    .to_degree_windows();
    match result {
        DegreeWindowResult::Windows(windows) => Some(windows),
        DegreeWindowResult::Boundary => None,
    }
}
/// Logical longitude from the ordered H3 boundary.  The directed turn is
/// accumulated only across vendor-adjacent endpoint identities; interpreting
/// the same spans as an unordered circular union loses the 2π sheet.
fn certified_h3_longitude(arcs: &H3ArcSet) -> Option<CertifiedLongitudeRad> {
    let mut ordered = arcs.iter().peekable();
    let first = *ordered.peek()?;
    let first_span = first.longitude?;
    let tau = Bound::new(2.0 * PI_LO, 2.0 * PI_HI)?;
    let mut sheet = 0_i64;
    let mut minimum = first_span.start;
    let mut maximum = first_span.start;
    while let Some(arc) = ordered.next() {
        let next = ordered.peek().copied().unwrap_or(first);
        if arc.endpoints[1] != next.endpoints[0] || arc.kind != ExactArcKind::General {
            return None;
        }
        let span = arc.longitude?;
        let shift = tau.mul(Bound::exact(sheet as f64)?)?;
        let start = span.start.add(shift)?;
        let end = span.end.add(shift)?;
        minimum = Bound::new(
            minimum.lo.min(start.lo).min(end.lo),
            minimum.hi.min(start.hi).min(end.hi),
        )?;
        maximum = Bound::new(
            maximum.lo.max(start.lo).max(end.lo),
            maximum.hi.max(start.hi).max(end.hi),
        )?;
        sheet = sheet.checked_add(span.turn)?;
    }
    match sheet {
        // The sign of the ordered sheet is physical information, not merely
        // a longitude spelling: it identifies the pole contained by the
        // boundary. Dropping it pruned polar cells before arc classification.
        1 => return Some(CertifiedLongitudeRad::FullThroughNorthPole),
        -1 => return Some(CertifiedLongitudeRad::FullThroughSouthPole),
        0 => {},
        _ => return None,
    }
    let width = maximum.sub(minimum)?;
    if width.hi >= tau.lo {
        return None;
    }
    Some(CertifiedLongitudeRad::Span(LongitudeSpan::ordered(
        minimum, maximum,
    )?))
}
/// Exact H3 logical bbox from the one ordered native boundary plan.  A
/// failure is `Boundary`, never a narrow negative certificate.
pub(super) fn exact_h3_bbox(arcs: &H3ArcSet) -> DegreeWindowResult {
    let Some(bounds) = (|| {
        let mut latitude: Option<Bound> = None;
        let mut vertex_latitudes = [None; MAX_H3_BOUNDARY_ARCS];
        for (latitude, arc) in vertex_latitudes.iter_mut().zip(arcs.iter()) {
            *latitude = Some(certified_vector_latitude(arc.start)?);
        }
        let vertex_latitudes = vertex_latitudes.get(..arcs.len())?;
        let first_latitude = vertex_latitudes.first()?;
        let next_latitudes = vertex_latitudes
            .iter()
            .skip(1)
            .chain(std::iter::once(first_latitude));
        for ((arc, start_latitude), end_latitude) in
            arcs.iter().zip(vertex_latitudes).zip(next_latitudes)
        {
            let arc_latitude = certified_arc_latitude_bounds_from_endpoints(
                arc,
                start_latitude.as_ref().copied()?,
                end_latitude.as_ref().copied()?,
            )?;
            latitude = Some(match latitude {
                Some(current) => Bound::new(
                    current.lo.min(arc_latitude.lo),
                    current.hi.max(arc_latitude.hi),
                )?,
                None => arc_latitude,
            });
        }
        let longitude = certified_h3_longitude(arcs)?;
        let latitude = match longitude {
            CertifiedLongitudeRad::FullThroughNorthPole => Bound::new(latitude?.lo, HALF_PI_HI)?,
            CertifiedLongitudeRad::FullThroughSouthPole => Bound::new(-HALF_PI_HI, latitude?.hi)?,
            CertifiedLongitudeRad::Full | CertifiedLongitudeRad::Span(_) => latitude?,
        };
        Some(CertifiedCircularBoundsRad {
            latitude,
            longitude,
        })
    })() else {
        return DegreeWindowResult::Boundary;
    };
    bounds.to_degree_windows()
}
/// Exact H3 logical bbox without constructing a centre fan.  Bbox traversal
/// needs only the ordered native boundary; fan ambiguity belongs solely to
/// the overlap relation and may not widen this independent path.
#[cfg(test)]
pub(in crate::grid) fn exact_h3_bbox_for_cell(cell: h3o::CellIndex) -> DegreeWindowResult {
    h3_ordered_arcs(cell).map_or(DegreeWindowResult::Boundary, |arcs| exact_h3_bbox(&arcs))
}
/// Prove that every point in every supplied rectangle is inside `cell`.
///
/// H3's public boundary is a rounded presentation. The ordered projection
/// seeds above retain the true edge arcs, so each edge's oriented great-circle
/// halfspace is tested over the complete parameter rectangle. A positive
/// result is the only affirmative answer; failure means that the caller must
/// try a coarser ancestor or report the family no-single-cell result.
pub(crate) fn h3_cell_contains_windows(cell: h3o::CellIndex, windows: &[H3BoundingWindow]) -> bool {
    let Some(arcs) = h3_ordered_arcs(cell) else {
        return false;
    };
    let center = h3o::LatLng::from(cell);
    let Some(center_longitude) = Bound::exact(center.lng()) else {
        return false;
    };
    let Some(center_latitude) = Bound::exact(center.lat()) else {
        return false;
    };
    let Some(center) = certified_degree_vector(center_longitude, center_latitude) else {
        return false;
    };

    let mut inward = [None; MAX_H3_BOUNDARY_ARCS];
    for (normal, arc) in inward.iter_mut().zip(arcs.iter()) {
        let Some(side) = arc.normal.dot(center) else {
            return false;
        };
        *normal = if side.strictly_positive() {
            Some(arc.normal)
        } else if side.strictly_negative() {
            Some(arc.normal.neg())
        } else {
            return false;
        };
    }

    windows.iter().all(|window| {
        let Some(lon0) = degrees_to_radians_bound(window.west) else {
            return false;
        };
        let Some(lon1) = degrees_to_radians_bound(window.east) else {
            return false;
        };
        let Some(lat0) = degrees_to_radians_bound(window.south) else {
            return false;
        };
        let Some(lat1) = degrees_to_radians_bound(window.north) else {
            return false;
        };
        if window.west > window.east || window.south > window.north {
            return false;
        }
        inward.iter().take(arcs.len()).all(|normal| {
            normal.as_ref().is_some_and(|normal| {
                h3_plane_min_with_uncertainty(*normal, lon0, lon1, lat0, lat1)
                    .is_some_and(|value| value >= 0.0)
            })
        })
    })
}
pub(super) fn degrees_to_radians_bound(value: f64) -> Option<Bound> {
    degree_bound_to_radians(Bound::exact(value)?)
}
/// Lower-bound a plane over a rectangle using the analytic scalar minimum and
/// the raw projection interval's coefficient uncertainty. The scalar minimum
/// accounts for all four edges and both interior stationary directions; the
/// final ulp term covers the trigonometric evaluation itself.
fn h3_plane_min_with_uncertainty(
    normal: Vec3Bound,
    lon0: Bound,
    lon1: Bound,
    lat0: Bound,
    lat1: Bound,
) -> Option<f64> {
    let nx = normal.x.lo.certified_midpoint(normal.x.hi)?;
    let ny = normal.y.lo.certified_midpoint(normal.y.hi)?;
    let nz = normal.z.lo.certified_midpoint(normal.z.hi)?;
    let nominal = scalar_plane_min(nx, ny, nz, lon0, lon1, lat0, lat1)?;
    let uncertainty = upward_sum([
        bound_radius(normal.x)?,
        bound_radius(normal.y)?,
        bound_radius(normal.z)?,
    ])?;
    let scale = nx.abs().max(ny.abs()).max(nz.abs()).max(1.0);
    down(nominal - uncertainty - 256.0 * f64::EPSILON * scale)
}
fn bound_radius(value: Bound) -> Option<f64> {
    let midpoint = value.lo.certified_midpoint(value.hi)?;
    (midpoint - value.lo)
        .max(value.hi - midpoint)
        .is_finite()
        .then_some((midpoint - value.lo).max(value.hi - midpoint))
}
/// Exact stationary-point inventory for `n · (cos(lat)cos(lon),
/// cos(lat)sin(lon), sin(lat))` over a closed rectangle. This is a proof
/// helper, not a finite sample test: every candidate comes from a boundary or
/// a derivative zero of the analytic function.
fn scalar_plane_min(
    nx: f64,
    ny: f64,
    nz: f64,
    lon0: Bound,
    lon1: Bound,
    lat0: Bound,
    lat1: Bound,
) -> Option<f64> {
    let lon0 = lon0.lo;
    let lon1 = lon1.hi;
    let lat0 = lat0.lo;
    let lat1 = lat1.hi;
    let mut minimum = f64::INFINITY;
    let mut visit = |lon: f64, lat: f64| {
        let value = plane_value(nx, ny, nz, lon, lat);
        if value.is_finite() {
            minimum = minimum.min(value);
        }
    };

    for &lon in &[lon0, lon1] {
        for &lat in &[lat0, lat1] {
            visit(lon, lat);
        }
        let s = nx * lon.cos() + ny * lon.sin();
        let alpha = nz.atan2(s);
        for k in -1..=1 {
            let lat = alpha + std::f64::consts::PI * f64::from(k);
            if lat >= lat0 && lat <= lat1 {
                visit(lon, lat);
            }
        }
    }
    for &lat in &[lat0, lat1] {
        let phi = ny.atan2(nx);
        for k in -2..=2 {
            let lon = phi + std::f64::consts::PI + std::f64::consts::TAU * f64::from(k);
            if lon >= lon0 && lon <= lon1 {
                visit(lon, lat);
            }
        }
    }
    let phi = ny.atan2(nx);
    for base in [phi, phi + std::f64::consts::PI] {
        for k in -2..=2 {
            let lon = base + std::f64::consts::TAU * f64::from(k);
            if lon < lon0 || lon > lon1 {
                continue;
            }
            let s = nx * lon.cos() + ny * lon.sin();
            let alpha = nz.atan2(s);
            for k in -1..=1 {
                let lat = alpha + std::f64::consts::PI * f64::from(k);
                if lat >= lat0 && lat <= lat1 {
                    visit(lon, lat);
                }
            }
        }
    }
    minimum.is_finite().then_some(minimum)
}
fn plane_value(nx: f64, ny: f64, nz: f64, lon: f64, lat: f64) -> f64 {
    let (sin_lon, cos_lon) = lon.sin_cos();
    let (sin_lat, cos_lat) = lat.sin_cos();
    cos_lat * (nx * cos_lon + ny * sin_lon) + nz * sin_lat
}

#[cfg(test)]
mod tests;
