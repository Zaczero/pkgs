//! Deepest single S2 cell that **provably** contains a geometry's lon/lat
//! **bounding box**.
//!
//! Contract (sibling-consistent with geohash/tile/h3):
//! 1. **Point** (exactly one vertex, no segments): exact L30 leaf via
//!    [`CellId::from_point`].
//! 2. **Multi-point aggregates** (MultiPoint / all-point GeometryCollection)
//!    and **regions** (line / polygon / mixed): the R18 sound-conservative
//!    lon/lat **bbox** path — deepest cell that provably contains the
//!    envelope. Leaf-LCA of multipoint vertices is NOT used: it contains the
//!    point set but not the bbox region (and over-rejects faces that closed-
//!    contain the envelope).
//! 3. **Bbox descent**:
//!    - Root multi-face gate: closed halfspace with relative
//!      [`closed_eps`] (scaled to edge-normal magnitude) so exact shared face
//!      edges dual-match and raise, while cube-vertex micro-boxes never
//!      false-accept a non-containing face. Prefer a margin root when one
//!      exists under the unique closed root.
//!    - Descent is **margin-only**: every inward halfspace min over the bbox
//!      must be `≥ +margin_eps` (absolute floor + relative to edge-normal
//!      magnitude). Near a boundary or at tiny scale, stop at the coarser
//!      containing cell — never over-descend on near-zero false positives.
//!    - Multi-child margin ties use canonical [`CellId::from_point`] ownership
//!      of the bbox center.
//!    - Zero-width/height cube-edge segments may reconcile to the Hilbert LCA
//!      of samples when the halfspace sibling is wrong for dual assignment.
//!
//! Coverer fail-open `Boundary` is intentionally unused here (correct for
//! outer covers, not multi-face proof). No amplification beyond the bbox.
#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use crate::geometry::{Bounds, Shape};
use crate::grid::s2::cell::Cell;
use crate::grid::s2::cellid::CellId;
use crate::grid::s2::projection::{NUM_FACES, Point3, lonlat_to_point};

/// Deepest S2 cell whose region contains the shape (bbox contract).
///
/// `None` when the shape has no coordinates, or when no single cell contains
/// the lon/lat bounding box (multi-face span). Near cell boundaries the result
/// may be one level coarser than the theoretical deepest (always containing).
///
/// Antimeridian-crossing shapes (west > east bounds, or geometry that crosses
/// ±180) derive candidates from **normalized spherical samples** (vertices +
/// short-arc edge midpoints) rather than the planar lon/lat envelope, which
/// would take the long way around and reject real containing cells.
pub(crate) fn bounding_cell(shape: &Shape) -> Option<CellId> {
    let mut n_points = 0_usize;
    let mut only: Option<Point3> = None;
    let mut samples: Vec<(f64, f64)> = Vec::new();
    shape.for_each_point(|point| {
        n_points += 1;
        samples.push((point.x, point.y));
        if n_points == 1 {
            only = Some(point_from_lonlat(point.x, point.y));
        }
    });
    if n_points == 0 {
        return None;
    }

    // Single point (no segments): exact L30 leaf. Multi-point aggregates share
    // the region bbox path so the cell contains the envelope, not just vertices.
    if shape.segment_count() == 0 && n_points == 1 {
        return Some(CellId::from_point(only.expect("n_points == 1")));
    }

    let bounds = shape.bounds()?;
    // Seam / wrap: use spherical sample LCA (vertices already collected).
    if bounds.minx() > bounds.maxx() || shape.crosses_antimeridian() {
        return bounding_cell_from_samples(&samples);
    }
    bounding_cell_bbox(bounds)
}

/// Deepest cell containing every lon/lat sample (Hilbert LCA of L30 leaves,
/// raised until closed halfspaces contain all samples). Used for
/// antimeridian-normalized spherical extents.
fn bounding_cell_from_samples(samples: &[(f64, f64)]) -> Option<CellId> {
    if samples.is_empty() {
        return None;
    }
    if samples.len() == 1 {
        return Some(CellId::from_lonlat(samples[0].0, samples[0].1));
    }
    let mut leaves: Vec<CellId> = samples
        .iter()
        .map(|&(lon, lat)| CellId::from_lonlat(lon, lat))
        .collect();
    leaves.sort_unstable();
    leaves.dedup();
    // Hilbert LCA: walk all leaves up until they share one cell.
    let mut level = leaves[0].level();
    let candidate = loop {
        let at = leaves
            .iter()
            .map(|leaf| leaf.parent(level).unwrap_or(*leaf))
            .collect::<Vec<_>>();
        if at.iter().all(|c| *c == at[0]) {
            break at[0];
        }
        if level == 0 {
            return None;
        }
        level -= 1;
    };
    let mut candidate = candidate;
    // Raise until every sample is closed-contained (halfspaces).
    loop {
        let bbox_samples_ok = samples.iter().all(|&(lon, lat)| {
            let p = point_from_lonlat(lon, lat);
            Cell::from_id(candidate).contains_point(p)
        });
        if bbox_samples_ok {
            return Some(candidate);
        }
        if candidate.level() == 0 {
            return None;
        }
        candidate = candidate.parent(candidate.level() - 1)?;
    }
}

/// Deepest cell that **provably** contains the lon/lat rectangle `bounds`.
pub(crate) fn bounding_cell_bbox(bounds: Bounds) -> Option<CellId> {
    let minx = bounds.minx();
    let miny = bounds.miny();
    let maxx = bounds.maxx();
    let maxy = bounds.maxy();

    // Plain == so -0.0 and +0.0 are treated as the same coordinate (to_bits
    // would miss the point-degenerate path for signed zero). Bounds are
    // already finite, so NaN is not a concern.
    #[expect(
        clippy::float_cmp,
        reason = "finite bbox edges; signed zero must match"
    )]
    if minx == maxx && miny == maxy {
        return Some(CellId::from_lonlat(minx, miny));
    }

    let bbox = LonLatBBox {
        lon0: minx.to_radians(),
        lon1: maxx.to_radians(),
        lat0: miny.to_radians(),
        lat1: maxy.to_radians(),
    };

    // Face roots that closed-contain the whole bbox (slack for exact face
    // edges). Exactly one closed root required: a true shared face edge
    // closed-belongs to two roots → multi-face raise. Prefer the unique
    // margin root when it is that same closed root.
    let mut closed_root: Option<CellId> = None;
    let mut margin_root: Option<CellId> = None;
    let mut n_closed = 0_usize;
    for face_idx in 0..NUM_FACES {
        let root = CellId::from_face(face_idx);
        if !bbox_closed_in_cell(root, &bbox) {
            continue;
        }
        n_closed += 1;
        closed_root = Some(root);
        if bbox_margin_in_cell(root, &bbox) {
            margin_root = Some(root);
        }
    }
    if n_closed != 1 {
        return None;
    }
    let root = margin_root.unwrap_or_else(|| closed_root.expect("n_closed == 1"));

    let cell = deepest_margin_child(root, &bbox, minx, miny, maxx, maxy);
    Some(hilbert_reconcile(cell, &bbox, minx, miny, maxx, maxy))
}

/// Ensure the halfspace result Hilbert-contains same-face corner/edge samples.
///
/// Cube-edge lon/lat segments can closed-contain under halfspaces in a sibling
/// of the Hilbert-owning cell (shared edge, dual leaf assignment). When the
/// halfspace cell misses those leaves, prefer the single-face Hilbert LCA of
/// the samples when it still closed-contains the bbox. Never walk the
/// halfspace cell up to a face root solely for dual-edge samples — geometric
/// closed containment remains authoritative there.
fn hilbert_reconcile(
    halfspace: CellId,
    bbox: &LonLatBBox,
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
) -> CellId {
    let samples = bbox_hilbert_samples(minx, miny, maxx, maxy);
    if hilbert_samples_ok(halfspace, &samples) {
        return halfspace;
    }
    // Solid area boxes keep geometric halfspace authority (corner leaves on a
    // face diagonal may dual-assign; that is not a non-containing verdict).
    // Zero-width / zero-height segments on cube edges need the Hilbert LCA:
    // halfspaces can pick the wrong sibling of a shared edge.
    // Plain == so -0.0 and +0.0 count as a zero-width/height edge.
    #[expect(
        clippy::float_cmp,
        reason = "finite bbox edges; signed zero must match"
    )]
    let degenerate = minx == maxx || miny == maxy;
    if !degenerate {
        return halfspace;
    }
    let face = halfspace.face();
    let same_face: Vec<CellId> = samples
        .iter()
        .copied()
        .filter(|leaf| leaf.face() == face)
        .collect();
    if same_face.len() >= 2
        && let Some(mut lca) = common_ancestor(&same_face)
    {
        // Walk LCA up only until it closed-contains.
        loop {
            if bbox_closed_in_cell(lca, bbox) && hilbert_samples_ok(lca, &samples) {
                return lca;
            }
            let level = lca.level();
            if level == 0 {
                break;
            }
            match lca.parent(level - 1) {
                Some(p) => lca = p,
                None => break,
            }
        }
    }
    halfspace
}

/// Corner + edge-midpoint L30 leaves of the lon/lat rectangle.
fn bbox_hilbert_samples(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Vec<CellId> {
    let mx = f64::midpoint(minx, maxx);
    let my = f64::midpoint(miny, maxy);
    let pts = [
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (mx, miny),
        (mx, maxy),
        (minx, my),
        (maxx, my),
        (mx, my),
    ];
    let mut leaves: Vec<CellId> = pts
        .iter()
        .map(|&(lon, lat)| CellId::from_lonlat(lon, lat))
        .collect();
    leaves.sort_unstable();
    leaves.dedup();
    leaves
}

/// True when every sample whose face matches `cell` is Hilbert-contained.
///
/// Samples dual-assigned to another face are ignored (cube-edge leaf policy);
/// if none share the cell's face the check is vacuously true.
fn hilbert_samples_ok(cell: CellId, samples: &[CellId]) -> bool {
    let face = cell.face();
    for &leaf in samples {
        if leaf.face() != face {
            continue;
        }
        if !cell.contains(leaf) {
            return false;
        }
    }
    true
}

/// Descend while a child **provably** (positive-margin) contains the bbox.
///
/// HARD INVARIANT: never descend on near-zero halfspace mins. If no child
/// clears [`bbox_margin_in_cell`], stop at the current coarser cell that does
/// contain the bbox (via the closed root / prior margin steps).
fn deepest_margin_child(
    root: CellId,
    bbox: &LonLatBBox,
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
) -> CellId {
    let mut current = root;
    loop {
        let Some(children) = current.children() else {
            return current;
        };

        let mut margin = [CellId::from_face(0); 4];
        let mut n = 0_usize;
        for child in children {
            if bbox_margin_in_cell(child, bbox) {
                margin[n] = child;
                n += 1;
            }
        }

        match n {
            0 => return current,
            1 => current = margin[0],
            _ => {
                let Some(pick) = canonical_owner(&margin[..n], minx, miny, maxx, maxy) else {
                    return current;
                };
                if !bbox_margin_in_cell(pick, bbox) {
                    return current;
                }
                current = pick;
            },
        }
    }
}

/// Among containing candidates, the one that Hilbert-owns the bbox center leaf.
fn canonical_owner(
    candidates: &[CellId],
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
) -> Option<CellId> {
    let lon = f64::midpoint(minx, maxx);
    let lat = f64::midpoint(miny, maxy);
    let leaf = CellId::from_lonlat(lon, lat);
    candidates.iter().copied().find(|&cell| cell.contains(leaf))
}

/// Closed lon/lat rectangle in radians.
#[derive(Clone, Copy, Debug)]
struct LonLatBBox {
    lon0: f64,
    lon1: f64,
    lat0: f64,
    lat1: f64,
}

/// Relative closed-halfspace slack as a fraction of the edge-normal magnitude.
/// Absolute EPS is unsound near cube vertices / micro-boxes: a real geometric
/// protrusion of ~1e-12 class (e.g. face-3 edge min ≈ −7e-13 on a 1e-4° box at
/// (−180,−45)) sits inside a fixed 1e-12 band and false-accepts a non-containing
/// face. Scale with `|n|` so float noise (~ε_mach · `|n| · |p|`, unit-sphere
/// `|p| ≈ 1`) is absorbed while true outside mins reject. Dual face-edge roots
/// still dual-match when both mins are true zeros within this band; otherwise
/// multi-face raise (conservative).
const CLOSED_HALFSPACE_REL: f64 = 64.0 * f64::EPSILON; // ~1.4e-14

/// Floor on the positive margin required for descent. Must sit above the
/// float-noise band that previously produced slightly-positive false mins on
/// cells that do not actually contain the bbox (~1e-13..1e-12 class).
const MARGIN_ABS_FLOOR: f64 = 1e-11;

/// Relative margin fraction of the edge-normal magnitude so deep cells keep a
/// fractional clearance (tiny `|v_i × v_{i+1}|`) rather than a one-size floor
/// that either blocks all high-level descent or is too weak at face roots.
const MARGIN_REL: f64 = 1e-8;

/// Scale of an edge normal for relative halfspace tolerances.
fn normal_scale(normal: Point3) -> f64 {
    let n2 = normal.x * normal.x + normal.y * normal.y + normal.z * normal.z;
    n2.sqrt().max(1e-30)
}

/// Closed-containment slack: relative to `|normal|` (unit-sphere points).
fn closed_eps(normal: Point3) -> f64 {
    CLOSED_HALFSPACE_REL * normal_scale(normal)
}

/// Positive halfspace margin scaled to the edge-normal magnitude.
fn margin_eps(normal: Point3) -> f64 {
    let n_scale = normal_scale(normal);
    MARGIN_ABS_FLOOR.max(MARGIN_REL * n_scale)
}

/// Closed containment with relative root slack: min ≥ −[`closed_eps`].
fn bbox_closed_in_cell(id: CellId, bbox: &LonLatBBox) -> bool {
    let vertices = Cell::from_id(id).vertices_point();
    for k in 0..4 {
        let a = vertices[k];
        let b = vertices[(k + 1) & 3];
        let normal = a.cross(b);
        if side_plane_min(normal, bbox) < -closed_eps(normal) {
            return false;
        }
    }
    true
}

/// Provable positive-margin containment used for **descent** only.
fn bbox_margin_in_cell(id: CellId, bbox: &LonLatBBox) -> bool {
    let vertices = Cell::from_id(id).vertices_point();
    for k in 0..4 {
        let a = vertices[k];
        let b = vertices[(k + 1) & 3];
        let normal = a.cross(b);
        if side_plane_min(normal, bbox) < margin_eps(normal) {
            return false;
        }
    }
    true
}

/// Analytic minimum of `f(lon,lat) = cos(lat)*(nx cos lon + ny sin lon) + nz sin(lat)`
/// over the closed rectangle `[lon0,lon1]×[lat0,lat1]` (radians).
fn side_plane_min(normal: Point3, bbox: &LonLatBBox) -> f64 {
    let (nx, ny, nz) = (normal.x, normal.y, normal.z);
    let LonLatBBox {
        lon0,
        lon1,
        lat0,
        lat1,
    } = *bbox;

    let mut min_val = f64::INFINITY;

    // Four corners.
    for &lon in &[lon0, lon1] {
        for &lat in &[lat0, lat1] {
            min_val = min_val.min(plane_dot(nx, ny, nz, lon, lat));
        }
    }

    // Longitude edges (fixed lon): minimize over lat.
    min_val = min_val.min(lat_edge_min(nx, ny, nz, lon0, lat0, lat1));
    min_val = min_val.min(lat_edge_min(nx, ny, nz, lon1, lat0, lat1));

    // Latitude edges (fixed lat): minimize over lon.
    min_val = min_val.min(lon_edge_min(nx, ny, nz, lat0, lon0, lon1));
    min_val = min_val.min(lon_edge_min(nx, ny, nz, lat1, lon0, lon1));

    // Interior stationary longitudes of the horizontal sinusoid: atan2(ny,nx)
    // and +π. At each, minimize over lat.
    let phi = ny.atan2(nx);
    let two_pi = std::f64::consts::TAU;
    for k in 0..2 {
        let mut lon = phi + std::f64::consts::PI * f64::from(k);
        // Fold into [lon0, lon0 + 2π).
        let mut t = (lon - lon0) / two_pi;
        t = t.floor();
        lon -= t * two_pi;
        if lon > lon1 + 1e-15 {
            lon -= two_pi;
        }
        if lon >= lon0 - 1e-15 && lon <= lon1 + 1e-15 {
            let lon = lon.clamp(lon0, lon1);
            min_val = min_val.min(lat_edge_min(nx, ny, nz, lon, lat0, lat1));
        }
    }

    min_val
}

fn plane_dot(nx: f64, ny: f64, nz: f64, lon: f64, lat: f64) -> f64 {
    let (sin_lon, cos_lon) = lon.sin_cos();
    let (sin_lat, cos_lat) = lat.sin_cos();
    cos_lat * (nx * cos_lon + ny * sin_lon) + nz * sin_lat
}

/// Min of f at fixed `lon` over `lat ∈ [lat0, lat1]`.
fn lat_edge_min(nx: f64, ny: f64, nz: f64, lon: f64, lat0: f64, lat1: f64) -> f64 {
    let (sin_lon, cos_lon) = lon.sin_cos();
    let s = nx * cos_lon + ny * sin_lon;
    // g(lat) = cos(lat)*s + sin(lat)*nz = R cos(lat - α), α = atan2(nz, s).
    // Critical points α (max) and α+π (min).
    let mut min_val = plane_dot(nx, ny, nz, lon, lat0).min(plane_dot(nx, ny, nz, lon, lat1));
    let alpha = nz.atan2(s);
    for k in 0..2 {
        let mut lat = alpha + std::f64::consts::PI * f64::from(k);
        // Latitudes live in [-π, π] after one wrap; clamp domain is [-π/2, π/2].
        if lat > std::f64::consts::PI {
            lat -= std::f64::consts::TAU;
        } else if lat < -std::f64::consts::PI {
            lat += std::f64::consts::TAU;
        }
        if lat >= lat0 - 1e-15 && lat <= lat1 + 1e-15 {
            min_val = min_val.min(plane_dot(nx, ny, nz, lon, lat.clamp(lat0, lat1)));
        }
    }
    min_val
}

/// Min of f at fixed `lat` over `lon ∈ [lon0, lon1]`.
fn lon_edge_min(nx: f64, ny: f64, nz: f64, lat: f64, lon0: f64, lon1: f64) -> f64 {
    let cos_lat = lat.cos();
    let mut min_val = plane_dot(nx, ny, nz, lon0, lat).min(plane_dot(nx, ny, nz, lon1, lat));
    if cos_lat.abs() < 1e-18 {
        // Pole: independent of lon.
        return min_val;
    }
    let phi = ny.atan2(nx);
    let two_pi = std::f64::consts::TAU;
    for k in 0..2 {
        let mut lon = phi + std::f64::consts::PI * f64::from(k);
        let mut t = (lon - lon0) / two_pi;
        t = t.floor();
        lon -= t * two_pi;
        if lon > lon1 + 1e-15 {
            lon -= two_pi;
        }
        if lon >= lon0 - 1e-15 && lon <= lon1 + 1e-15 {
            min_val = min_val.min(plane_dot(nx, ny, nz, lon.clamp(lon0, lon1), lat));
        }
    }
    min_val
}

fn point_from_lonlat(lon: f64, lat: f64) -> Point3 {
    if lat >= 90.0 {
        return Point3::new(0.0, 0.0, 1.0);
    }
    if lat <= -90.0 {
        return Point3::new(0.0, 0.0, -1.0);
    }
    lonlat_to_point(lon, lat)
}

fn common_ancestor(leaves: &[CellId]) -> Option<CellId> {
    let mut cell = leaves[0];
    for &other in &leaves[1..] {
        cell = lca(cell, other)?;
    }
    Some(cell)
}

fn lca(a: CellId, b: CellId) -> Option<CellId> {
    let lsb = |id: CellId| id.raw() & id.raw().wrapping_neg();
    let mut bits = a.raw() ^ b.raw();
    let lsb_a = lsb(a);
    let lsb_b = lsb(b);
    if bits < lsb_a {
        bits = lsb_a;
    }
    if bits < lsb_b {
        bits = lsb_b;
    }
    let msb = 63_u32.saturating_sub(bits.leading_zeros());
    if msb > 60 {
        return None;
    }
    let level = ((60 - msb) / 2) as u8;
    a.parent(level)
}

#[cfg(test)]
#[path = "bounding_tests.rs"]
mod tests;
