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
//!    - A child is selected only when it is the unique positive-margin child;
//!      shared-edge ownership never selects a candidate.
//!    - Zero-width/height regions are still checked as complete closed
//!      rectangles; cell-id ownership at a shared edge is not a proof.
//!
//! Coverer fail-open `Boundary` is intentionally unused here (correct for
//! outer covers, not multi-face proof). No amplification beyond the bbox.
#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use crate::geometry::{Bounds, Shape};
use crate::grid::s2::cell::Cell;
use crate::grid::s2::cellid::{CellId, Face};
use crate::grid::s2::projection::{Point3, lonlat_to_point};

/// Deepest S2 cell whose region contains the shape (bbox contract).
///
/// `None` when the shape has no coordinates, or when no single cell contains
/// the lon/lat bounding box (multi-face span). Near cell boundaries the result
/// may be one level coarser than the theoretical deepest (always containing).
///
/// Antimeridian-crossing shapes use the complete normalized spherical extent,
/// represented as two closed longitude windows. A candidate is accepted only
/// when both windows pass the cell halfspaces; vertex ownership is never a
/// containment certificate.
pub(crate) fn bounding_cell(shape: &Shape) -> Option<CellId> {
    let mut n_points = 0_usize;
    let mut only: Option<Point3> = None;
    shape.for_each_point(|point| {
        n_points += 1;
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
    // Seam / wrap: preserve the narrow physical extent instead of taking the
    // 358-degree planar envelope. The windows are proved as complete
    // rectangles, including their interiors and shared seam endpoints.
    if bounds.minx() > bounds.maxx() || shape.crosses_antimeridian() {
        let bounds = crate::geometry::geographic_crossing_bounds(shape)?;
        return bounding_cell_regions(bounds);
    }
    bounding_cell_bbox(bounds)
}

/// Deepest cell whose closed spherical halfspaces contain every rectangle in
/// a seam-aware extent. L30 leaves only choose an ancestor path; acceptance is
/// exclusively by complete halfspace proofs.
fn bounding_cell_regions(bounds: Bounds) -> Option<CellId> {
    let windows = longitude_windows(bounds);
    let mut leaves = Vec::with_capacity(windows.len() * 4);
    for window in &windows {
        leaves.extend([
            CellId::from_lonlat(window.lon0.to_degrees(), bounds.miny()),
            CellId::from_lonlat(window.lon1.to_degrees(), bounds.miny()),
            CellId::from_lonlat(window.lon1.to_degrees(), bounds.maxy()),
            CellId::from_lonlat(window.lon0.to_degrees(), bounds.maxy()),
        ]);
    }
    leaves.sort_unstable();
    leaves.dedup();
    if leaves.is_empty() {
        return None;
    }
    let mut level = leaves[0].level();
    let mut candidate = loop {
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
    loop {
        let vertices = Cell::from_id(candidate).vertices_point();
        if windows
            .iter()
            .all(|window| bbox_closed_in_cell(&vertices, window))
        {
            return Some(deepest_margin_child_regions(candidate, &windows));
        }
        if candidate.level() == 0 {
            return None;
        }
        candidate = candidate.parent(candidate.level() - 1)?;
    }
}

fn longitude_windows(bounds: Bounds) -> Vec<LonLatBBox> {
    let lat0 = bounds.miny().to_radians();
    let lat1 = bounds.maxy().to_radians();
    if bounds.minx() <= bounds.maxx() {
        vec![LonLatBBox {
            lon0: bounds.minx().to_radians(),
            lon1: bounds.maxx().to_radians(),
            lat0,
            lat1,
        }]
    } else {
        vec![
            LonLatBBox {
                lon0: bounds.minx().to_radians(),
                lon1: std::f64::consts::PI,
                lat0,
                lat1,
            },
            LonLatBBox {
                lon0: -std::f64::consts::PI,
                lon1: bounds.maxx().to_radians(),
                lat0,
                lat1,
            },
        ]
    }
}

fn deepest_margin_child_regions(root: CellId, windows: &[LonLatBBox]) -> CellId {
    let mut current = root;
    loop {
        let Some(children) = current.children() else {
            return current;
        };
        let mut containing = children.into_iter().filter(|&child| {
            let vertices = Cell::from_id(child).vertices_point();
            windows
                .iter()
                .all(|window| bbox_margin_in_cell(&vertices, window))
        });
        let Some(child) = containing.next() else {
            return current;
        };
        // Positive-margin children are disjoint. Retain the parent if
        // arithmetic ever reports a second one rather than choosing by a
        // point owner at a shared boundary.
        if containing.next().is_some() {
            return current;
        }
        current = child;
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
    for face in Face::ALL {
        let root = CellId::from_face(face);
        let vertices = Cell::from_id(root).vertices_point();
        if !bbox_closed_in_cell(&vertices, &bbox) {
            continue;
        }
        n_closed += 1;
        closed_root = Some(root);
        if bbox_margin_in_cell(&vertices, &bbox) {
            margin_root = Some(root);
        }
    }
    if n_closed != 1 {
        return None;
    }
    let root = margin_root.unwrap_or_else(|| closed_root.expect("n_closed == 1"));

    Some(deepest_margin_child(root, &bbox))
}

/// Corner + edge-midpoint L30 leaves used only by independent test oracles.
#[cfg(test)]
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
#[cfg(test)]
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
fn deepest_margin_child(root: CellId, bbox: &LonLatBBox) -> CellId {
    let mut current = root;
    loop {
        let Some(children) = current.children() else {
            return current;
        };

        let mut margin = [CellId::from_face(Face::ALL[0]); 4];
        let mut n = 0_usize;
        for child in children {
            let vertices = Cell::from_id(child).vertices_point();
            if bbox_margin_in_cell(&vertices, bbox) {
                margin[n] = child;
                n += 1;
            }
        }

        match n {
            1 => current = margin[0],
            _ => return current,
        }
    }
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
fn bbox_closed_in_cell(vertices: &[Point3; 4], bbox: &LonLatBBox) -> bool {
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
fn bbox_margin_in_cell(vertices: &[Point3; 4], bbox: &LonLatBBox) -> bool {
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

#[cfg(test)]
#[path = "bounding_tests.rs"]
mod tests;
