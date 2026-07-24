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
    reason = "minx/miny/maxx/maxy are the domain bbox spellings"
)]

use super::cell::Cell;
use super::cellid::CellId;
use super::projection::{NUM_FACES, Point3, lonlat_to_point};
use crate::geometry::{Bounds, Shape};

/// Deepest S2 cell whose region contains the shape (bbox contract).
///
/// `None` when the shape has no coordinates, or when no single cell contains
/// the lon/lat bounding box (multi-face span). Near cell boundaries the result
/// may be one level coarser than the theoretical deepest (always containing).
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
    bounding_cell_bbox(bounds)
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
#[inline]
fn normal_scale(normal: Point3) -> f64 {
    let n2 = normal.x * normal.x + normal.y * normal.y + normal.z * normal.z;
    n2.sqrt().max(1e-30)
}

/// Closed-containment slack: relative to `|normal|` (unit-sphere points).
#[inline]
fn closed_eps(normal: Point3) -> f64 {
    CLOSED_HALFSPACE_REL * normal_scale(normal)
}

/// Positive halfspace margin scaled to the edge-normal magnitude.
#[inline]
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

#[inline]
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
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring};

    fn line(a: (f64, f64), b: (f64, f64)) -> Shape {
        Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_unchecked_xy(a.0, a.1),
                Point::new_unchecked_xy(b.0, b.1),
            ]))
            .expect("test line"),
        )
    }

    fn assert_hilbert_contains_samples(cell: CellId, samples: &[(f64, f64)]) {
        for &(lon, lat) in samples {
            assert!(
                cell.contains(CellId::from_lonlat(lon, lat)),
                "cell {} L{} must Hilbert-contain ({lon},{lat})",
                cell.token(),
                cell.level(),
            );
        }
    }

    /// Geometric soundness: closed halfspaces cover the whole bbox; no child
    /// clears the positive-margin gate (deepest-provable). Degenerate
    /// cube-edge segments also require same-face Hilbert sample containment.
    fn bbox_sound_ok(cell: CellId, minx: f64, miny: f64, maxx: f64, maxy: f64) {
        let bbox = LonLatBBox {
            lon0: minx.to_radians(),
            lon1: maxx.to_radians(),
            lat0: miny.to_radians(),
            lat1: maxy.to_radians(),
        };
        assert!(
            bbox_closed_in_cell(cell, &bbox),
            "cell {} L{} must closed-contain bbox ({minx},{miny},{maxx},{maxy})",
            cell.token(),
            cell.level(),
        );
        let samples = bbox_hilbert_samples(minx, miny, maxx, maxy);
        #[expect(
            clippy::float_cmp,
            reason = "finite bbox edges; signed zero must match"
        )]
        let degenerate = minx == maxx || miny == maxy;
        if degenerate {
            assert!(
                hilbert_samples_ok(cell, &samples),
                "cell {} L{} must Hilbert-contain same-face samples of degenerate bbox",
                cell.token(),
                cell.level(),
            );
        }
        // Deepest-provable: no child clears the positive margin.
        if let Some(children) = cell.children() {
            for child in children {
                assert!(
                    !bbox_margin_in_cell(child, &bbox),
                    "cell {} L{} is not deepest-provable: child {} still margin-contains",
                    cell.token(),
                    cell.level(),
                    child.token(),
                );
            }
        }
    }

    fn dense_edge_geom_ok(cell: CellId, minx: f64, miny: f64, maxx: f64, maxy: f64) {
        bbox_sound_ok(cell, minx, miny, maxx, maxy);
    }

    /// Off-seam Hilbert densify (stricter; only where face assignment is unique).
    fn dense_edge_hilbert_ok(cell: CellId, minx: f64, miny: f64, maxx: f64, maxy: f64) {
        let n = 10_000_usize;
        for i in 0..n {
            let t = i as f64 / (n - 1) as f64;
            let lon = minx + t * (maxx - minx);
            let lat = miny + t * (maxy - miny);
            for (x, y) in [(lon, miny), (lon, maxy), (minx, lat), (maxx, lat)] {
                assert!(
                    cell.contains(CellId::from_lonlat(x, y)),
                    "cell {} L{} dense miss at ({x},{y}) for bbox ({minx},{miny},{maxx},{maxy})",
                    cell.token(),
                    cell.level(),
                );
            }
        }
    }

    #[test]
    fn point_is_exact_l30_leaf() {
        let shape = Shape::Point(Point::new_unchecked_xy(13.4, 52.5));
        let cell = bounding_cell(&shape).expect("point");
        assert_eq!(cell, CellId::from_lonlat(13.4, 52.5));
        assert_eq!(cell.level(), 30);
    }

    #[test]
    fn multipoint_uses_bbox_path_same_as_box() {
        let shape = Shape::MultiPoint(CoordSeq::from(vec![
            Point::new_unchecked_xy(0.1, 0.1),
            Point::new_unchecked_xy(0.2, 0.2),
        ]));
        let cell = bounding_cell(&shape).expect("same face");
        let bbox = bounding_cell_bbox(Bounds::new_unchecked(0.1, 0.1, 0.2, 0.2)).expect("bbox");
        assert_eq!(cell, bbox);
        dense_edge_geom_ok(cell, 0.1, 0.1, 0.2, 0.2);
    }

    #[test]
    fn multipoint_oracle_repros_match_bbox() {
        // Leaf-LCA returned non-containing L7 'a8eb4'; bbox path is L6 'a8eb'.
        let a = Shape::MultiPoint(CoordSeq::from(vec![
            Point::new_unchecked_xy(170.0, -60.0),
            Point::new_unchecked_xy(170.2, -59.8),
        ]));
        let cell_a = bounding_cell(&a).expect("a");
        assert_eq!(cell_a.token(), "a8eb");
        assert_eq!(cell_a.level(), 6);
        assert_eq!(
            cell_a,
            bounding_cell_bbox(Bounds::new_unchecked(170.0, -60.0, 170.2, -59.8)).expect("box a")
        );
        // Leaf-LCA multi-face raise; face root '3' closed-contains the bbox.
        let b = Shape::MultiPoint(CoordSeq::from(vec![
            Point::new_unchecked_xy(45.0, -20.0),
            Point::new_unchecked_xy(45.2, -19.8),
        ]));
        let cell_b = bounding_cell(&b).expect("b");
        assert_eq!(cell_b.token(), "3");
        assert_eq!(cell_b.level(), 0);
        assert_eq!(
            cell_b,
            bounding_cell_bbox(Bounds::new_unchecked(45.0, -20.0, 45.2, -19.8)).expect("box b")
        );
    }

    #[test]
    fn multipoint_multi_face_is_none() {
        let shape = Shape::MultiPoint(CoordSeq::from(vec![
            Point::new_unchecked_xy(-100.0, 0.0),
            Point::new_unchecked_xy(100.0, 0.0),
        ]));
        assert!(bounding_cell(&shape).is_none());
    }

    #[test]
    fn multipoint_seam_straddle_matches_bbox() {
        let shape = Shape::MultiPoint(CoordSeq::from(vec![
            Point::new_unchecked_xy(45.0, 0.5),
            Point::new_unchecked_xy(46.0, 0.5),
        ]));
        assert_eq!(
            bounding_cell(&shape),
            bounding_cell_bbox(Bounds::new_unchecked(45.0, 0.5, 46.0, 0.5))
        );
    }

    #[test]
    fn berlin_single_cell() {
        let cell = bounding_cell_bbox(Bounds::new_unchecked(13.3, 52.4, 13.5, 52.6)).expect("cell");
        assert_eq!(cell.token(), "47a85");
        assert_eq!(cell.level(), 8);
        assert_hilbert_contains_samples(cell, &[
            (13.3, 52.4),
            (13.5, 52.4),
            (13.5, 52.6),
            (13.3, 52.6),
        ]);
        dense_edge_hilbert_ok(cell, 13.3, 52.4, 13.5, 52.6);
    }

    /// ~10m Berlin box: must never over-descend to a non-containing leaf.
    #[test]
    fn berlin_10m_box_always_contains() {
        let minx = 13.4;
        let miny = 52.5;
        let maxx = 13.4001;
        let maxy = 52.5001;
        let cell = bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)).expect("cell");
        dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
        assert!(
            cell.contains(CellId::from_lonlat(13.40005, 52.50005)),
            "cell {} L{} must contain interior",
            cell.token(),
            cell.level(),
        );
        // Corners under closed halfspaces (Hilbert face dual-assign may
        // disagree on exact boundary leaves).
        for &(x, y) in &[
            (13.4, 52.5),
            (13.4001, 52.5),
            (13.4001, 52.5001),
            (13.4, 52.5001),
        ] {
            let leaf = CellId::from_lonlat(x, y);
            if leaf.face() == cell.face() {
                assert!(
                    cell.contains(leaf),
                    "cell {} L{} must contain corner leaf ({x},{y})",
                    cell.token(),
                    cell.level(),
                );
            }
        }
        assert!(
            cell.level() < 30,
            "10m box must not over-descend to L30, got L{}",
            cell.level()
        );
        // Oracle deepest ~L17; conservatism may stop a level early.
        assert!(
            (10..=20).contains(&cell.level()),
            "10m box level out of expected band, got L{}",
            cell.level()
        );
    }

    #[test]
    fn multi_face_wide_box_is_none() {
        assert!(bounding_cell_bbox(Bounds::new_unchecked(-100.0, -40.0, 100.0, 40.0)).is_none());
    }

    #[test]
    fn multi_face_moderate_box_is_none() {
        assert!(bounding_cell_bbox(Bounds::new_unchecked(-50.0, 10.0, -32.0, 15.0)).is_none());
    }

    #[test]
    fn line_0_0_to_0_45_contains_samples() {
        // Zero-width face-center meridian: margin-only may stop at the face
        // root (min≈0 on child edges). Always containing is the invariant.
        let shape = line((0.0, 0.0), (0.0, 45.0));
        let cell = bounding_cell(&shape).expect("single-face");
        dense_edge_geom_ok(cell, 0.0, 0.0, 0.0, 45.0);
        assert_hilbert_contains_samples(cell, &[
            (0.0, 0.0),
            (0.0, 10.0),
            (0.0, 30.0),
            (0.0, 44.9),
            (0.0, 45.0),
        ]);
    }

    #[test]
    fn face_diagonal_meridian_135_lat0_to_5_raises() {
        for lon in [135.0, -135.0] {
            assert!(
                bounding_cell_bbox(Bounds::new_unchecked(lon, 0.0, lon, 5.0)).is_none(),
                "lon={lon} lat 0..5 must multi-face"
            );
        }
    }

    #[test]
    fn face_boundary_meridian_135_short_raises() {
        for lon in [-135.0, 135.0] {
            assert!(
                bounding_cell_bbox(Bounds::new_unchecked(lon, -10.0, lon, -9.7)).is_none(),
                "lon={lon} short face-boundary segment must multi-face"
            );
        }
    }

    #[test]
    fn cube_edge_meridian_45_to_60_raise_or_closed_contain() {
        for lon in [90.0, -90.0, -180.0, 180.0] {
            match bounding_cell_bbox(Bounds::new_unchecked(lon, 45.0, lon, 60.0)) {
                None => {},
                Some(cell) => dense_edge_geom_ok(cell, lon, 45.0, lon, 60.0),
            }
        }
    }

    #[test]
    fn south_pole_lon90_closed_contains() {
        let cell =
            bounding_cell_bbox(Bounds::new_unchecked(90.0, -80.0, 91.0, -79.0)).expect("cell");
        dense_edge_geom_ok(cell, 90.0, -80.0, 91.0, -79.0);
    }

    #[test]
    fn antimeridian_south_closed_contains() {
        for (minx, miny, maxx, maxy) in [
            (-180.0, -85.0, -179.0, -84.0),
            (-180.0, -80.0, -179.0, -79.0),
            (-180.0, -75.0, -179.0, -74.0),
        ] {
            let cell =
                bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)).expect("cell");
            dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
        }
    }

    #[test]
    fn seam_box_45_is_multi_face_or_dense_geom() {
        match bounding_cell_bbox(Bounds::new_unchecked(45.0, 0.0, 46.0, 1.0)) {
            None => {},
            Some(cell) => dense_edge_geom_ok(cell, 45.0, 0.0, 46.0, 1.0),
        }
    }

    #[test]
    fn exact_seam_repro_neg135_lat_neg35() {
        // Boundary-adjacent: may coarsen vs theoretical deepest under margin.
        let cell =
            bounding_cell_bbox(Bounds::new_unchecked(-135.0, -35.0, -134.8, -34.8)).expect("cell");
        dense_edge_geom_ok(cell, -135.0, -35.0, -134.8, -34.8);
    }

    #[test]
    fn exact_seam_repro_neg135_lat_0() {
        let cell =
            bounding_cell_bbox(Bounds::new_unchecked(-135.0, 0.0, -134.8, 0.2)).expect("cell");
        dense_edge_geom_ok(cell, -135.0, 0.0, -134.8, 0.2);
    }

    /// Skeptic counterexamples: 1e-6/1e-7 solid boxes that Strict over-descended.
    #[test]
    fn tiny_scale_solid_boxes_never_over_descend() {
        let cases = [
            (117.087_047_572_419_38, 38.620_414_518_783_92, 1e-6_f64),
            (138.685_538_819_117_9, -24.628_965_378_441_677, 1e-7),
            (-61.353_414_232_540_1, -22.102_649_469_574_658, 1e-6),
            (13.4, 52.5, 1e-4), // Berlin ~10m
            (0.0, 0.0, 1e-6),
            (-170.0, -80.0, 1e-6),
            (45.1, 0.1, 1e-6),
        ];
        for &(minx, miny, size) in &cases {
            let maxx = minx + size;
            let maxy = miny + size;
            if maxy > 90.0 || miny < -90.0 || maxx > 180.0 {
                continue;
            }
            let Some(cell) = bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy))
            else {
                continue;
            };
            dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
            // All same-face corners must be Hilbert-contained.
            for &(x, y) in &[
                (minx, miny),
                (maxx, miny),
                (maxx, maxy),
                (minx, maxy),
                (f64::midpoint(minx, maxx), f64::midpoint(miny, maxy)),
            ] {
                let leaf = CellId::from_lonlat(x, y);
                if leaf.face() == cell.face() {
                    assert!(
                        cell.contains(leaf),
                        "non-containing {} L{} at ({x},{y}) for size={size}",
                        cell.token(),
                        cell.level(),
                    );
                }
            }
        }
    }

    #[test]
    fn exact_seam_touch_matrix_no_false_reject() {
        let seams = [-135.0_f64, -45.0, 45.0, 135.0];
        let lats = [-40.0, -35.0, -20.0, -5.0, 0.0, 5.0, 20.0, 35.0, 40.0];
        let mut ok = 0_usize;
        let mut raised = 0_usize;
        for &lon0 in &seams {
            for &lat0 in &lats {
                for &(dw, dh) in &[(0.2_f64, 0.2), (0.05, 0.05), (1.0, 1.0), (5.0, 5.0)] {
                    let minx = lon0;
                    let maxx = lon0 + dw;
                    let miny = lat0;
                    let maxy = lat0 + dh;
                    if maxy > 90.0 || miny < -90.0 {
                        continue;
                    }
                    match bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)) {
                        None => raised += 1,
                        Some(cell) => {
                            dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
                            ok += 1;
                        },
                    }
                }
            }
        }
        for &lon in &seams {
            assert!(
                bounding_cell_bbox(Bounds::new_unchecked(lon, 0.0, lon, 5.0)).is_none(),
                "zero-width face edge lon={lon}"
            );
            raised += 1;
        }
        assert!(
            ok >= 100,
            "expected many single-face seam-touch successes, got {ok}"
        );
        assert!(
            raised >= 4,
            "expected genuine dual-root raises, got {raised}"
        );
    }

    #[test]
    fn small_equator_box_hilbert_contains_corners() {
        let cell = bounding_cell_bbox(Bounds::new_unchecked(0.0, 0.0, 1.0, 1.0)).expect("cell");
        assert_hilbert_contains_samples(cell, &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]);
        dense_edge_hilbert_ok(cell, 0.0, 0.0, 1.0, 1.0);
    }

    #[test]
    fn line_uses_envelope_hilbert() {
        let shape = line((-21.0, 21.5), (-1.2, 22.9));
        let cell = bounding_cell(&shape).expect("cell");
        let b = shape.bounds().expect("bounds");
        dense_edge_hilbert_ok(cell, b.minx(), b.miny(), b.maxx(), b.maxy());
    }

    #[test]
    fn poles_and_empty() {
        let north = bounding_cell(&Shape::Point(Point::new_unchecked_xy(0.0, 90.0))).expect("N");
        assert_eq!(north.level(), 30);
        assert!(bounding_cell(&Shape::empty_polygon()).is_none());
    }

    #[test]
    fn polygon_with_hole_uses_bbox() {
        let shell = Ring::from_trusted_closed(vec![
            Point::new_unchecked_xy(-1.0, -1.0),
            Point::new_unchecked_xy(1.0, -1.0),
            Point::new_unchecked_xy(1.0, 1.0),
            Point::new_unchecked_xy(-1.0, 1.0),
            Point::new_unchecked_xy(-1.0, -1.0),
        ]);
        let hole = Ring::from_trusted_closed(vec![
            Point::new_unchecked_xy(-0.2, -0.2),
            Point::new_unchecked_xy(-0.2, 0.2),
            Point::new_unchecked_xy(0.2, 0.2),
            Point::new_unchecked_xy(0.2, -0.2),
            Point::new_unchecked_xy(-0.2, -0.2),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![hole]));
        let cell = bounding_cell(&shape).expect("cell");
        dense_edge_hilbert_ok(cell, -1.0, -1.0, 1.0, 1.0);
    }

    #[test]
    fn false_reject_corpus_targets() {
        let cases = [
            (
                -163.431_550,
                34.574_955,
                -160.480_306,
                36.395_188,
                "7dd",
                4_u8,
            ),
            (-2.612_665, 41.780_618, -0.655_635, 43.761_980, "0d5", 4),
            (121.977_231, 41.851_431, 124.077_057, 42.468_080, "5e3", 4),
        ];
        for (minx, miny, maxx, maxy, token, level) in cases {
            let cell =
                bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)).expect("cell");
            assert_eq!(
                (cell.token().as_str(), cell.level()),
                (token, level),
                "bbox ({minx},{miny},{maxx},{maxy})"
            );
            dense_edge_hilbert_ok(cell, minx, miny, maxx, maxy);
        }
    }

    #[test]
    fn band_0_2_boxes_no_false_reject() {
        let lats = [
            35.3, 36.0, 38.0, 40.0, 42.0, 44.0, 44.8, -35.3, -36.0, -40.0, -44.0,
        ];
        let lons = [
            -160.0, -90.0, -45.0, -20.0, 0.0, 20.0, 45.0, 90.0, 120.0, 160.0,
        ];
        for &lat0 in &lats {
            for &lon0 in &lons {
                let minx = lon0;
                let maxx = lon0 + 0.2;
                let miny = lat0;
                let maxy = lat0 + 0.2;
                if !(maxy <= 90.0 && miny >= -90.0) {
                    continue;
                }
                match bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)) {
                    None => {},
                    Some(cell) => dense_edge_geom_ok(cell, minx, miny, maxx, maxy),
                }
            }
        }
    }

    /// Multi-scale soundness: 0 non-containing at ANY scale.
    #[test]
    fn multi_scale_soundness_zero_non_containing() {
        let sizes = [0.2_f64, 0.01, 1e-4, 1e-6];
        let lats = [
            -80.0, -60.0, -40.0, -20.0, 0.0, 20.0, 40.0, 52.5, 60.0, 80.0,
        ];
        let lons = [
            -170.0, -135.0, -90.0, -45.0, -20.0, 0.0, 13.4, 45.0, 90.0, 135.0, 170.0,
        ];
        let mut ok = 0_usize;
        for &size in &sizes {
            for &lat0 in &lats {
                for &lon0 in &lons {
                    let minx = lon0;
                    let maxx = lon0 + size;
                    let miny = lat0;
                    let maxy = lat0 + size;
                    if maxy > 90.0 || miny < -90.0 || maxx > 180.0 {
                        continue;
                    }
                    match bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy)) {
                        None => {},
                        Some(cell) => {
                            dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
                            let cx = f64::midpoint(minx, maxx);
                            let cy = f64::midpoint(miny, maxy);
                            let leaf = CellId::from_lonlat(cx, cy);
                            if leaf.face() == cell.face() {
                                assert!(
                                    cell.contains(leaf),
                                    "non-containing {} L{} for ({minx},{miny},{maxx},{maxy})",
                                    cell.token(),
                                    cell.level(),
                                );
                            }
                            ok += 1;
                        },
                    }
                }
            }
        }
        assert!(ok >= 200, "expected many successes, got {ok}");
    }

    #[test]
    fn soundness_matrix_success_is_dense_geom_or_none() {
        let cases: &[(f64, f64, f64, f64)] = &[
            (0.0, 0.0, 0.0, 45.0),
            (0.0, 0.0, 1.0, 1.0),
            (13.3, 52.4, 13.5, 52.6),
            (13.4, 52.5, 13.4001, 52.5001),
            (-1.0, 0.0, 0.0, 0.0),
            (90.0, -80.0, 91.0, -79.0),
            (-180.0, -85.0, -179.0, -84.0),
            (0.0, 45.0, 0.0, 88.0),
            (90.0, 45.0, 90.0, 60.0),
            (-90.0, 45.0, -90.0, 60.0),
            (-180.0, 45.0, -180.0, 60.0),
            (-135.0, -10.0, -135.0, -9.7),
            (135.0, -10.0, 135.0, -9.7),
            (135.0, 0.0, 135.0, 5.0),
            (-135.0, 0.0, -135.0, 5.0),
            (-45.0, 10.0, -40.0, 15.0),
            (135.0, -5.0, 140.0, 0.0),
            (-90.0, -40.0, -90.0, -39.9),
            (0.0, -90.0, 0.0, -88.0),
            (45.0, 0.0, 45.0, 1.0),
            (-135.0, 20.0, -134.5, 20.5),
            (45.0, 0.0, 46.0, 1.0),
            (-135.0, -35.0, -134.8, -34.8),
            (-135.0, 0.0, -134.8, 0.2),
        ];
        for &(minx, miny, maxx, maxy) in cases {
            let Some(cell) = bounding_cell_bbox(Bounds::new_unchecked(minx, miny, maxx, maxy))
            else {
                continue;
            };
            dense_edge_geom_ok(cell, minx, miny, maxx, maxy);
        }
        assert!(bounding_cell_bbox(Bounds::new_unchecked(-100.0, -40.0, 100.0, 40.0)).is_none());
        assert!(bounding_cell_bbox(Bounds::new_unchecked(-50.0, 10.0, -32.0, 15.0)).is_none());
        assert!(bounding_cell_bbox(Bounds::new_unchecked(135.0, 0.0, 135.0, 5.0)).is_none());
        assert!(bounding_cell_bbox(Bounds::new_unchecked(-135.0, 0.0, -135.0, 5.0)).is_none());
    }
}
