#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! The geometric view of an S2 cell: center, vertices, lon/lat boundary,
//! and exact spherical area. Decoded once from a [`CellId`], all `Copy` —
//! no heap, no wrapper types.

use super::cellid::{CellId, Face};
use super::projection::{
    Point3, face_uv_to_xyz, point_to_lonlat, siti_to_st, st_to_uv, valid_face_uv,
};
use crate::boundary::geographic::EARTH_AUTHALIC_RADIUS_M;
use crate::geometry::{Point, Polygon, Ring, Shape};

/// A cell's (u, v) bounds on its face.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct UvRect {
    pub u_lo: f64,
    pub u_hi: f64,
    pub v_lo: f64,
    pub v_hi: f64,
}

/// Z components of the face U/V axes (the `rect_bound` corner rule).
const U_AXIS_Z: [f64; 6] = [0.0, 0.0, 0.0, -1.0, -1.0, 0.0];
const V_AXIS_Z: [f64; 6] = [1.0, 1.0, 0.0, 0.0, 0.0, 0.0];
/// Lowest |latitude| reached by the polar faces: `asin(sqrt(1/3))`.
const POLE_MIN_LAT_DEG: f64 = 35.264_389_682_754_654;

/// A cell's exact lon/lat bound, in degrees. `lng_lo > lng_hi` means the
/// bound crosses the antimeridian; `(-180, 180)` means all longitudes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct LatLngRect {
    pub lat_lo: f64,
    pub lat_hi: f64,
    pub lng_lo: f64,
    pub lng_hi: f64,
}

impl LatLngRect {
    /// The closed-form level-0 face bounds.
    fn face_bound(face: Face) -> Self {
        let err = f64::EPSILON.to_degrees();
        let (lat_lo, lat_hi, lng_lo, lng_hi) = match face {
            Face::F0 => (-45.0, 45.0, -45.0, 45.0),
            Face::F1 => (-45.0, 45.0, 45.0, 135.0),
            Face::F2 => (POLE_MIN_LAT_DEG, 90.0, -180.0, 180.0),
            Face::F3 => (-45.0, 45.0, 135.0, -135.0), // crosses the antimeridian
            Face::F4 => (-45.0, 45.0, -135.0, -45.0),
            Face::F5 => (-90.0, -POLE_MIN_LAT_DEG, -180.0, 180.0),
        };
        Self {
            lat_lo: (lat_lo - err).max(-90.0),
            lat_hi: (lat_hi + err).min(90.0),
            lng_lo,
            lng_hi,
        }
    }

    /// Re-read `lng_lo..lng_hi` as the SHORTEST wrap-aware interval through
    /// the two longitudes it was built from, inflated by `err`, with
    /// endpoints normalized into `[-180, 180]` (the canonical S1 spelling:
    /// an interval touching the seam reads as wrapped, never as a plain
    /// interval poking past ±180 — the planar-certificate gate depends on
    /// this).
    fn shortest_lng(mut self, err: f64) -> Self {
        if self.lng_hi - self.lng_lo > 180.0 {
            // The short way crosses the antimeridian: swap to the wrapped
            // reading (lo > hi).
            (self.lng_lo, self.lng_hi) = (self.lng_hi, self.lng_lo);
        }
        self.lng_lo -= err;
        self.lng_hi += err;
        // Normalize the expanded endpoints back into [-180, 180]; crossing
        // an endpoint over the seam turns the interval wrapped.
        if self.lng_lo < -180.0 {
            self.lng_lo += 360.0;
        }
        if self.lng_hi > 180.0 {
            self.lng_hi -= 360.0;
        }
        // An interval touching the seam takes the wrapped spelling (the
        // canonical S1 reading, e.g. `[180, -177.5]`): the coverer then
        // fails open instead of planar-certifying against a rect that
        // misses the other spelling of the seam meridian, and the window
        // gate covers both spellings.
        if !self.is_full_lng() {
            if self.lng_lo <= -180.0 {
                self.lng_lo = 180.0;
            } else if self.lng_hi >= 180.0 {
                self.lng_hi = -180.0;
            }
        }
        self
    }

    /// A bound touching a pole spans all longitudes (canonical closure).
    fn polar_closure(mut self) -> Self {
        if self.lat_lo <= -90.0 + f64::EPSILON.to_degrees()
            || self.lat_hi >= 90.0 - f64::EPSILON.to_degrees()
        {
            self.lat_lo = self.lat_lo.max(-90.0);
            self.lat_hi = self.lat_hi.min(90.0);
            self.lng_lo = -180.0;
            self.lng_hi = 180.0;
        }
        self
    }

    /// Whether the bound crosses the antimeridian (wrapped reading).
    pub(crate) fn crosses_seam(self) -> bool {
        self.lng_lo > self.lng_hi
    }

    /// Whether the bound spans all longitudes.
    pub(crate) fn is_full_lng(self) -> bool {
        self.lng_lo <= -180.0 && self.lng_hi >= 180.0
    }

    /// The bound's longitude intervals in `[-180, 180]` (two when wrapped);
    /// the second is `None` for plain bounds.
    pub(crate) fn lng_windows(self) -> ((f64, f64), Option<(f64, f64)>) {
        if self.crosses_seam() {
            ((self.lng_lo, 180.0), Some((-180.0, self.lng_hi)))
        } else {
            ((self.lng_lo, self.lng_hi), None)
        }
    }
}

/// A decoded cell: id plus its face/UV extent.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Cell {
    id: CellId,
    face: Face,
    uv: UvRect,
}

impl Cell {
    /// Decode `id` (one `face_ij_orientation` pass).
    pub(crate) fn from_id(id: CellId) -> Self {
        let (u_lo, u_hi, v_lo, v_hi) = id.bound_uv();
        Self {
            id,
            face: id.face(),
            uv: UvRect {
                u_lo,
                u_hi,
                v_lo,
                v_hi,
            },
        }
    }

    /// The cell center on the unit sphere — the exact point the recursive
    /// subdivision splits at (not the UV-rect midpoint).
    pub(crate) fn center_point(self) -> Point3 {
        let (face, si, ti) = self.id.center_face_siti();
        face_uv_to_xyz(face, st_to_uv(siti_to_st(si)), st_to_uv(siti_to_st(ti))).normalized()
    }

    /// The cell center as a lon/lat point.
    pub(crate) fn center_lonlat(self) -> Point {
        let (lon, lat) = point_to_lonlat(self.center_point());
        Point::new_unchecked_xy(lon, lat)
    }

    /// The four corners on the unit sphere, CCW from the (lo, lo) corner.
    pub(crate) fn vertices_point(self) -> [Point3; 4] {
        let UvRect {
            u_lo,
            u_hi,
            v_lo,
            v_hi,
        } = self.uv;
        [
            face_uv_to_xyz(self.face, u_lo, v_lo).normalized(),
            face_uv_to_xyz(self.face, u_hi, v_lo).normalized(),
            face_uv_to_xyz(self.face, u_hi, v_hi).normalized(),
            face_uv_to_xyz(self.face, u_lo, v_hi).normalized(),
        ]
    }

    /// The four corners as lon/lat points (same order as
    /// [`Self::vertices_point`]).
    pub(crate) fn vertices_lonlat(self) -> [Point; 4] {
        self.vertices_point().map(|vertex| {
            let (lon, lat) = point_to_lonlat(vertex);
            Point::new_unchecked_xy(lon, lat)
        })
    }

    /// The cell outline as a closed lon/lat `Polygon` shape, tagged by the
    /// caller (the four-corner proxy the coverer classifies, and the public
    /// ``boundary``).
    pub(crate) fn boundary_shape(self) -> Shape {
        let vertices = self.vertices_lonlat();
        let mut shell = vertices.to_vec();
        shell.push(vertices[0]);
        Shape::Polygon(Polygon::new(Ring::from_trusted_closed(shell), Vec::new()))
    }

    /// The cell's exact latitude/longitude bound (canonical S2
    /// `RectBounder` math): for level > 0 the latitude range is attained at
    /// one diagonally-opposite vertex pair (chosen by the z-maximizing
    /// corner) and the longitude range at the other pair; level-0 faces use
    /// the closed-form table. The bound provably contains the lon/lat of
    /// every point of the true spherical cell — the sound box proxy the
    /// coverer certifies against.
    pub(crate) fn rect_bound(self, vertices: &[Point; 4]) -> LatLngRect {
        if self.id.level() == 0 {
            return LatLngRect::face_bound(self.face);
        }
        // Corner rule: pick (i, j) maximizing |z| from the face axes' z
        // components and the cell's uv signs.
        let u = self.uv.u_lo + self.uv.u_hi;
        let v = self.uv.v_lo + self.uv.v_hi;
        let face_index = usize::from(self.face.get());
        let i = usize::from(if U_AXIS_Z[face_index] == 0.0 {
            u < 0.0
        } else {
            u > 0.0
        });
        let j = usize::from(if V_AXIS_Z[face_index] == 0.0 {
            v < 0.0
        } else {
            v > 0.0
        });
        // Corner (i, j) maps to the CCW vertex order of `vertices_lonlat`:
        // (0,0) -> 0, (1,0) -> 1, (1,1) -> 2, (0,1) -> 3 — reusing the four
        // conversions the caller already paid for.
        let corner = |i: usize, j: usize| -> (f64, f64) {
            let vertex = vertices[match (i, j) {
                (0, 0) => 0,
                (1, 0) => 1,
                (1, 1) => 2,
                _ => 3,
            }];
            (vertex.x, vertex.y)
        };
        let (_, lat_a) = corner(i, j);
        let (_, lat_b) = corner(1 - i, 1 - j);
        let (lng_a, _) = corner(i, 1 - j);
        let (lng_b, _) = corner(1 - i, j);
        // 2 * DBL_EPSILON covers normalization/rounding drift (canonical).
        let err = (2.0 * f64::EPSILON).to_degrees();
        let rect = LatLngRect {
            lat_lo: lat_a.min(lat_b) - err,
            lat_hi: lat_a.max(lat_b) + err,
            lng_lo: lng_a.min(lng_b),
            lng_hi: lng_a.max(lng_b),
        };
        rect.shortest_lng(err).polar_closure()
    }

    /// Exact closed-cell point containment (the canonical `S2Cell::Contains`):
    /// the point projects onto this cell's face within its closed UV bounds.
    /// Closed-set semantics: points on shared edges belong to every touching
    /// cell.
    pub(crate) fn contains_point(self, point: Point3) -> bool {
        let Some((u, v)) = valid_face_uv(self.face, point) else {
            return false;
        };
        u >= self.uv.u_lo && u <= self.uv.u_hi && v >= self.uv.v_lo && v <= self.uv.v_hi
    }

    /// Exact spherical area in steradians (two spherical triangles).
    pub(crate) fn area_steradians(self) -> f64 {
        let [a, b, c, d] = self.vertices_point();
        triangle_area(a, b, c) + triangle_area(a, c, d)
    }

    /// Exact geodesic area in square meters on the authalic sphere (the
    /// same model the H3 backend uses, so the two grids compare).
    pub(crate) fn area_m2(self) -> f64 {
        self.area_steradians() * EARTH_AUTHALIC_RADIUS_M * EARTH_AUTHALIC_RADIUS_M
    }
}

/// Area of the spherical triangle `(a, b, c)` in steradians: l'Huilier by
/// default, Girard for long skinny triangles where l'Huilier degrades, and
/// negative numerical residue clamped to zero (canonical S2 strategy —
/// stable from level-30 slivers up to whole faces).
fn triangle_area(a: Point3, b: Point3, c: Point3) -> f64 {
    let sa = b.angle(c);
    let sb = c.angle(a);
    let sc = a.angle(b);
    let s = 0.5 * (sa + sb + sc);
    if s >= 3e-4 {
        // Consider whether the triangle is long and skinny: the spherical
        // excess scales with s² here, so l'Huilier loses precision.
        let s2 = s * s;
        let dmin = s - sa.max(sb).max(sc);
        if dmin < 1e-2 * s * s2 * s2 {
            // Girard's formula benefits from the more accurate angles
            // between the great-circle normals.
            let ab = a.cross(b);
            let bc = b.cross(c);
            let ac = a.cross(c);
            let girard = (ab.angle(ac) - ab.angle(bc) + bc.angle(ac)).max(0.0);
            if dmin < s * (0.1 * girard) {
                return girard;
            }
        }
    }
    let tangents =
        (0.5 * s).tan() * (0.5 * (s - sa)).tan() * (0.5 * (s - sb)).tan() * (0.5 * (s - sc)).tan();
    4.0 * tangents.max(0.0).sqrt().atan()
}

#[cfg(test)]
mod tests {
    use super::super::projection::MAX_LEVEL;
    use super::*;

    fn sample_cells() -> Vec<CellId> {
        let mut cells = Vec::new();
        for &(lon, lat) in &[
            (0.0, 0.0),
            (13.4, 52.5),
            (-122.42, 37.77),
            (179.999, -45.0),
            (-179.999, 45.0),
            (0.0, 89.999),
            (0.0, -89.999),
            (45.0, 35.26),
        ] {
            let leaf = CellId::from_lonlat(lon, lat);
            for level in [0, 1, 5, 12, 20, MAX_LEVEL] {
                cells.push(leaf.parent(level).expect("coarser"));
            }
        }
        cells
    }

    /// Centers and vertices match the reference crate within float noise.
    #[test]
    fn geometry_matches_oracle() {
        for id in sample_cells() {
            let cell = Cell::from_id(id);
            let oracle = ::s2::cell::Cell::from(::s2::cellid::CellID(id.raw()));
            let center = cell.center_point();
            let oracle_center = oracle.center();
            assert!((center.x - oracle_center.0.x).abs() < 1e-15);
            assert!((center.y - oracle_center.0.y).abs() < 1e-15);
            assert!((center.z - oracle_center.0.z).abs() < 1e-15);
            for (vertex, oracle_vertex) in
                cell.vertices_point().iter().zip(oracle.vertices().iter())
            {
                assert!((vertex.x - oracle_vertex.0.x).abs() < 1e-15);
                assert!((vertex.y - oracle_vertex.0.y).abs() < 1e-15);
                assert!((vertex.z - oracle_vertex.0.z).abs() < 1e-15);
            }
            // Exact area matches the oracle's exact_area. Leaf-scale cells
            // (~1e-18 sr) inherently cancel ~9 digits in (s - angle) in
            // either implementation, so the differential tolerance is
            // relative 1e-6 — far below any practical meaning (7e-11 m²
            // at leaf scale); the invariant tests pin the math itself.
            let area = cell.area_steradians();
            let oracle_area = oracle.exact_area();
            assert!(
                (area - oracle_area).abs() <= 1e-6 * oracle_area.max(1e-30),
                "area {area} vs oracle {oracle_area} at level {}",
                id.level()
            );
        }
    }

    /// Area invariants: each face is 4π/6, the six faces sum to 4π, and
    /// children sum to their parent.
    #[test]
    fn area_invariants() {
        let full: f64 = (0..6)
            .map(|face| Cell::from_id(CellId::from_face(face)).area_steradians())
            .sum();
        assert!((full - 4.0 * std::f64::consts::PI).abs() < 1e-9);
        for face in 0..6 {
            let area = Cell::from_id(CellId::from_face(face)).area_steradians();
            assert!((area - 4.0 * std::f64::consts::PI / 6.0).abs() < 1e-10);
        }
        for id in sample_cells() {
            let Some(children) = id.children() else {
                continue;
            };
            let parent_area = Cell::from_id(id).area_steradians();
            let child_sum: f64 = children
                .iter()
                .map(|&child| Cell::from_id(child).area_steradians())
                .sum();
            assert!(
                (parent_area - child_sum).abs() <= 1e-9 * parent_area.max(1e-30),
                "children do not sum to parent at level {}",
                id.level()
            );
        }
    }

    /// `rect_bound` matches the reference crate's canonical bound within
    /// float noise — including polar faces, seam-crossing cells, and
    /// level 0 — and contains every sampled cell point.
    #[test]
    fn rect_bound_matches_oracle() {
        for id in sample_cells() {
            let cell = Cell::from_id(id);
            let rect = cell.rect_bound(&cell.vertices_lonlat());
            let oracle = ::s2::cell::Cell::from(::s2::cellid::CellID(id.raw())).rect_bound();
            assert!(
                (rect.lat_lo - oracle.lat.lo.to_degrees()).abs() < 1e-9,
                "{id:?} lat_lo"
            );
            assert!(
                (rect.lat_hi - oracle.lat.hi.to_degrees()).abs() < 1e-9,
                "{id:?} lat_hi"
            );
            // The oracle's S1 intervals normalize +180 to -180; compare
            // longitudes modulo 360.
            let lng_near = |ours: f64, oracle: f64| {
                let diff = (ours - oracle).rem_euclid(360.0);
                diff < 1e-9 || diff > 360.0 - 1e-9
            };
            if rect.is_full_lng() {
                assert!(oracle.lng.is_full(), "{id:?} expected full lng");
            } else {
                assert!(
                    lng_near(rect.lng_lo, oracle.lng.lo.to_degrees()),
                    "{id:?} lng_lo"
                );
                assert!(
                    lng_near(rect.lng_hi, oracle.lng.hi.to_degrees()),
                    "{id:?} lng_hi"
                );
            }
            // Containment: vertices and center lie inside the bound.
            let inside = |point: crate::geometry::Point| {
                let lat_ok = (rect.lat_lo..=rect.lat_hi).contains(&point.y);
                let lng_ok = if rect.is_full_lng() {
                    true
                } else if rect.crosses_seam() {
                    point.x >= rect.lng_lo || point.x <= rect.lng_hi
                } else {
                    (rect.lng_lo..=rect.lng_hi).contains(&point.x)
                };
                lat_ok && lng_ok
            };
            assert!(inside(cell.center_lonlat()), "{id:?} center outside bound");
            for vertex in cell.vertices_lonlat() {
                assert!(inside(vertex), "{id:?} vertex {vertex:?} outside bound");
            }
        }
    }

    /// The boundary shape is a closed four-corner lon/lat ring.
    #[test]
    fn boundary_is_closed_quad() {
        let cell = Cell::from_id(CellId::from_lonlat(13.4, 52.5).parent(12).expect("coarser"));
        let Shape::Polygon(polygon) = cell.boundary_shape() else {
            panic!("expected a polygon");
        };
        assert_eq!(polygon.shell.len(), 5);
        assert_eq!(polygon.shell.point_at(0), polygon.shell.point_at(4));
    }
}
