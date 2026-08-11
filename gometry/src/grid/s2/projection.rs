//! The S2 cube-face projection: unit-sphere XYZ ↔ face/UV ↔ ST ↔ IJ.
//!
//! Canonical Google S2 quadratic projection — cell ids and tokens depend on
//! these exact transforms, so they match the C++/Go/s2sphere references
//! bit-for-bit at the integer (IJ) layer.

use crate::grid::s2::cellid::Face;

/// Number of cube faces.
pub(crate) const NUM_FACES: u8 = 6;
/// Finest subdivision level.
pub(crate) const MAX_LEVEL: u8 = 30;
/// Leaf-grid size per face axis: `2^MAX_LEVEL` IJ positions.
pub(crate) const MAX_SIZE: u32 = 1 << MAX_LEVEL;

/// A point on (or near) the unit sphere in geocentric coordinates.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Point3 {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Point3 {
    pub(crate) const fn new(x: f64, y: f64, z: f64) -> Self {
        Self { x, y, z }
    }

    pub(crate) fn normalized(self) -> Self {
        let norm = (self.x * self.x + self.y * self.y + self.z * self.z).sqrt();
        Self::new(self.x / norm, self.y / norm, self.z / norm)
    }

    pub(crate) fn dot(self, other: Self) -> f64 {
        self.x * other.x + self.y * other.y + self.z * other.z
    }

    pub(crate) fn cross(self, other: Self) -> Self {
        Self::new(
            self.y * other.z - self.z * other.y,
            self.z * other.x - self.x * other.z,
            self.x * other.y - self.y * other.x,
        )
    }

    /// The angle to `other` in radians (numerically stable for small and
    /// near-π angles, unlike plain `acos` of the dot product).
    #[expect(
        dead_code,
        reason = "spherical angle helper retained for S2 identity; area path uses factored solid-angle formula"
    )]
    pub(crate) fn angle(self, other: Self) -> f64 {
        let cross = self.cross(other);
        (cross.x * cross.x + cross.y * cross.y + cross.z * cross.z)
            .sqrt()
            .atan2(self.dot(other))
    }
}

/// Geocentric XYZ of a lon/lat in degrees (unit sphere).
pub(crate) fn lonlat_to_point(lon: f64, lat: f64) -> Point3 {
    let (sin_lon, cos_lon) = lon.to_radians().sin_cos();
    let (sin_lat, cos_lat) = lat.to_radians().sin_cos();
    Point3::new(cos_lat * cos_lon, cos_lat * sin_lon, sin_lat)
}

/// The closed-cell owner representation of an admitted lon/lat point.
///
/// This is deliberately narrower than [`lonlat_to_point`]: ordinary S2 ID
/// construction retains the upstream trig rounding, while a coverage point
/// owner must enumerate *every* cell meeting a physical cube seam or pole.
/// Exact face centres and accepted normalized poles therefore need literal
/// zero/one components; `sin(90°)`'s residual is enough to select one face
/// and silently omit its closed-set neighbours.
pub(crate) fn lonlat_to_closed_owner_point(lon: f64, lat: f64) -> Point3 {
    if lat.to_bits() == 90.0_f64.to_bits() {
        return Point3::new(0.0, 0.0, 1.0);
    }
    if lat.to_bits() == (-90.0_f64).to_bits() {
        return Point3::new(0.0, 0.0, -1.0);
    }
    let (sin_lat, cos_lat) = lat.to_radians().sin_cos();
    let (sin_lon, cos_lon) = match lon.to_bits() {
        0 | 0x8000_0000_0000_0000 => (0.0, 1.0),
        bits if bits == 90.0_f64.to_bits() => (1.0, 0.0),
        bits if bits == (-90.0_f64).to_bits() => (-1.0, 0.0),
        bits if bits == 180.0_f64.to_bits() || bits == (-180.0_f64).to_bits() => (0.0, -1.0),
        _ => lon.to_radians().sin_cos(),
    };
    Point3::new(cos_lat * cos_lon, cos_lat * sin_lon, sin_lat)
}

/// Lon/lat in degrees of a (not necessarily unit) point.
pub(crate) fn point_to_lonlat(point: Point3) -> (f64, f64) {
    let lon = point.y.atan2(point.x).to_degrees();
    // Plain ops: scalar `hypot` is a libm call below x86-64-v3, and these
    // components cannot overflow (unit-sphere magnitudes).
    let lat = point
        .z
        .atan2((point.x * point.x + point.y * point.y).sqrt())
        .to_degrees();
    (lon, lat)
}

/// The cube face containing `point` (ties resolve like canonical S2: the
/// strictly larger magnitude wins, earlier axis on ties; +axis faces 0-2,
/// -axis faces 3-5).
pub(crate) fn face(point: Point3) -> Face {
    let (ax, ay, az) = (point.x.abs(), point.y.abs(), point.z.abs());
    let (id, value) = if ay > ax {
        if az > ay {
            (Face::F2, point.z)
        } else {
            (Face::F1, point.y)
        }
    } else if az > ax {
        (Face::F2, point.z)
    } else {
        (Face::F0, point.x)
    };
    match (id, value < 0.0) {
        (Face::F0, false) => Face::F0,
        (Face::F1, false) => Face::F1,
        (Face::F2, false) => Face::F2,
        (Face::F0, true) => Face::F3,
        (Face::F1, true) => Face::F4,
        (Face::F2, true) => Face::F5,
        (Face::F3 | Face::F4 | Face::F5, _) => unreachable!("base axis is positive"),
    }
}

/// (u, v) of `point` on `face`; the point must genuinely lie on that face's
/// half-space (`face(point) == face`).
pub(crate) fn valid_face_xyz_to_uv(face: impl Into<Face>, p: Point3) -> (f64, f64) {
    match face.into() {
        Face::F0 => (p.y / p.x, p.z / p.x),
        Face::F1 => (-p.x / p.y, p.z / p.y),
        Face::F2 => (-p.x / p.z, -p.y / p.z),
        Face::F3 => (p.z / p.x, p.y / p.x),
        Face::F4 => (p.z / p.y, -p.x / p.y),
        Face::F5 => (-p.y / p.z, -p.x / p.z),
    }
}

/// (u, v) of `point` on `face` when the point lies in that face's open
/// half-space; `None` otherwise (mirrors the canonical `face_xyz_to_uv`).
pub(crate) fn valid_face_uv(face: impl Into<Face>, p: Point3) -> Option<(f64, f64)> {
    let face = face.into();
    let valid = match face {
        Face::F0 => p.x > 0.0,
        Face::F1 => p.y > 0.0,
        Face::F2 => p.z > 0.0,
        Face::F3 => p.x < 0.0,
        Face::F4 => p.y < 0.0,
        Face::F5 => p.z < 0.0,
    };
    valid.then(|| valid_face_xyz_to_uv(face, p))
}

/// The face and (u, v) coordinates of `point`.
pub(crate) fn xyz_to_face_uv(point: Point3) -> (Face, f64, f64) {
    let f = face(point);
    let (u, v) = valid_face_xyz_to_uv(f, point);
    (f, u, v)
}

/// Geocentric XYZ of face/(u, v) coordinates (not normalized).
pub(crate) fn face_uv_to_xyz(face: impl Into<Face>, u: f64, v: f64) -> Point3 {
    match face.into() {
        Face::F0 => Point3::new(1.0, u, v),
        Face::F1 => Point3::new(-u, 1.0, v),
        Face::F2 => Point3::new(-u, -v, 1.0),
        Face::F3 => Point3::new(-1.0, -v, -u),
        Face::F4 => Point3::new(v, -1.0, -u),
        Face::F5 => Point3::new(v, u, -1.0),
    }
}

/// Quadratic ST → UV (canonical S2 projection; preserves cell-id compat).
pub(crate) fn st_to_uv(s: f64) -> f64 {
    if s >= 0.5 {
        (1.0 / 3.0) * (4.0 * s * s - 1.0)
    } else {
        (1.0 / 3.0) * (1.0 - 4.0 * (1.0 - s) * (1.0 - s))
    }
}

/// Quadratic UV → ST (inverse of [`st_to_uv`]).
pub(crate) fn uv_to_st(u: f64) -> f64 {
    if u >= 0.0 {
        0.5 * (1.0 + 3.0 * u).sqrt()
    } else {
        1.0 - 0.5 * (1.0 - 3.0 * u).sqrt()
    }
}

/// Discretize an ST value to the leaf-grid IJ coordinate.
pub(crate) fn st_to_ij(s: f64) -> i32 {
    (f64::from(MAX_SIZE) * s)
        .floor()
        .clamp(0.0, f64::from(MAX_SIZE - 1)) as i32
}

/// Minimum ST value of leaf-grid coordinate `i` (valid up to one position
/// beyond the grid: `0..=2^30`).
pub(crate) fn ij_to_st_min(i: i32) -> f64 {
    f64::from(i) / f64::from(MAX_SIZE)
}

/// ST value of an si/ti coordinate (the half-leaf grid: `0..=2^31`).
pub(crate) fn siti_to_st(si: u32) -> f64 {
    f64::from(si) / (2.0 * f64::from(MAX_SIZE))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Internal consistency over a global sweep including cube corners and
    /// face-edge midpoints (the tie-break hot spots). Bit-exactness against
    /// the reference implementation is proven transitively by the leaf-id
    /// differential tests in `cellid` — any projection deviation changes
    /// leaf ids (the oracle crate's own projection module is private).
    #[test]
    fn projection_is_self_consistent() {
        let mut samples = Vec::new();
        for lon_step in -36..=36 {
            for lat_step in -18..=18 {
                samples.push((f64::from(lon_step) * 5.0, f64::from(lat_step) * 5.0));
            }
        }
        samples.extend([
            (45.0, 35.264_389_682_754_654),
            (-45.0, 35.264_389_682_754_654),
            (135.0, -35.264_389_682_754_654),
            (45.0, 0.0),
            (-135.0, 0.0),
            (0.0, 90.0),
            (0.0, -90.0),
            (180.0, 0.0),
            (-180.0, 0.0),
        ]);
        for (lon, lat) in samples {
            let point = lonlat_to_point(lon, lat);
            let (face, u, v) = xyz_to_face_uv(point);
            assert!(face.get() < NUM_FACES);
            assert!((-1.0..=1.0).contains(&u) && (-1.0..=1.0).contains(&v));
            // ST/UV round trip and the discretized leaf coordinates.
            let (s_value, t_value) = (uv_to_st(u), uv_to_st(v));
            assert!((st_to_uv(s_value) - u).abs() < 1e-15);
            assert!((st_to_uv(t_value) - v).abs() < 1e-15);
            assert!((0..i64::from(MAX_SIZE)).contains(&i64::from(st_to_ij(s_value))));
            assert!((0..i64::from(MAX_SIZE)).contains(&i64::from(st_to_ij(t_value))));
            // XYZ reconstruction stays on the same face.
            let rebuilt = face_uv_to_xyz(face, u, v).normalized();
            assert!((rebuilt.x - point.x).abs() < 1e-14);
            assert!((rebuilt.y - point.y).abs() < 1e-14);
            assert!((rebuilt.z - point.z).abs() < 1e-14);
        }
    }

    /// Lon/lat → XYZ → lon/lat round-trips within float tolerance.
    #[test]
    fn lonlat_round_trip() {
        for &(lon, lat) in &[
            (0.0, 0.0),
            (179.999_999, 89.999_999),
            (-179.999_999, -89.999_999),
            (13.4, 52.5),
            (-122.42, 37.77),
        ] {
            let (rlon, rlat) = point_to_lonlat(lonlat_to_point(lon, lat));
            assert!((rlon - lon).abs() < 1e-9, "{lon} -> {rlon}");
            assert!((rlat - lat).abs() < 1e-9, "{lat} -> {rlat}");
        }
    }
}
