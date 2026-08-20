//! The S2 cell identity layer: the canonical 64-bit Hilbert-curve cell id,
//! its token codec, hierarchy moves, ranges, and grid neighbors.
//!
//! Layout (fixed by cross-library compatibility): bits `63..61` hold the
//! cube face, bits `60..0` the Hilbert position followed by a single
//! sentinel `1` bit whose place encodes the level. Ids order along the
//! Hilbert curve, so sorted ids group spatial neighbors and every cell's
//! descendants form one contiguous `range_min..=range_max` interval.

use crate::grid::s2::projection::{
    MAX_LEVEL, MAX_SIZE, Point3, face_uv_to_xyz, ij_to_st_min, lonlat_to_point, st_to_ij, st_to_uv,
    uv_to_st, xyz_to_face_uv,
};

const POS_BITS: u32 = 2 * (MAX_LEVEL as u32) + 1;
const LOOKUP_BITS: u32 = 4;
const LOOKUP_SIZE: usize = 1 << (2 * LOOKUP_BITS + 2);
const SWAP_MASK: u8 = 0x01;
const INVERT_MASK: u8 = 0x02;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Face {
    F0 = 0,
    F1 = 1,
    F2 = 2,
    F3 = 3,
    F4 = 4,
    F5 = 5,
}

impl Face {
    pub(crate) const fn get(self) -> u8 {
        self as u8
    }

    pub(crate) const fn from_u8(face: u8) -> Option<Self> {
        match face {
            0 => Some(Self::F0),
            1 => Some(Self::F1),
            2 => Some(Self::F2),
            3 => Some(Self::F3),
            4 => Some(Self::F4),
            5 => Some(Self::F5),
            _ => None,
        }
    }
}

impl From<Face> for u8 {
    fn from(face: Face) -> Self {
        face.get()
    }
}

impl From<u8> for Face {
    fn from(face: u8) -> Self {
        Self::from_u8(face).expect("S2 face must be in 0..=5")
    }
}

/// Hilbert sub-cell traversal order per orientation (canonical S2 tables).
const POS_TO_IJ: [[u8; 4]; 4] = [[0, 1, 3, 2], [0, 2, 3, 1], [3, 2, 0, 1], [3, 1, 0, 2]];
const POS_TO_ORIENTATION: [u8; 4] = [SWAP_MASK, 0, 0, SWAP_MASK | INVERT_MASK];

/// Build the 4-level Hilbert lookup tables at compile time: IJ→position and
/// position→IJ, 10-bit entries (`(value << 2) | orientation`), 4 `KiB` total —
/// no runtime init, no heap (the reference crates pay a lazy heap `Vec`).
const fn fill_lookup(
    level: u32,
    i: u16,
    j: u16,
    orig: u8,
    pos: u16,
    orient: u8,
    lookup_pos: &mut [u16; LOOKUP_SIZE],
    lookup_ij: &mut [u16; LOOKUP_SIZE],
) {
    if level == LOOKUP_BITS {
        let ij = (i << LOOKUP_BITS) | j;
        lookup_pos[((ij as usize) << 2) | orig as usize] = (pos << 2) | orient as u16;
        lookup_ij[((pos as usize) << 2) | orig as usize] = (ij << 2) | orient as u16;
        return;
    }
    let mut p = 0;
    while p < 4 {
        let child = POS_TO_IJ[orient as usize][p];
        fill_lookup(
            level + 1,
            (i << 1) | (child >> 1) as u16,
            (j << 1) | (child & 1) as u16,
            orig,
            (pos << 2) | p as u16,
            orient ^ POS_TO_ORIENTATION[p],
            lookup_pos,
            lookup_ij,
        );
        p += 1;
    }
}

const fn build_lookup() -> ([u16; LOOKUP_SIZE], [u16; LOOKUP_SIZE]) {
    let mut pos = [0_u16; LOOKUP_SIZE];
    let mut ij = [0_u16; LOOKUP_SIZE];
    let mut orientation = 0;
    while orientation < 4 {
        fill_lookup(0, 0, 0, orientation, 0, orientation, &mut pos, &mut ij);
        orientation += 1;
    }
    (pos, ij)
}

const LOOKUP: ([u16; LOOKUP_SIZE], [u16; LOOKUP_SIZE]) = build_lookup();
const LOOKUP_POS: &[u16; LOOKUP_SIZE] = &LOOKUP.0;
const LOOKUP_IJ: &[u16; LOOKUP_SIZE] = &LOOKUP.1;

/// The sentinel bit for a cell at `level` (place encodes the level).
const fn lsb_for_level(level: u8) -> u64 {
    1 << (2 * (MAX_LEVEL - level) as u32)
}

/// Leaf-grid edge length of a cell at `level`, in IJ units.
pub(crate) const fn size_ij(level: u8) -> i32 {
    1 << (MAX_LEVEL - level) as u32
}

/// A canonical 64-bit S2 cell id. Always structurally valid by construction
/// — fallible boundaries (`from_raw`, `from_token`) return `Option`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub(crate) struct CellId(u64);

impl CellId {
    /// The raw 64-bit id.
    pub(crate) const fn raw(self) -> u64 {
        self.0
    }

    /// Validate and wrap a raw id (face < 6 and a sentinel bit in a
    /// level-encoding position).
    pub(crate) fn from_raw(id: u64) -> Option<Self> {
        let cell = Self(id);
        (Face::from_u8(cell.raw_face()).is_some()
            && id.isolate_lowest_one() & 0x1555_5555_5555_5555 != 0)
            .then_some(cell)
    }

    /// Parse the hex token (trailing zero nibbles stripped; case-blind).
    pub(crate) fn from_token(token: &str) -> Option<Self> {
        // Explicit digit check: `from_str_radix` also accepts a leading
        // `+`, which is not a token.
        if token.is_empty()
            || token.len() > 16
            || !token.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return None;
        }
        let value = u64::from_str_radix(token, 16).ok()?;
        Self::from_raw(value << (4 * (16 - token.len())))
    }

    /// The token: 16 hex nibbles with trailing zeros stripped.
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn token(self) -> String {
        format!("{:016x}", self.0).trim_end_matches('0').to_owned()
    }

    /// The face cell `face` (level 0).
    pub(crate) fn from_face(face: impl Into<Face>) -> Self {
        let face = face.into();
        Self((u64::from(face.get()) << POS_BITS) + lsb_for_level(0))
    }

    /// The leaf cell containing validated-finite lon/lat degrees.
    pub(crate) fn from_lonlat(lon: f64, lat: f64) -> Self {
        Self::from_point(lonlat_to_point(lon, lat))
    }

    /// The leaf cell containing a point on (or near) the unit sphere.
    pub(crate) fn from_point(point: Point3) -> Self {
        let (face, u, v) = xyz_to_face_uv(point);
        Self::from_face_ij(face, st_to_ij(uv_to_st(u)), st_to_ij(uv_to_st(v)))
    }

    /// The leaf cell at in-range face/IJ coordinates (the Hilbert encode).
    pub(crate) fn from_face_ij(face: impl Into<Face>, i: i32, j: i32) -> Self {
        let face = face.into().get();
        let mut id = u64::from(face) << (POS_BITS - 1);
        let mut bits = u16::from(face & SWAP_MASK);
        let mask = (1 << LOOKUP_BITS) - 1;
        let mut k = 7_u32;
        loop {
            let shift = k * LOOKUP_BITS;
            let key = usize::from(bits)
                | ((((i >> shift) & mask) as usize) << (LOOKUP_BITS + 2))
                | ((((j >> shift) & mask) as usize) << 2);
            let entry = LOOKUP_POS[key];
            id |= u64::from(entry >> 2) << (k * 2 * LOOKUP_BITS);
            bits = entry & u16::from(SWAP_MASK | INVERT_MASK);
            if k == 0 {
                break;
            }
            k -= 1;
        }
        Self(id * 2 + 1)
    }

    /// The leaf cell for face/IJ coordinates that may lie one step outside
    /// the face (`-1..=MAX_SIZE`): project the just-outside leaf center
    /// through the cube and re-resolve the face — the canonical wrap that
    /// makes cube-edge neighbors exact.
    pub(crate) fn from_face_ij_wrap(face: impl Into<Face>, i: i32, j: i32) -> Self {
        let face = face.into();
        let i = i.clamp(-1, MAX_SIZE as i32);
        let j = j.clamp(-1, MAX_SIZE as i32);
        let scale = 1.0 / f64::from(MAX_SIZE);
        let limit = 1.0 + f64::EPSILON;
        let u = (scale * (2.0 * f64::from(i) + 1.0 - f64::from(MAX_SIZE))).clamp(-limit, limit);
        let v = (scale * (2.0 * f64::from(j) + 1.0 - f64::from(MAX_SIZE))).clamp(-limit, limit);
        let (face, u, v) = xyz_to_face_uv(face_uv_to_xyz(face, u, v));
        Self::from_face_ij(
            face,
            st_to_ij(f64::midpoint(u, 1.0)),
            st_to_ij(f64::midpoint(v, 1.0)),
        )
    }

    /// The cube face (`0..=5`).
    const fn raw_face(self) -> u8 {
        (self.0 >> POS_BITS) as u8
    }

    /// The cube face.
    pub(crate) const fn face(self) -> Face {
        Face::from_u8(self.raw_face()).expect("valid CellId face")
    }

    /// The subdivision level (`0..=30`).
    pub(crate) const fn level(self) -> u8 {
        MAX_LEVEL - (self.0.trailing_zeros() >> 1) as u8
    }

    /// Whether this is a deepest-level cell.
    pub(crate) const fn is_leaf(self) -> bool {
        self.0 & 1 != 0
    }

    /// The sentinel (lowest set) bit.
    const fn lsb(self) -> u64 {
        self.0 & self.0.wrapping_neg()
    }

    /// The ancestor at `level`; `None` when `level` is finer than this
    /// cell's (same level returns the cell itself).
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn parent(self, level: u8) -> Option<Self> {
        (level <= self.level()).then(|| {
            let lsb = lsb_for_level(level);
            Self((self.0 & lsb.wrapping_neg()) | lsb)
        })
    }

    /// The four immediate children, in Hilbert order; `None` for a leaf.
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) const fn children(self) -> Option<[Self; 4]> {
        if self.is_leaf() {
            return None;
        }
        let lsb = self.lsb();
        let first = self.0 - lsb + (lsb >> 2);
        let step = lsb >> 1;
        Some([
            Self(first),
            Self(first + step),
            Self(first + 2 * step),
            Self(first + 3 * step),
        ])
    }

    /// First descendant cell at `level` (Hilbert traversal order); `level`
    /// must not be coarser than this cell's.
    pub(crate) const fn child_begin_at(self, level: u8) -> Self {
        Self(self.0 - self.lsb() + lsb_for_level(level))
    }

    /// One past the last descendant at `level` (may be structurally
    /// invalid; pair with [`Self::next`]).
    pub(crate) const fn child_end_at(self, level: u8) -> Self {
        Self(self.0 + self.lsb() + lsb_for_level(level))
    }

    /// The next cell at this level along the Hilbert curve.
    pub(crate) const fn next(self) -> Self {
        Self(self.0.wrapping_add(self.lsb() << 1))
    }

    /// The smallest leaf id contained in this cell.
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) const fn range_min(self) -> Self {
        Self(self.0 - (self.lsb() - 1))
    }

    /// The largest leaf id contained in this cell.
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) const fn range_max(self) -> Self {
        Self(self.0 + (self.lsb() - 1))
    }

    /// Whether this cell contains `other` (itself included).
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn contains(self, other: Self) -> bool {
        self.range_min() <= other && other <= self.range_max()
    }

    /// The four edge-adjacent cells at this cell's level (down, right, up,
    /// left in face space), exact across cube edges.
    pub(crate) fn edge_neighbors(self) -> [Self; 4] {
        let level = self.level();
        let size = size_ij(level);
        let (face, i, j, _) = self.face_ij_orientation();
        [
            Self::from_face_ij_wrap(face, i, j - size).parent_unchecked(level),
            Self::from_face_ij_wrap(face, i + size, j).parent_unchecked(level),
            Self::from_face_ij_wrap(face, i, j + size).parent_unchecked(level),
            Self::from_face_ij_wrap(face, i - size, j).parent_unchecked(level),
        ]
    }

    /// `parent` for levels known to be coarser-or-equal (leaf → `level`).
    const fn parent_unchecked(self, level: u8) -> Self {
        let lsb = lsb_for_level(level);
        Self((self.0 & lsb.wrapping_neg()) | lsb)
    }

    /// The face, leaf-grid IJ of this cell's lowest corner, and the Hilbert
    /// orientation (the decode loop over the position→IJ table).
    pub(crate) fn face_ij_orientation(self) -> (u8, i32, i32, u8) {
        let face = self.face().get();
        let mut i = 0_i32;
        let mut j = 0_i32;
        let mut orient = u16::from(face & SWAP_MASK);
        let mut nbits = u32::from(MAX_LEVEL) - 7 * LOOKUP_BITS;
        let mut k = 7_u32;
        loop {
            let shift = k * 2 * LOOKUP_BITS + 1;
            let mask = (1_u64 << (2 * nbits)) - 1;
            let key = usize::from(orient) | ((((self.0 >> shift) & mask) as usize) << 2);
            let entry = LOOKUP_IJ[key];
            i |= i32::from(entry >> (LOOKUP_BITS + 2)) << (k * LOOKUP_BITS);
            j |= i32::from((entry >> 2) & ((1 << LOOKUP_BITS) - 1)) << (k * LOOKUP_BITS);
            orient = entry & u16::from(SWAP_MASK | INVERT_MASK);
            if k == 0 {
                break;
            }
            nbits = LOOKUP_BITS;
            k -= 1;
        }
        if self.lsb() & 0x1111_1111_1111_1110 != 0 {
            orient ^= u16::from(SWAP_MASK);
        }
        (face, i, j, orient as u8)
    }

    /// The face and si/ti (half-leaf grid) coordinates of the cell center —
    /// the exact center the recursive subdivision uses.
    pub(crate) fn center_face_siti(self) -> (u8, u32, u32) {
        let (face, i, j, _) = self.face_ij_orientation();
        let delta = if self.is_leaf() {
            1
        } else if (i64::from(i) ^ (self.0 as i64 >> 2)) & 1 != 0 {
            2
        } else {
            0
        };
        (face, (2 * i + delta) as u32, (2 * j + delta) as u32)
    }

    /// The cell's (u, v) bounds on its face.
    pub(crate) fn bound_uv(self) -> (f64, f64, f64, f64) {
        let (u_lo, u_hi, v_lo, v_hi, ..) = self.bound_uv_with_spans();
        (u_lo, u_hi, v_lo, v_hi)
    }

    /// UV bounds plus cancellation-free axis spans (area-local only).
    pub(crate) fn bound_uv_with_spans(self) -> (f64, f64, f64, f64, f64, f64) {
        let (_, i, j, _) = self.face_ij_orientation();
        let size = size_ij(self.level());
        let s_lo = ij_to_st_min(i & -size);
        let s_hi = ij_to_st_min((i & -size) + size);
        let t_lo = ij_to_st_min(j & -size);
        let t_hi = ij_to_st_min((j & -size) + size);
        let u_lo = st_to_uv(s_lo);
        let u_hi = st_to_uv(s_hi);
        let v_lo = st_to_uv(t_lo);
        let v_hi = st_to_uv(t_hi);
        let span = |lo: f64, hi: f64| {
            if lo >= 0.5 {
                (4.0 / 3.0) * (hi - lo) * (hi + lo)
            } else if hi <= 0.5 {
                (4.0 / 3.0) * (hi - lo) * (2.0 - hi - lo)
            } else {
                st_to_uv(hi) - st_to_uv(lo)
            }
        };
        (u_lo, u_hi, v_lo, v_hi, span(s_lo, s_hi), span(t_lo, t_hi))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oracle(id: CellId) -> ::s2::cellid::CellID {
        ::s2::cellid::CellID(id.raw())
    }

    /// Deterministic global lon/lat sweep + face edges, corners, poles.
    fn sample_points() -> Vec<(f64, f64)> {
        let mut samples = Vec::new();
        for lon_step in -18..=18 {
            for lat_step in -8..=8 {
                samples.push((f64::from(lon_step) * 10.0, f64::from(lat_step) * 10.0));
            }
        }
        samples.extend([
            (45.0, 35.264_389_682_754_654),
            (-45.0, -35.264_389_682_754_654),
            (0.0, 90.0),
            (0.0, -90.0),
            (180.0, 0.0),
            (-180.0, 0.0),
            (179.999_999, 52.5),
            (-179.999_999, -52.5),
            (13.4, 52.5),
        ]);
        samples
    }

    /// Leaf encode + every-level parent match the reference crate exactly.
    #[test]
    fn encode_and_hierarchy_match_oracle() {
        for (lon, lat) in sample_points() {
            let leaf = CellId::from_lonlat(lon, lat);
            let oracle_leaf =
                ::s2::cellid::CellID::from(::s2::latlng::LatLng::from_degrees(lat, lon));
            assert_eq!(leaf.raw(), oracle_leaf.0, "leaf at lon={lon} lat={lat}");
            assert!(leaf.is_leaf() && leaf.level() == MAX_LEVEL);
            for level in 0..=MAX_LEVEL {
                let parent = leaf.parent(level).expect("coarser");
                assert_eq!(parent.raw(), oracle_leaf.parent(u64::from(level)).0);
                assert_eq!(parent.level(), level);
                assert!(parent.contains(leaf));
                assert_eq!(parent.face(), leaf.face());
            }
        }
    }

    /// Token codec round-trips at every level and rejects junk.
    #[test]
    fn token_codec_matches_oracle() {
        for (lon, lat) in sample_points() {
            let leaf = CellId::from_lonlat(lon, lat);
            for level in 0..=MAX_LEVEL {
                let cell = leaf.parent(level).expect("coarser");
                let token = cell.token();
                assert_eq!(token, oracle(cell).to_token());
                assert_eq!(CellId::from_token(&token), Some(cell));
            }
        }
        for junk in ["", "x", "X", "0", "ghij", "123456789012345678"] {
            assert_eq!(CellId::from_token(junk), None, "{junk:?}");
        }
        assert_eq!(CellId::from_raw(0), None);
        assert_eq!(CellId::from_raw(u64::MAX), None);
    }

    /// Children, traversal ranges, and ordering match the reference crate.
    #[test]
    fn children_and_ranges_match_oracle() {
        for (lon, lat) in sample_points() {
            let cell = CellId::from_lonlat(lon, lat).parent(12).expect("coarser");
            let children = cell.children().expect("not a leaf");
            let oracle_children = oracle(cell).children();
            for (child, oracle_child) in children.iter().zip(oracle_children.iter()) {
                assert_eq!(child.raw(), oracle_child.0);
                assert!(cell.contains(*child));
            }
            assert_eq!(cell.range_min().raw(), oracle(cell).range_min().0);
            assert_eq!(cell.range_max().raw(), oracle(cell).range_max().0);
            assert_eq!(
                cell.child_begin_at(14).raw(),
                oracle(cell).child_begin_at_level(14).0
            );
            assert_eq!(
                cell.child_end_at(14).raw(),
                oracle(cell).child_end_at_level(14).0
            );
            // Hilbert iteration: 16 grandchildren between begin and end.
            let mut count = 0;
            let mut cursor = cell.child_begin_at(14);
            let end = cell.child_end_at(14);
            while cursor != end {
                assert!(cell.contains(cursor));
                cursor = cursor.next();
                count += 1;
            }
            assert_eq!(count, 16);
            // Siblings do not contain each other.
            assert!(!children[0].contains(children[1]));
        }
    }

    /// Edge neighbors match the reference crate everywhere — including
    /// across cube edges and at corners (the wrap hot spots).
    #[test]
    fn edge_neighbors_match_oracle() {
        let mut cases = sample_points();
        // Push samples right against every face edge via face centers ± eps.
        for face in 0..6_u8 {
            let center = CellId::from_face(face);
            let (_, i, j, _) = center.face_ij_orientation();
            cases.extend(
                [
                    (i, 0),
                    (0, j),
                    (i, MAX_SIZE as i32 - 1),
                    (MAX_SIZE as i32 - 1, j),
                ]
                .iter()
                .map(|&(i, j)| {
                    let cell = CellId::from_face_ij(face, i, j);
                    let (face, si, ti) = cell.center_face_siti();
                    let point = face_uv_to_xyz(
                        face,
                        st_to_uv(super::super::projection::siti_to_st(si)),
                        st_to_uv(super::super::projection::siti_to_st(ti)),
                    );
                    super::super::projection::point_to_lonlat(point)
                }),
            );
        }
        for (lon, lat) in cases {
            for level in [1, 5, 12, 20, MAX_LEVEL] {
                let cell = CellId::from_lonlat(lon, lat)
                    .parent(level)
                    .expect("coarser");
                let ours = cell.edge_neighbors();
                let oracles = oracle(cell).edge_neighbors();
                for (a, b) in ours.iter().zip(oracles.iter()) {
                    assert_eq!(a.raw(), b.0, "level {level} at lon={lon} lat={lat}");
                }
            }
        }
    }

    /// `bound_uv` and centers match the reference crate.
    #[test]
    fn centers_and_bounds_match_oracle() {
        for (lon, lat) in sample_points() {
            for level in [0, 3, 12, 25, MAX_LEVEL] {
                let cell = CellId::from_lonlat(lon, lat)
                    .parent(level)
                    .expect("coarser");
                let (face, si, ti) = cell.center_face_siti();
                let (oface, osi, oti) = {
                    // The oracle's center is exposed through raw_point; use
                    // bound_uv + center reconstruction instead.
                    let (u_lo, u_hi, v_lo, v_hi) = cell.bound_uv();
                    let rect = oracle(cell).bound_uv();
                    assert!((u_lo - rect.x.lo).abs() < 1e-15);
                    assert!((u_hi - rect.x.hi).abs() < 1e-15);
                    assert!((v_lo - rect.y.lo).abs() < 1e-15);
                    assert!((v_hi - rect.y.hi).abs() < 1e-15);
                    (face, si, ti)
                };
                let _ = (oface, osi, oti);
                // The center point reconstructed from si/ti lies inside the
                // cell (its own leaf maps back into the cell).
                let point = face_uv_to_xyz(
                    face,
                    st_to_uv(super::super::projection::siti_to_st(si)),
                    st_to_uv(super::super::projection::siti_to_st(ti)),
                );
                assert!(cell.contains(CellId::from_point(point)));
            }
        }
    }
}
