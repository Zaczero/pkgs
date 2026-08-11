#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! XYZ web-mercator tiles: the slippy-map `z/x/y` addressing scheme.
//!
//! A `Tile` is `(z, x, y)` with mercantile-compatible math; the packed
//! `u64` id stores the zoom in the top six bits and the Morton-interleaved
//! `x`/`y` below, so integer order equals `(zoom, locality)` order. Tokens
//! are quadkeys (Bing's digit-per-level path; the empty quadkey is the
//! `z0` world tile). Zoom caps at 29 — the deliberate pre-v1 limit the
//! packed id affords.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use std::f64::consts::PI;

use crate::boundary::geographic::WEB_MERCATOR_MAX_LATITUDE;
use crate::curves::{morton_deinterleave, morton_interleave};
use crate::geometry::{Bounds, Point, Shape};

pub(crate) const TILE_MAX_ZOOM: u8 = 29;

/// The Web Mercator latitude domain edge.
pub(crate) const TILE_MAX_LATITUDE: f64 = WEB_MERCATOR_MAX_LATITUDE;

/// Whether `lat` lies inside the Web Mercator / slippy-tile domain.
pub(crate) fn latitude_in_tile_domain(lat: f64) -> bool {
    (-TILE_MAX_LATITUDE..=TILE_MAX_LATITUDE).contains(&lat)
}

/// Reject a covering geometry whose any vertex latitude is outside
/// ±[`TILE_MAX_LATITUDE`]. Tile coverings cannot represent latitudes past the
/// Web Mercator edge; silently clipping (identical cell counts for ±84 vs
/// ±89.9) is the defect this gate closes. Matches the point/tile_cells path
/// that raises rather than clamps.
///
/// Returns the first out-of-domain latitude, or `Ok(())` when every vertex is
/// in domain (or the shape has no vertices).
pub(crate) fn ensure_shape_in_tile_domain(shape: &Shape) -> Result<(), f64> {
    let mut bad = None;
    shape.for_each_point(|point: Point| {
        if bad.is_none() && !latitude_in_tile_domain(point.y) {
            bad = Some(point.y);
        }
    });
    bad.map_or(Ok(()), Err)
}

/// One XYZ tile address; `0 <= x, y < 2^z`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Tile {
    pub z: u8,
    pub x: u32,
    pub y: u32,
}

crate::heapless!(Tile);

impl Tile {
    /// The packed id: zoom in bits 63..58, Morton-interleaved `x`/`y`
    /// (y in the higher bit of each pair, so the id sorts in quadkey
    /// order) left-aligned below.
    pub(crate) fn id(self) -> u64 {
        let morton = morton_interleave(self.x, self.y);
        (u64::from(self.z) << 58) | ((morton << (58 - 2 * u32::from(self.z))) & ((1 << 58) - 1))
    }

    /// Rebuild from a packed id; `None` when malformed (bad zoom or set
    /// bits below the tile's depth).
    pub(crate) fn from_id(id: u64) -> Option<Self> {
        let z = (id >> 58) as u8;
        if z > TILE_MAX_ZOOM {
            return None;
        }
        let payload = id & ((1 << 58) - 1);
        let shift = 58 - 2 * u32::from(z);
        if payload & ((1 << shift) - 1) != 0 {
            return None;
        }
        let morton = payload >> shift;
        let (x, y) = morton_deinterleave(morton);
        Some(Self { z, x, y })
    }

    /// The tile containing a lon/lat point inside the Web Mercator domain.
    ///
    /// Returns `None` when `lat` is outside ±[`TILE_MAX_LATITUDE`] (about
    /// ±85.05112878°). Out-of-domain latitudes are rejected rather than
    /// silently clamped — callers that need a typed error raise at the
    /// boundary; mercantile-style clamp is not offered.
    pub(crate) fn from_lonlat(lon: f64, lat: f64, zoom: u8) -> Option<Self> {
        if !latitude_in_tile_domain(lat) {
            return None;
        }
        let cells = 2_f64.powi(i32::from(zoom));
        let x = ((lon + 180.0) / 360.0 * cells).floor();
        let lat_rad = lat.to_radians();
        // Canonical Web Mercator forward form: one tangent instead of the
        // equivalent `tan(lat) + sec(lat)` pair used by the traditional
        // slippy-map spelling. This is the same one-trig identity used by the
        // in-core projection kernel.
        let mercator_y = (PI / 4.0 + lat_rad / 2.0).tan().ln();
        let normalized_y = (1.0 - mercator_y / PI) / 2.0 * cells;
        let fraction = normalized_y.fract().abs();
        let boundary_distance = fraction.min(1.0 - fraction);
        // The two algebraically equivalent spellings round differently only
        // within a few ulps of an integer tile boundary. Preserve the existing
        // public row assignment there with the historical expression; ordinary
        // coordinates stay on the one-trig fast path.
        let boundary_slack = 16.0 * f64::EPSILON * cells;
        let y = if boundary_distance <= boundary_slack {
            ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0 * cells).floor()
        } else {
            normalized_y.floor()
        };
        let last = cells - 1.0;
        Some(Self {
            z: zoom,
            x: x.clamp(0.0, last) as u32,
            y: y.clamp(0.0, last) as u32,
        })
    }

    /// The quadkey token (one base-4 digit per level; empty at `z0`).
    pub(crate) fn quadkey(self) -> String {
        (1..=u32::from(self.z))
            .rev()
            .map(|level| {
                let mask = 1_u32 << (level - 1);
                let digit = u8::from(self.x & mask != 0) + 2 * u8::from(self.y & mask != 0);
                char::from(b'0' + digit)
            })
            .collect()
    }

    /// Parse a quadkey token.
    pub(crate) fn from_quadkey(token: &str) -> Result<Self, String> {
        let bytes = token.as_bytes();
        if bytes.len() > TILE_MAX_ZOOM as usize {
            return Err(format!(
                "quadkeys have at most {TILE_MAX_ZOOM} digits, got {} in {token:?}",
                bytes.len()
            ));
        }
        let (mut x, mut y) = (0_u32, 0_u32);
        for &byte in bytes {
            let digit = byte.wrapping_sub(b'0');
            if digit > 3 {
                return Err(format!(
                    "invalid quadkey digit {:?} in {token:?}",
                    char::from(byte)
                ));
            }
            x = (x << 1) | u32::from(digit & 1);
            y = (y << 1) | u32::from(digit >> 1);
        }
        Ok(Self {
            z: bytes.len() as u8,
            x,
            y,
        })
    }

    /// Lon/lat bounds (mercantile `bounds`).
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn bounds(self) -> Bounds {
        let cells = 2_f64.powi(i32::from(self.z));
        let lon = |x: f64| x / cells * 360.0 - 180.0;
        let lat = |y: f64| {
            let n = PI * (1.0 - 2.0 * y / cells);
            n.sinh().atan().to_degrees()
        };
        Bounds::new_unchecked(
            lon(f64::from(self.x)),
            lat(f64::from(self.y) + 1.0),
            lon(f64::from(self.x) + 1.0),
            lat(f64::from(self.y)),
        )
    }

    /// The parent at `zoom` (must be coarser or equal).
    pub(crate) fn parent_at(self, zoom: u8) -> Self {
        let shift = u32::from(self.z - zoom);
        Self {
            z: zoom,
            x: self.x >> shift,
            y: self.y >> shift,
        }
    }

    /// The four children one zoom finer, in quadkey order.
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) const fn children(self) -> [Self; 4] {
        let (z, x, y) = (self.z + 1, self.x << 1, self.y << 1);
        [
            Self { z, x, y },
            Self { z, x: x + 1, y },
            Self { z, x, y: y + 1 },
            Self {
                z,
                x: x + 1,
                y: y + 1,
            },
        ]
    }

    /// The neighbor `dx` columns east and `dy` rows SOUTH (tile rows grow
    /// southward); wraps across the antimeridian, `None` past the edges.
    pub(crate) fn neighbor(self, dx: i64, dy: i64) -> Option<Self> {
        let cells = 1_i64 << u32::from(self.z);
        // `dx ∈ {-1,0,1}` and `|dx| ≤ cells`, so the branchless wrap reproduces
        // `rem_euclid` exactly without the `idiv` it compiles to (hot in `neighbors`).
        let x = super::wrap_axis(i64::from(self.x) + dx, cells);
        let y = i64::from(self.y) + dy;
        if y < 0 || y >= cells {
            return None;
        }
        Some(Self {
            z: self.z,
            x: x as u32,
            y: y as u32,
        })
    }

    /// The up-to-8 surrounding tiles, row-major from north-west (tile rows
    /// grow south, so rows walk `[-1, 0, 1]`). At low zoom the antimeridian
    /// wrap can make two offsets land on the same tile (or on self) — each
    /// distinct tile appears once.
    pub(crate) fn neighbors(self) -> Vec<Self> {
        super::ring_neighbors(self, [-1, 0, 1], Self::neighbor)
    }
}

/// The single z0 world tile — the coverer's seed.
pub(crate) const fn root() -> Tile {
    Tile { z: 0, x: 0, y: 0 }
}

pub(crate) use crate::grid::cell_set::{compact_with_floor, uncompact, uncompact_unlimited};

impl crate::grid::coverer::RectCell for Tile {
    fn depth(self) -> u8 {
        self.z
    }
    fn bounds(self) -> crate::geometry::Bounds {
        Self::bounds(self)
    }
    fn children(self) -> impl Iterator<Item = Self> {
        Self::children(self).into_iter()
    }
    fn push_children_bounds(self, parent: Bounds, out: &mut Vec<(Self, Bounds)>) {
        // Lon is linear in tile x; lat is mercator — one mid-row latitude
        // (parent y + 0.5) splits the four children, matching `bounds()` bit-
        // for-bit without four full sinh/atan recomputes.
        let mid_lon = f64::midpoint(parent.minx(), parent.maxx());
        // `1 << z` is exact for z ≤ 29 (TILE_MAX_ZOOM); same value as `2.0.powi(z)`.
        let cells = f64::from(1_u32 << u32::from(self.z));
        let n = PI * (1.0 - 2.0 * (f64::from(self.y) + 0.5) / cells);
        let mid_lat = n.sinh().atan().to_degrees();
        let kids = Self::children(self);
        out.push((
            kids[0],
            Bounds::new_unchecked(parent.minx(), mid_lat, mid_lon, parent.maxy()),
        ));
        out.push((
            kids[1],
            Bounds::new_unchecked(mid_lon, mid_lat, parent.maxx(), parent.maxy()),
        ));
        out.push((
            kids[2],
            Bounds::new_unchecked(parent.minx(), parent.miny(), mid_lon, mid_lat),
        ));
        out.push((
            kids[3],
            Bounds::new_unchecked(mid_lon, parent.miny(), parent.maxx(), mid_lat),
        ));
    }
    fn edge_neighbors(self) -> [Option<Self>; 4] {
        // Tile rows grow southward (y down), so south is y+1 and north is y-1;
        // east/west do NOT wrap the antimeridian (None at x = 0 / last column).
        let cells = 1_u32 << u32::from(self.z);
        let at = |x: u32, y: u32| Self { z: self.z, x, y };
        [
            (self.y + 1 < cells).then(|| at(self.x, self.y + 1)), // south (miny)
            (self.x + 1 < cells).then(|| at(self.x + 1, self.y)), // east (maxx)
            (self.y > 0).then(|| at(self.x, self.y - 1)),         // north (maxy)
            (self.x > 0).then(|| at(self.x - 1, self.y)),         // west (minx)
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_tile_y(lat: f64, zoom: u8) -> u32 {
        let cells = 2_f64.powi(i32::from(zoom));
        let lat_rad = lat
            .clamp(-TILE_MAX_LATITUDE, TILE_MAX_LATITUDE)
            .to_radians();
        let y = ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0 * cells).floor();
        y.clamp(0.0, cells - 1.0) as u32
    }

    #[test]
    fn push_children_bounds_match_child_bounds_bit_exact() {
        use crate::grid::coverer::RectCell as _;
        let parents = [
            Tile { z: 0, x: 0, y: 0 },
            Tile { z: 5, x: 10, y: 20 },
            Tile {
                z: 10,
                x: 486,
                y: 332,
            },
            Tile {
                z: 15,
                x: 10_000,
                y: 12_000,
            },
        ];
        for parent in parents {
            let mut derived = Vec::new();
            parent.push_children_bounds(parent.bounds(), &mut derived);
            let kids = parent.children();
            assert_eq!(derived.len(), 4);
            for (i, (child, bounds)) in derived.into_iter().enumerate() {
                assert_eq!(child, kids[i]);
                let expected = child.bounds();
                assert_eq!(
                    (
                        bounds.minx().to_bits(),
                        bounds.miny().to_bits(),
                        bounds.maxx().to_bits(),
                        bounds.maxy().to_bits()
                    ),
                    (
                        expected.minx().to_bits(),
                        expected.miny().to_bits(),
                        expected.maxx().to_bits(),
                        expected.maxy().to_bits()
                    ),
                    "child {i} of {parent:?}"
                );
            }
        }
    }

    #[test]
    fn matches_mercantile_vectors() {
        // mercantile.tile(-105.939, 35.687, 9) == Tile(105, 201, 9)
        let tile = Tile::from_lonlat(-105.939, 35.687, 9).expect("in domain");
        assert_eq!((tile.x, tile.y, tile.z), (105, 201, 9));
        // mercantile.quadkey(486, 332, 10) == '0313102310'
        let qk = Tile {
            z: 10,
            x: 486,
            y: 332,
        };
        assert_eq!(qk.quadkey(), "0313102310");
        assert_eq!(Tile::from_quadkey("0313102310").unwrap(), qk);
        assert_eq!(Tile::from_quadkey("").unwrap(), Tile { z: 0, x: 0, y: 0 });
        // mercantile.bounds(486, 332, 10)
        let bounds = qk.bounds();
        assert!((bounds.minx() - -9.140_625).abs() < 1e-9);
        assert!((bounds.maxx() - -8.789_062_5).abs() < 1e-9);
        assert!((bounds.miny() - 53.120_405_283_106_564).abs() < 1e-9);
        assert!((bounds.maxy() - 53.330_872_983_017_07).abs() < 1e-9);
    }

    #[test]
    fn compact_merges_sibling_quads_and_uncompact_inverts() {
        let parent = Tile { z: 5, x: 10, y: 20 };
        // All four children of `parent` compact back into it…
        let children = parent.children().to_vec();
        assert_eq!(compact_with_floor(children.clone(), 0), vec![parent]);
        assert_eq!(compact_with_floor(children.clone(), 5), vec![parent]);
        // …unless the floor (coarsest allowed) forbids producing zoom 5.
        assert_eq!(
            compact_with_floor(children.clone(), 6),
            uncompact(&children, 6).expect("within budget")
        );
        // Three of four siblings do NOT merge (incomplete quad).
        let partial: Vec<Tile> = children[..3].to_vec();
        assert_eq!(compact_with_floor(partial, 0).len(), 3);
        // uncompact expands back to a uniform zoom and round-trips a compacted set.
        let expanded = uncompact(&[parent], 7).expect("within budget");
        assert_eq!(expanded.len(), 16);
        assert_eq!(compact_with_floor(expanded, 0), vec![parent]);
        // A tile nested under an ancestor already present is absorbed —
        // including when the ancestor is COARSER than the merge floor.
        let mixed = vec![parent, Tile { z: 7, x: 41, y: 81 }];
        assert_eq!(compact_with_floor(mixed.clone(), 0), vec![parent]);
        assert_eq!(compact_with_floor(mixed, 6), vec![parent]);
    }

    #[test]
    fn ids_hierarchy_and_neighbors_are_consistent() {
        let tile = Tile {
            z: 10,
            x: 486,
            y: 332,
        };
        assert_eq!(Tile::from_id(tile.id()), Some(tile));
        // (z, morton) id order: children sort after the parent and before
        // the parent's east neighbor's children.
        let children = tile.children();
        assert_eq!(children.len(), 4);
        assert!(children.iter().all(|child| child.parent_at(10) == tile));
        assert!(
            children
                .array_windows::<2>()
                .all(|pair| pair[0].quadkey() < pair[1].quadkey())
        );
        assert_eq!(tile.parent_at(0), Tile { z: 0, x: 0, y: 0 });
        // Out-of-domain latitudes are rejected (no silent clamp).
        assert!(Tile::from_lonlat(0.0, 90.0, 5).is_none());
        assert!(Tile::from_lonlat(0.0, -90.0, 5).is_none());
        assert!(Tile::from_lonlat(0.0, TILE_MAX_LATITUDE, 5).is_some());
        assert!(Tile::from_lonlat(0.0, -TILE_MAX_LATITUDE, 5).is_some());
        // Antimeridian wrap and pole edges.
        let east_edge = Tile { z: 4, x: 15, y: 7 };
        assert_eq!(east_edge.neighbor(1, 0).unwrap().x, 0);
        assert!(Tile { z: 4, x: 3, y: 0 }.neighbor(0, -1).is_none());
        assert_eq!(Tile { z: 0, x: 0, y: 0 }.neighbors().len(), 0);
        // z0 has no distinct neighbors (wraps onto itself).
        let id_order_parent = Tile { z: 3, x: 1, y: 1 };
        let id_order_other = Tile { z: 3, x: 2, y: 1 };
        assert!(id_order_parent.id() < id_order_other.id());
    }

    #[test]
    fn one_trig_web_mercator_matches_tile_rows_at_boundaries() {
        for zoom in 0..=TILE_MAX_ZOOM {
            let cells = 1_u32 << u32::from(zoom);
            let rows = [
                0,
                1.min(cells - 1),
                cells / 4,
                cells / 2,
                3 * cells / 4,
                cells.saturating_sub(2),
                cells - 1,
            ];
            for row in rows {
                let n = PI * (1.0 - 2.0 * f64::from(row) / f64::from(cells));
                let boundary = n.sinh().atan().to_degrees();
                for lat in [boundary.next_down(), boundary, boundary.next_up()] {
                    if !(-TILE_MAX_LATITUDE..=TILE_MAX_LATITUDE).contains(&lat) {
                        assert!(
                            Tile::from_lonlat(0.0, lat, zoom).is_none(),
                            "zoom={zoom} row={row} lat={lat:?}"
                        );
                        continue;
                    }
                    assert_eq!(
                        Tile::from_lonlat(0.0, lat, zoom).expect("in domain").y,
                        reference_tile_y(lat, zoom),
                        "zoom={zoom} row={row} lat={lat:?}"
                    );
                }
            }
            for lat in [
                -85.051_128_779_806_6,
                -80.0,
                -66.513_260_443_111_86,
                -45.0,
                -1e-12,
                0.0,
                1e-12,
                45.0,
                66.513_260_443_111_86,
                80.0,
                85.051_128_779_806_6,
            ] {
                assert_eq!(
                    Tile::from_lonlat(0.0, lat, zoom).expect("in domain").y,
                    reference_tile_y(lat, zoom),
                    "zoom={zoom} lat={lat:?}"
                );
            }
            for lat in [-90.0, 90.0, 86.0, -86.0] {
                assert!(
                    Tile::from_lonlat(0.0, lat, zoom).is_none(),
                    "zoom={zoom} lat={lat:?}"
                );
            }
        }
    }

    #[test]
    fn uncompact_rejects_over_budget() {
        let root = Tile { z: 0, x: 0, y: 0 };
        let err = uncompact(&[root], 10).expect_err("z0 → z10 exceeds budget");
        assert!(err.estimated > crate::grid::UNCOMPACT_MAX_CELLS);
    }
}
