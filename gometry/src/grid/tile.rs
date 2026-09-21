#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
//! XYZ web-mercator tiles: the slippy-map `z/x/y` addressing scheme.
//!
//! A `Tile` is `(z, x, y)` with mercantile-compatible math; the packed
//! `u64` id stores the zoom in the top six bits and the Morton-interleaved
//! `x`/`y` below, so integer order equals `(zoom, locality)` order. Tokens
//! are quadkeys (Bing's digit-per-level path; the empty quadkey is the
//! `z0` world tile). Zoom caps at 29 — the deliberate pre-v1 limit the
//! packed id affords.
use std::f64::consts::PI;

use crate::boundary::geographic::normalized_geographic_pole;
use crate::curves::{morton_deinterleave, morton_interleave};
use crate::geometry::{Bounds, Point, Shape};
use crate::grid::coverer::RectCell;

pub(crate) const TILE_MAX_ZOOM: u8 = 29;

/// The latitude edge of the square Web Mercator tile grid.
pub(crate) const TILE_MAX_LATITUDE: f64 = 85.051_128_779_806_6;

/// Whether `lat` lies inside the square Web Mercator / slippy-tile grid.
pub(crate) fn latitude_in_tile_domain(lat: f64) -> bool {
    (-TILE_MAX_LATITUDE..=TILE_MAX_LATITUDE).contains(&lat)
}

/// Reject a tile-covering geometry whose any non-polar vertex latitude is
/// outside ±[`TILE_MAX_LATITUDE`]. Tile coverings cannot represent latitudes
/// past the square Web Mercator tile-grid edge; silently clipping (identical
/// cell counts for ±84 vs ±89.9) is the defect this gate closes. A physical pole is the one
/// admitted exception: the rectangular coverer's geographic authority can
/// classify a pole-touching source without representing the pole as a tile
/// point. Matches the point/tile_cells path for every non-polar coordinate.
///
/// Returns the first latitude outside the square tile grid, or `Ok(())` when
/// every vertex is in the grid (or the shape has no vertices).
pub(crate) fn ensure_cover_shape_in_tile_domain(shape: &Shape) -> Result<(), f64> {
    let mut bad = None;
    shape.for_each_point(|point: Point| {
        if bad.is_none()
            && !latitude_in_tile_domain(point.y)
            && normalized_geographic_pole(point.y).is_none()
        {
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

fn tile_cells(zoom: u8) -> f64 {
    2_f64.powi(i32::from(zoom))
}

/// One emitted longitude boundary in the canonical `[-180, 180]` spelling.
fn longitude_boundary(index: u32, cells: f64) -> f64 {
    f64::from(index) / cells * 360.0 - 180.0
}

/// One emitted latitude boundary, with rows increasing southward.
fn latitude_boundary(index: u32, cells: f64) -> f64 {
    let n = PI * (1.0 - 2.0 * f64::from(index) / cells);
    n.sinh().atan().to_degrees()
}

/// Correct a longitude seed against the emitted half-open boundaries.
fn locate_longitude(lon: f64, seed: u32, cells: f64) -> u32 {
    let limit = cells as u32;
    let mut index = seed.min(limit - 1);
    while index > 0 && lon < longitude_boundary(index, cells) {
        index -= 1;
    }
    while index + 1 < limit && lon >= longitude_boundary(index + 1, cells) {
        index += 1;
    }
    index
}

/// Correct a latitude seed against the emitted north-inclusive, south-exclusive
/// boundaries. The boundary at row `index` is that row's north edge.
fn locate_latitude(lat: f64, seed: u32, cells: f64) -> u32 {
    let limit = cells as u32;
    let mut index = seed.min(limit - 1);
    while index > 0 && lat > latitude_boundary(index, cells) {
        index -= 1;
    }
    while index + 1 < limit && lat <= latitude_boundary(index + 1, cells) {
        index += 1;
    }
    index
}

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

    /// The tile containing a lon/lat point inside the square Web Mercator tile grid.
    ///
    /// Returns `None` when `lat` is outside ±[`TILE_MAX_LATITUDE`] (about
    /// ±85.05112878°). Latitudes outside the square tile grid are rejected
    /// rather than silently clamped — callers that need a typed error raise at
    /// the boundary; mercantile-style clamp is not offered.
    pub(crate) fn from_lonlat(lon: f64, lat: f64, zoom: u8) -> Option<Self> {
        let (lon, lat) = super::canonical_lonlat(lon, lat);
        if !latitude_in_tile_domain(lat) {
            return None;
        }
        let cells = tile_cells(zoom);
        let last = cells - 1.0;
        // The forward projection is only a seed. Point ownership follows the
        // rounded boundaries emitted by `bounds()` on both axes.
        let x_seed = (((lon + 180.0) / 360.0 * cells).floor().clamp(0.0, last)) as u32;
        let lat_rad = lat.to_radians();
        // Canonical Web Mercator forward form: one tangent instead of the
        // equivalent `tan(lat) + sec(lat)` pair. This is the same one-trig
        // identity used by the in-core projection kernel.
        let mercator_y = (PI / 4.0 + lat_rad / 2.0).tan().ln();
        let normalized_y = (1.0 - mercator_y / PI) / 2.0 * cells;
        let y_seed = normalized_y.floor().clamp(0.0, last) as u32;
        Some(Self {
            z: zoom,
            x: locate_longitude(lon, x_seed, cells),
            y: locate_latitude(lat, y_seed, cells),
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
    ///
    /// The returned binary64 edges are the representational boundaries used
    /// for point ownership; this does not claim exact ideal-transcendental
    /// Web Mercator inversion.
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn bounds(self) -> Bounds {
        let cells = tile_cells(self.z);
        Bounds::new_raw(
            longitude_boundary(self.x, cells),
            latitude_boundary(self.y + 1, cells),
            longitude_boundary(self.x + 1, cells),
            latitude_boundary(self.y, cells),
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
    pub(crate) fn neighbors(self) -> crate::error::Result<Vec<Self>> {
        super::ring_neighbors(self, [-1, 0, 1], Self::neighbor)
    }
}

/// The single z0 world tile — the coverer's seed.
pub(crate) const fn root() -> Tile {
    Tile { z: 0, x: 0, y: 0 }
}

#[cfg(test)]
use crate::grid::cell_set::{compact_with_floor, uncompact};

impl RectCell for Tile {
    fn depth(self) -> u8 {
        self.z
    }
    fn bounds(self) -> crate::geometry::Bounds {
        Self::bounds(self)
    }
    fn children(self) -> impl ExactSizeIterator<Item = Self> {
        Self::children(self).into_iter()
    }
    fn push_children_bounds(
        self,
        parent: Bounds,
        out: &mut Vec<(Self, Bounds)>,
    ) -> crate::error::Result<()> {
        // Child edges use the same emitted-boundary functions as `bounds()`.
        // The child grid has twice the parent's cell count, so the shared
        // midpoint is just its odd-indexed boundary.
        let child_cells = tile_cells(self.z) * 2.0;
        let child_x = self.x * 2;
        let child_y = self.y * 2;
        let mid_lon = longitude_boundary(child_x + 1, child_cells);
        let mid_lat = latitude_boundary(child_y + 1, child_cells);
        let kids = Self::children(self);
        crate::try_push_vec(
            out,
            (
                kids[0],
                Bounds::new_raw(parent.minx(), mid_lat, mid_lon, parent.maxy()),
            ),
        )?;
        crate::try_push_vec(
            out,
            (
                kids[1],
                Bounds::new_raw(mid_lon, mid_lat, parent.maxx(), parent.maxy()),
            ),
        )?;
        crate::try_push_vec(
            out,
            (
                kids[2],
                Bounds::new_raw(parent.minx(), parent.miny(), mid_lon, mid_lat),
            ),
        )?;
        crate::try_push_vec(
            out,
            (
                kids[3],
                Bounds::new_raw(mid_lon, parent.miny(), parent.maxx(), mid_lat),
            ),
        )?;
        Ok(())
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
            Tile {
                z: 28,
                x: 123_456_789,
                y: 234_567_890,
            },
        ];
        for parent in parents {
            let mut derived = Vec::new();
            parent
                .push_children_bounds(parent.bounds(), &mut derived)
                .expect("child bounds");
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
    fn periodic_seam_has_canonical_owner() {
        for lat in [-45.0, 0.0, 45.0] {
            let owner = Tile::from_lonlat(-180.0, lat, TILE_MAX_ZOOM).unwrap();
            for lon in [180.0, 540.0, -540.0] {
                assert_eq!(Tile::from_lonlat(lon, lat, TILE_MAX_ZOOM), Some(owner));
            }
            assert_eq!(
                Tile::from_lonlat((-180.0_f64).next_up(), lat, TILE_MAX_ZOOM),
                Some(owner)
            );
            assert_ne!(
                Tile::from_lonlat(180.0_f64.next_down(), lat, TILE_MAX_ZOOM),
                Some(owner)
            );
        }
    }

    #[test]
    fn compact_merges_sibling_quads_and_uncompact_inverts() {
        let parent = Tile { z: 5, x: 10, y: 20 };
        // All four children of `parent` compact back into it…
        let children = parent.children().to_vec();
        assert_eq!(
            compact_with_floor(children.clone(), 0).into_public_vec(),
            vec![parent]
        );
        assert_eq!(
            compact_with_floor(children.clone(), 5).into_public_vec(),
            vec![parent]
        );
        // …unless the floor (coarsest allowed) forbids producing zoom 5.
        assert_eq!(
            compact_with_floor(children.clone(), 6).into_public_vec(),
            uncompact(&children, 6)
                .expect("descendant count fits usize")
                .into_public_vec()
        );
        // Three of four siblings do NOT merge (incomplete quad).
        let partial: Vec<Tile> = children[..3].to_vec();
        assert_eq!(compact_with_floor(partial, 0).as_slice().len(), 3);
        // uncompact expands back to a uniform zoom and round-trips a compacted set.
        let expanded = uncompact(&[parent], 7).expect("descendant count fits usize");
        assert_eq!(expanded.as_slice().len(), 16);
        assert_eq!(
            compact_with_floor(expanded.as_slice().to_vec(), 0).into_public_vec(),
            vec![parent]
        );
        // A tile nested under an ancestor already present is absorbed —
        // including when the ancestor is COARSER than the merge floor.
        let mixed = vec![parent, Tile { z: 7, x: 41, y: 81 }];
        assert_eq!(
            compact_with_floor(mixed.clone(), 0).into_public_vec(),
            vec![parent]
        );
        assert_eq!(compact_with_floor(mixed, 6).into_public_vec(), vec![parent]);
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
        assert_eq!(Tile { z: 0, x: 0, y: 0 }.neighbors().unwrap().len(), 0);
        // z0 has no distinct neighbors (wraps onto itself).
        let id_order_parent = Tile { z: 3, x: 1, y: 1 };
        let id_order_other = Tile { z: 3, x: 2, y: 1 };
        assert!(id_order_parent.id() < id_order_other.id());
    }

    #[test]
    fn maximum_zoom_id_round_trips_full_morton_payload() {
        let last = (1_u32 << TILE_MAX_ZOOM) - 1;
        let tile = Tile {
            z: TILE_MAX_ZOOM,
            x: last,
            y: last,
        };
        assert_eq!(tile.id(), 0x77FF_FFFF_FFFF_FFFF);
        assert_eq!(Tile::from_id(tile.id()), Some(tile));
    }

    #[test]
    fn emitted_latitude_boundaries_have_stable_owners() {
        for zoom in 0..=TILE_MAX_ZOOM {
            let cells = tile_cells(zoom) as u32;
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
                let boundary = latitude_boundary(row, f64::from(cells));
                assert_eq!(
                    Tile::from_lonlat(0.0, boundary, zoom).expect("in domain").y,
                    row,
                    "exact north edge: zoom={zoom} row={row}"
                );
                if row > 0 {
                    assert_eq!(
                        Tile::from_lonlat(0.0, boundary.next_up(), zoom)
                            .expect("in domain")
                            .y,
                        row - 1,
                        "north of edge: zoom={zoom} row={row}"
                    );
                }
                if row + 1 < cells {
                    assert_eq!(
                        Tile::from_lonlat(0.0, boundary.next_down(), zoom)
                            .expect("in domain")
                            .y,
                        row,
                        "south of edge: zoom={zoom} row={row}"
                    );
                }
            }
        }
    }

    #[test]
    fn named_latitude_boundaries_keep_literal_row_ownership() {
        let cases = [
            (4, 79.171_334_640_819_44, 2),
            (4, 79.171_334_640_819_45, 2),
            (4, 79.171_334_640_819_47, 1),
            (4, -74.019_543_311_502_28, 13),
            (29, 83.731_647_374_545_96, 20_227_569),
            (29, 83.731_647_374_545_97, 20_227_569),
            (29, 83.731_647_374_545_99, 20_227_568),
            (29, -80.579_680_698_441_98, 481_729_089),
        ];
        for (zoom, lat, expected) in cases {
            assert_eq!(
                Tile::from_lonlat(0.0, lat, zoom).expect("in domain").y,
                expected,
                "zoom={zoom} lat={lat:?}"
            );
        }
    }

    #[test]
    fn emitted_longitude_boundaries_are_half_open() {
        let cases = [
            (1, 1, -5e-324, 0),
            (1, 1, 0.0, 1),
            (1, 1, 5e-324, 1),
            (29, 0x1000_0000, 0.0, 0x1000_0000),
        ];
        for (zoom, index, lon, expected) in cases {
            assert_eq!(
                Tile::from_lonlat(lon, 0.0, zoom).expect("in domain").x,
                expected,
                "zoom={zoom} index={index} lon={lon:?}"
            );
        }
    }

    #[test]
    fn emitted_boundaries_are_strictly_ordered() {
        for zoom in 0..=TILE_MAX_ZOOM {
            let cells = tile_cells(zoom) as u32;
            let count = if zoom <= 20 { cells + 1 } else { 1_001 };
            let mut previous_lon = longitude_boundary(0, f64::from(cells));
            let mut previous_lat = latitude_boundary(0, f64::from(cells));
            for offset in 1..count {
                let index = if zoom <= 20 {
                    offset
                } else {
                    u32::try_from(u64::from(offset) * u64::from(cells) / u64::from(count - 1))
                        .expect("sampled boundary index fits u32")
                };
                let lon = longitude_boundary(index, f64::from(cells));
                let lat = latitude_boundary(index, f64::from(cells));
                assert!(lon > previous_lon, "longitude zoom={zoom} index={index}");
                assert!(lat < previous_lat, "latitude zoom={zoom} index={index}");
                previous_lon = lon;
                previous_lat = lat;
            }
            assert_eq!(
                latitude_boundary(0, f64::from(cells)).to_bits(),
                TILE_MAX_LATITUDE.to_bits(),
                "north edge zoom={zoom}"
            );
            assert_eq!(
                latitude_boundary(cells, f64::from(cells)).to_bits(),
                (-TILE_MAX_LATITUDE).to_bits(),
                "south edge zoom={zoom}"
            );
        }
    }

    #[test]
    fn parent_and_child_boundaries_are_bit_identical() {
        for zoom in 0..TILE_MAX_ZOOM {
            let cells = tile_cells(zoom);
            let child_cells = cells * 2.0;
            let count = cells as u32;
            let indices = if zoom <= 20 {
                (0..=count).collect::<Vec<_>>()
            } else {
                (0_u32..=1_000)
                    .map(|offset| {
                        u32::try_from(u64::from(offset) * u64::from(count) / 1_000)
                            .expect("sampled boundary index fits u32")
                    })
                    .collect::<Vec<_>>()
            };
            for index in indices {
                assert_eq!(
                    longitude_boundary(index, cells).to_bits(),
                    longitude_boundary(index * 2, child_cells).to_bits(),
                    "longitude zoom={zoom} index={index}"
                );
                assert_eq!(
                    latitude_boundary(index, cells).to_bits(),
                    latitude_boundary(index * 2, child_cells).to_bits(),
                    "latitude zoom={zoom} index={index}"
                );
            }
        }
    }

    #[test]
    fn outer_latitude_edges_keep_admission_and_error_contract() {
        for zoom in 0..=TILE_MAX_ZOOM {
            let last = tile_cells(zoom) as u32 - 1;
            assert_eq!(
                Tile::from_lonlat(0.0, TILE_MAX_LATITUDE, zoom)
                    .expect("north edge is in domain")
                    .y,
                0
            );
            assert_eq!(
                Tile::from_lonlat(0.0, -TILE_MAX_LATITUDE, zoom)
                    .expect("south edge is in domain")
                    .y,
                last
            );
            for lat in [-90.0, 90.0, -86.0, 86.0] {
                assert!(
                    Tile::from_lonlat(0.0, lat, zoom).is_none(),
                    "zoom={zoom} lat={lat:?}"
                );
            }
        }
    }
}
