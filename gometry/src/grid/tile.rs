//! XYZ web-mercator tiles: the slippy-map `z/x/y` addressing scheme.
//!
//! A `Tile` is `(z, x, y)` with mercantile-compatible math; the packed
//! `u64` id stores the zoom in the top six bits and the Morton-interleaved
//! `x`/`y` below, so integer order equals `(zoom, locality)` order. Tokens
//! are quadkeys (Bing's digit-per-level path; the empty quadkey is the
//! `z0` world tile). Zoom caps at 29 — the deliberate pre-v1 limit the
//! packed id affords.
use std::f64::consts::PI;
use std::ops::ControlFlow;

use crate::curves::{morton_deinterleave, morton_interleave};
use crate::geographic::lonlat::normalized_geographic_pole;
use crate::geometry::numerics::product::exact_power_of_two;
use crate::geometry::{Bounds, Point, Shape};
use crate::grid::coverer::RectCell;

pub(crate) const TILE_MAX_ZOOM: u8 = 29;

/// The low 58 bits of a packed tile id hold its Morton payload.
const TILE_ID_PAYLOAD_MASK: u64 = 0x03FF_FFFF_FFFF_FFFF;

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
    let bad = match shape.try_for_each_first_xy_point_run(|point: Point| {
        if !latitude_in_tile_domain(point.y) && normalized_geographic_pole(point.y).is_none() {
            ControlFlow::Break(point.y)
        } else {
            ControlFlow::Continue(())
        }
    }) {
        ControlFlow::Continue(()) => None,
        ControlFlow::Break(latitude) => Some(latitude),
    };
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
    exact_power_of_two(i32::from(zoom))
}

/// One emitted longitude boundary in the canonical `[-180, 180]` spelling.
fn longitude_boundary(index: u32, cells: f64) -> f64 {
    f64::from(index) / cells * 360.0 - 180.0
}

/// One emitted latitude boundary, with rows increasing southward.
fn latitude_boundary(index: u32, cells: f64) -> f64 {
    if index == 0 {
        return TILE_MAX_LATITUDE;
    }
    if index == cells as u32 {
        return -TILE_MAX_LATITUDE;
    }
    let n = PI * (1.0 - 2.0 * f64::from(index) / cells);
    // These rounded values define half-open row ownership. Repeated platform
    // libm evaluations may vary, so use one arithmetic kernel for emitted and
    // queried boundaries.
    libm::atan(libm::sinh(n)).to_degrees()
}

/// Correct a longitude seed against the emitted half-open boundaries.
fn locate_longitude(lon: f64, seed: u32, cells: f64) -> u32 {
    let limit = cells as u32;
    let mut index = seed.min(limit.wrapping_sub(1));
    while index > 0 && lon < longitude_boundary(index, cells) {
        index = index.wrapping_sub(1);
    }
    loop {
        let next = index.wrapping_add(1);
        if next >= limit || lon < longitude_boundary(next, cells) {
            break;
        }
        index = next;
    }
    index
}

/// Correct a latitude seed against the emitted north-inclusive, south-exclusive
/// boundaries. The boundary at row `index` is that row's north edge.
fn locate_latitude(lat: f64, seed: u32, cells: f64) -> u32 {
    let limit = cells as u32;
    let mut index = seed.min(limit.wrapping_sub(1));
    while index > 0 && lat > latitude_boundary(index, cells) {
        index = index.wrapping_sub(1);
    }
    loop {
        let next = index.wrapping_add(1);
        if next >= limit || lat > latitude_boundary(next, cells) {
            break;
        }
        index = next;
    }
    index
}

impl Tile {
    /// The packed id: zoom in bits 63..58, Morton-interleaved `x`/`y`
    /// (y in the higher bit of each pair, so the id sorts in quadkey
    /// order) left-aligned below.
    pub(crate) fn id(self) -> u64 {
        let morton = morton_interleave(self.x, self.y);
        let shift = 58_u32.wrapping_sub(2_u32.wrapping_mul(u32::from(self.z)));
        u64::from(self.z).wrapping_shl(58) | (morton.wrapping_shl(shift) & TILE_ID_PAYLOAD_MASK)
    }

    /// Rebuild from a packed id; `None` when malformed (bad zoom or set
    /// bits below the tile's depth).
    pub(crate) fn from_id(id: u64) -> Option<Self> {
        let z = id.wrapping_shr(58) as u8;
        if z > TILE_MAX_ZOOM {
            return None;
        }
        let payload = id & TILE_ID_PAYLOAD_MASK;
        let shift = 58_u32.wrapping_sub(2_u32.wrapping_mul(u32::from(z)));
        if payload & 1_u64.wrapping_shl(shift).wrapping_sub(1) != 0 {
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
                let mask = 1_u32.wrapping_shl(level.wrapping_sub(1));
                let x_bit = u8::from(self.x & mask != 0);
                let y_bit = u8::from(self.y & mask != 0).wrapping_shl(1);
                char::from(b'0'.wrapping_add(x_bit | y_bit))
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
            latitude_boundary(self.y.wrapping_add(1), cells),
            longitude_boundary(self.x.wrapping_add(1), cells),
            latitude_boundary(self.y, cells),
        )
    }

    /// The parent at `zoom` (must be coarser or equal).
    pub(crate) fn parent_at(self, zoom: u8) -> Self {
        let shift = u32::from(self.z.wrapping_sub(zoom));
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
        let (z, x, y) = (
            self.z.wrapping_add(1),
            self.x.wrapping_shl(1),
            self.y.wrapping_shl(1),
        );
        [
            Self { z, x, y },
            Self {
                z,
                x: x.wrapping_add(1),
                y,
            },
            Self {
                z,
                x,
                y: y.wrapping_add(1),
            },
            Self {
                z,
                x: x.wrapping_add(1),
                y: y.wrapping_add(1),
            },
        ]
    }

    /// The neighbor `dx` columns east and `dy` rows SOUTH (tile rows grow
    /// southward); wraps across the antimeridian, `None` past the edges.
    pub(crate) fn neighbor(self, dx: i64, dy: i64) -> Option<Self> {
        let cells = 1_i64 << u32::from(self.z);
        // `dx ∈ {-1,0,1}` and `|dx| ≤ cells`, so the branchless wrap reproduces
        // `rem_euclid` exactly without the `idiv` it compiles to (hot in `neighbors`).
        let x = super::wrap_axis(i64::from(self.x).wrapping_add(dx), cells);
        let y = i64::from(self.y).wrapping_add(dy);
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
    fn push_children_bounds(self, parent: Bounds, out: &mut Vec<(Self, Bounds)>) {
        // Child edges use the same emitted-boundary functions as `bounds()`.
        // The child grid has twice the parent's cell count, so the shared
        // midpoint is just its odd-indexed boundary.
        let child_cells = tile_cells(self.z) * 2.0;
        let child_x = self.x.wrapping_shl(1);
        let child_y = self.y.wrapping_shl(1);
        let mid_lon = longitude_boundary(child_x.wrapping_add(1), child_cells);
        let mid_lat = latitude_boundary(child_y.wrapping_add(1), child_cells);
        let kids = Self::children(self);
        out.reserve(kids.len());
        out.extend([
            (
                kids[0],
                Bounds::new_raw(parent.minx(), mid_lat, mid_lon, parent.maxy()),
            ),
            (
                kids[1],
                Bounds::new_raw(mid_lon, mid_lat, parent.maxx(), parent.maxy()),
            ),
            (
                kids[2],
                Bounds::new_raw(parent.minx(), parent.miny(), mid_lon, mid_lat),
            ),
            (
                kids[3],
                Bounds::new_raw(mid_lon, parent.miny(), parent.maxx(), mid_lat),
            ),
        ]);
    }
    fn edge_neighbors(self) -> [Option<Self>; 4] {
        // Tile rows grow southward (y down), so south is y+1 and north is y-1;
        // east/west do NOT wrap the antimeridian (None at x = 0 / last column).
        let cells = 1_u32 << u32::from(self.z);
        let at = |x: u32, y: u32| Self { z: self.z, x, y };
        let south = self.y.wrapping_add(1);
        let east = self.x.wrapping_add(1);
        [
            (south < cells).then(|| at(self.x, south)), // south (miny)
            (east < cells).then(|| at(east, self.y)),   // east (maxx)
            (self.y > 0).then(|| at(self.x, self.y.wrapping_sub(1))), // north (maxy)
            (self.x > 0).then(|| at(self.x.wrapping_sub(1), self.y)), // west (minx)
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boundary_indices(cells: u32) -> Vec<u32> {
        let middle = cells / 2;
        let mut indices = vec![
            0,
            1.min(cells),
            cells / 4,
            middle.saturating_sub(1),
            middle,
            middle.wrapping_add(1).min(cells),
            cells.wrapping_mul(3) / 4,
            cells.saturating_sub(2),
            cells.wrapping_sub(1),
            cells,
        ];
        indices.sort_unstable();
        indices.dedup();
        indices
    }

    #[test]
    fn tile_cover_domain_keeps_first_bad_latitude_and_the_pole_exception() {
        let shape = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(0.0, 86.0)),
            Shape::Point(Point::new_raw_xy(0.0, 89.0)),
        ])
        .unwrap();
        let Err(latitude) = ensure_cover_shape_in_tile_domain(&shape) else {
            panic!("the first latitude is outside the tile domain");
        };
        assert_eq!(latitude, 86.0);

        for latitude in [90.0, 90.0 + 0.5e-12] {
            let pole = Shape::Point(Point::new_raw_xy(0.0, latitude));
            assert_eq!(ensure_cover_shape_in_tile_domain(&pole), Ok(()));
        }
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
            Tile {
                z: 28,
                x: 123_456_789,
                y: 234_567_890,
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

    #[rstest::rstest]
    #[case::negative_lat_45_0(-45.0)]
    #[case::lat_0_0(0.0)]
    #[case::lat_45_0(45.0)]
    fn periodic_seam_has_canonical_owner(#[case] lat: f64) {
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
        assert_eq!(Tile { z: 0, x: 0, y: 0 }.neighbors().len(), 0);
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
    fn from_id_rejects_zoom_above_maximum_and_nonzero_unused_bits() {
        assert_eq!(Tile::from_id(0x7800_0000_0000_0000), None);
        assert_eq!(Tile::from_id(1), None);
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

    #[rstest::rstest]
    #[case::zoom_4(4, 79.171_334_640_819_44, 2)]
    #[case::zoom_4_f672b540(4, 79.171_334_640_819_45, 2)]
    #[case::zoom_4_0de56745(4, 79.171_334_640_819_47, 1)]
    #[case::zoom_4_660cb8cc(4, -74.019_543_311_502_28, 13)]
    #[case::zoom_29(29, 83.731_647_374_545_96, 20_227_569)]
    #[case::zoom_29_3d42b00c(29, 83.731_647_374_545_97, 20_227_569)]
    #[case::zoom_29_a87cfa28(29, 83.731_647_374_545_99, 20_227_568)]
    #[case::zoom_29_b6d4779f(29, -80.579_680_698_441_98, 481_729_089)]
    fn named_latitude_boundaries_keep_literal_row_ownership(
        #[case] zoom: u8,
        #[case] lat: f64,
        #[case] expected: u32,
    ) {
        assert_eq!(
            Tile::from_lonlat(0.0, lat, zoom).expect("in domain").y,
            expected,
            "zoom={zoom} lat={lat:?}"
        );
    }

    #[rstest::rstest]
    #[case::zoom_1(1, 1, -5e-324, 0)]
    #[case::zoom_1_a114632d(1, 1, 0.0, 1)]
    #[case::zoom_1_33acc4df(1, 1, 5e-324, 1)]
    #[case::zoom_29(29, 0x1000_0000, 0.0, 0x1000_0000)]
    fn emitted_longitude_boundaries_are_half_open(
        #[case] zoom: u8,
        #[case] index: u32,
        #[case] lon: f64,
        #[case] expected: u32,
    ) {
        assert_eq!(
            Tile::from_lonlat(lon, 0.0, zoom).expect("in domain").x,
            expected,
            "zoom={zoom} index={index} lon={lon:?}"
        );
    }

    #[test]
    fn emitted_boundaries_are_strictly_ordered() {
        for zoom in 0..=TILE_MAX_ZOOM {
            let cells = tile_cells(zoom) as u32;
            for &[previous, index] in boundary_indices(cells).array_windows::<2>() {
                let previous_lon = longitude_boundary(previous, f64::from(cells));
                let previous_lat = latitude_boundary(previous, f64::from(cells));
                let lon = longitude_boundary(index, f64::from(cells));
                let lat = latitude_boundary(index, f64::from(cells));
                assert!(lon > previous_lon, "longitude zoom={zoom} index={index}");
                assert!(lat < previous_lat, "latitude zoom={zoom} index={index}");
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
            for index in boundary_indices(count) {
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
