//! Packed geohash cells: the classic base-32 lon/lat bisection code.
//!
//! A `Geohash` packs up to 12 base-32 characters (5 bits each) left-aligned
//! into a `u64`, so within one precision the integer order equals the
//! token's lexicographic order — the property that makes sorted cell sets
//! and range membership work like the numeric systems. Even bit positions
//! (from the top) refine longitude, odd refine latitude.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use crate::geometry::Bounds;

/// The canonical geohash base-32 alphabet (no `a`, `i`, `l`, `o`).
const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

const ALPHABET_VALUE: [i8; 256] = build_alphabet_lut();

const fn build_alphabet_lut() -> [i8; 256] {
    let mut lut = [-1_i8; 256];
    let mut index = 0;
    while index < 32 {
        lut[ALPHABET[index] as usize] = index as i8;
        index += 1;
    }
    lut
}

pub(crate) const GEOHASH_MAX_PRECISION: u8 = 12;

/// One geohash cell: `precision * 5` significant bits, left-aligned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Geohash {
    /// Character bits, left-aligned from bit 63; trailing bits zero.
    pub bits: u64,
    /// Character count, `1..=12`.
    pub precision: u8,
}

crate::heapless!(Geohash);

fn axis_counts(precision: u8) -> (u32, u32) {
    let total = u32::from(precision) * 5;
    (total.div_ceil(2), total / 2)
}

/// Discretize one axis to its cell index — the closed form of the geohash
/// bisection loop (`value >= mid` picks the upper half at each bit).
fn discretize_axis(value: f64, min: f64, max: f64, bits: u32) -> u64 {
    if bits == 0 {
        return 0;
    }
    let cells = 1_u64 << bits;
    let span = max - min;
    let mut index = (((value - min) / span) * cells as f64) as u64;
    index = index.min(cells - 1);

    // Every bisection boundary is exactly representable here: `min`, `max`,
    // and `span` are integers; `bits <= 30`; and `index * span` needs fewer
    // than 53 significant bits. Dividing by `2**bits` is an exact exponent
    // adjustment, so this expression produces the same dyadic midpoint as
    // the serial bisection. Correct the arithmetic estimate against those
    // exact boundaries to preserve the canonical tie rule (`>=` goes upper),
    // including the immediate `nextafter` neighbors of every boundary.
    let boundary = |index: u64| min + (index as f64 * span) / cells as f64;
    if index > 0 && value < boundary(index) {
        index -= 1;
    } else if index + 1 < cells && value >= boundary(index + 1) {
        index += 1;
    }
    index
}

fn alphabet_value(byte: u8) -> Option<u64> {
    let value = ALPHABET_VALUE[byte.to_ascii_lowercase() as usize];
    (value >= 0).then_some(value as u64)
}

impl Geohash {
    /// Parse a token (case-insensitive, canonicalized lowercase). The
    /// message-only error keeps the kernel boundary-agnostic — the Python
    /// layer raises its `ParseError`.
    pub(crate) fn parse(token: &str) -> Result<Self, String> {
        let bytes = token.as_bytes();
        if bytes.is_empty() || bytes.len() > GEOHASH_MAX_PRECISION as usize {
            return Err(format!(
                "geohash tokens have 1 to 12 characters, got {token:?}"
            ));
        }
        let mut bits = 0_u64;
        for (index, &byte) in bytes.iter().enumerate() {
            let Some(value) = alphabet_value(byte) else {
                return Err(format!(
                    "invalid geohash character {:?} in {token:?}",
                    char::from(byte)
                ));
            };
            bits |= value << (64 - 5 - 5 * index);
        }
        Ok(Self {
            bits,
            precision: bytes.len() as u8,
        })
    }

    /// The cell containing a lon/lat point at `precision`.
    pub(crate) fn from_lonlat(lon: f64, lat: f64, precision: u8) -> Self {
        let (lon_count, lat_count) = axis_counts(precision);
        let lon_index = discretize_axis(
            lon,
            crate::boundary::geographic::MIN_LONGITUDE,
            crate::boundary::geographic::MAX_LONGITUDE,
            lon_count,
        );
        let lat_index = discretize_axis(
            lat,
            crate::boundary::geographic::MIN_LATITUDE,
            crate::boundary::geographic::MAX_LATITUDE,
            lat_count,
        );
        Self::from_axes(lon_index, lat_index, precision)
    }

    /// The canonical lowercase token.
    pub(crate) fn token(self) -> String {
        (0..self.precision)
            .map(|index| {
                let value = (self.bits >> (64 - 5 - 5 * u32::from(index))) & 0x1F;
                char::from(ALPHABET[value as usize])
            })
            .collect()
    }

    /// A `u64` that is unique per cell AND orders like the derived
    /// `Ord = (bits, precision)`. `bits` alone is NOT unique — a cell and its
    /// zeroth child share it (the child appends only zero bits) — so a
    /// membership search keyed on `bits` would conflate them. `precision`
    /// (`1..=12`, 4 bits) fits in the `>= 4` trailing zero bits `bits` always
    /// has (at most `12*5 = 60` bits are used), so OR-ing it in disambiguates
    /// without disturbing the high-bit ordering.
    pub(crate) fn identity_key(self) -> u64 {
        self.bits | u64::from(self.precision)
    }

    /// The cell's lon/lat bounding rectangle.
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn bounds(self) -> Bounds {
        let (lon_index, lat_index) = self.split_axes();
        let (lon_count, lat_count) = axis_counts(self.precision);
        let lon_span = 360.0 / (1_u64 << lon_count) as f64;
        let lat_span = 180.0 / (1_u64 << lat_count) as f64;
        Bounds::new_unchecked(
            -180.0 + lon_index as f64 * lon_span,
            -90.0 + lat_index as f64 * lat_span,
            -180.0 + (lon_index + 1) as f64 * lon_span,
            -90.0 + (lat_index + 1) as f64 * lat_span,
        )
    }

    /// Cell center (lon, lat).
    #[cfg(test)]
    pub(crate) fn center(self) -> (f64, f64) {
        let bounds = self.bounds();
        (
            f64::midpoint(bounds.minx(), bounds.maxx()),
            f64::midpoint(bounds.miny(), bounds.maxy()),
        )
    }

    /// The parent at `precision` (must be coarser or equal).
    pub(crate) fn parent_at(self, precision: u8) -> Self {
        let keep = u32::from(precision) * 5;
        let mask = if keep == 0 {
            0
        } else {
            u64::MAX << (64 - keep)
        };
        Self {
            bits: self.bits & mask,
            precision,
        }
    }

    /// All children one level finer, in token order.
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(crate) fn children(self) -> impl DoubleEndedIterator<Item = Self> + ExactSizeIterator {
        let precision = self.precision + 1;
        (0..32).map(move |value| Self {
            bits: self.bits | ((value as u64) << (64 - 5 * u32::from(precision))),
            precision,
        })
    }

    /// The neighbor cell `dx` columns east and `dy` rows north, wrapping
    /// across the antimeridian; `None` past the poles.
    pub(crate) fn neighbor(self, dx: i64, dy: i64) -> Option<Self> {
        let (lon_bits, lat_bits) = self.split_axes();
        let lon_count = u32::from(self.precision) * 5 - u32::from(self.precision) * 5 / 2;
        let lat_count = u32::from(self.precision) * 5 / 2;
        let lon_cells = 1_i64 << lon_count;
        let lat_cells = 1_i64 << lat_count;
        // `dx ∈ {-1,0,1}` and `lon_cells ≥ 8`, so the branchless wrap reproduces
        // `rem_euclid` exactly without the `idiv` it compiles to (hot in `neighbors`).
        let lon = super::wrap_axis(lon_bits as i64 + dx, lon_cells) as u64;
        let lat = lat_bits as i64 + dy;
        if lat < 0 || lat >= lat_cells {
            return None;
        }
        Some(Self::from_axes(lon, lat as u64, self.precision))
    }

    /// The 8 surrounding cells (those that exist), row-major from
    /// north-west to south-east (lat grows north, so rows walk `[1, 0, -1]`).
    pub(crate) fn neighbors(self) -> Vec<Self> {
        super::ring_neighbors(self, [1, 0, -1], Self::neighbor)
    }

    /// The (lon, lat) bit planes, left-aligned at bit 31. Geohash bits
    /// interleave lon-first from bit 63, so lon sits on the ODD u64
    /// positions and lat on the even ones; trailing bits are zero, so
    /// same-precision planes compare in column/row order as-is.
    const fn axis_planes(self) -> (u32, u32) {
        let (lat, lon) = crate::curves::morton_deinterleave(self.bits);
        (lon, lat)
    }

    /// De-interleave the packed bits into (lon, lat) axis integers.
    fn split_axes(self) -> (u64, u64) {
        let total = u32::from(self.precision) * 5;
        let (lon, lat) = self.axis_planes();
        (
            u64::from(lon >> (32 - total.div_ceil(2))),
            u64::from(lat >> (32 - total / 2)),
        )
    }

    /// Re-interleave axis integers into a packed cell.
    fn from_axes(lon: u64, lat: u64, precision: u8) -> Self {
        let total = u32::from(precision) * 5;
        let lon_plane = (lon << (32 - total.div_ceil(2))) as u32;
        let lat_plane = (lat << (32 - total / 2)) as u32;
        Self {
            bits: crate::curves::morton_interleave(lat_plane, lon_plane),
            precision,
        }
    }
}

pub(crate) use crate::grid::cell_set::{compact_with_floor, uncompact, uncompact_unlimited};

/// The 32 precision-1 root cells, in token order — the coverer's seeds.
pub(crate) fn roots() -> Vec<Geohash> {
    (0..32_u64)
        .map(|value| Geohash {
            bits: value << (64 - 5),
            precision: 1,
        })
        .collect()
}

impl crate::grid::coverer::RectCell for Geohash {
    fn depth(self) -> u8 {
        self.precision
    }
    fn bounds(self) -> crate::geometry::Bounds {
        Self::bounds(self)
    }
    fn children(self) -> impl Iterator<Item = Self> {
        Self::children(self)
    }
    fn edge_neighbors(self) -> [Option<Self>; 4] {
        // Geohash rows grow northward (lat up), so south is lat-1 and north is
        // lat+1; east/west do NOT wrap the antimeridian (None at the lon edges).
        let (lon_bits, lat_bits) = self.split_axes();
        let total = u32::from(self.precision) * 5;
        let lon_cells = 1_u64 << (total - total / 2);
        let lat_cells = 1_u64 << (total / 2);
        let at = |lon: u64, lat: u64| Self::from_axes(lon, lat, self.precision);
        [
            (lat_bits > 0).then(|| at(lon_bits, lat_bits - 1)), // south (miny)
            (lon_bits + 1 < lon_cells).then(|| at(lon_bits + 1, lat_bits)), // east (maxx)
            (lat_bits + 1 < lat_cells).then(|| at(lon_bits, lat_bits + 1)), // north (maxy)
            (lon_bits > 0).then(|| at(lon_bits - 1, lat_bits)), // west (minx)
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn discretize_axis_reference(value: f64, min: f64, max: f64, bits: u32) -> u64 {
        let mut index = 0_u64;
        let (mut lo, mut hi) = (min, max);
        for _ in 0..bits {
            let mid = f64::midpoint(lo, hi);
            index <<= 1;
            if value >= mid {
                index |= 1;
                lo = mid;
            } else {
                hi = mid;
            }
        }
        index
    }

    #[test]
    fn round_trips_known_vectors() {
        // georust/geohash and geohash.org vectors.
        let cell = Geohash::from_lonlat(-120.6623, 35.3003, 9);
        assert_eq!(cell.token(), "9q60y60rh");
        assert_eq!(Geohash::parse("9q60y60rh").unwrap(), cell);
        assert_eq!(
            Geohash::from_lonlat(112.5584, 37.8324, 9).token(),
            "ww8p1r4t8"
        );
        assert_eq!(Geohash::from_lonlat(117.0, 32.0, 3).token(), "wte");
        // Case-insensitive parse canonicalizes.
        assert_eq!(Geohash::parse("WW8P1R4T8").unwrap().token(), "ww8p1r4t8");
    }

    #[test]
    fn bounds_parents_children_and_neighbors() {
        let cell = Geohash::parse("ww8p1r4t8").unwrap();
        let bounds = cell.bounds();
        let (lon, lat) = cell.center();
        assert!(bounds.minx() <= 112.5584 && 112.5584 <= bounds.maxx());
        assert!(bounds.miny() <= 37.8324 && 37.8324 <= bounds.maxy());
        assert!((lon - 112.5584).abs() < 1e-4 && (lat - 37.8324).abs() < 1e-4);
        assert_eq!(cell.parent_at(4).token(), "ww8p");
        let children: Vec<_> = cell.parent_at(4).children().collect();
        assert_eq!(children.len(), 32);
        assert!(children.iter().any(|child| child.token() == "ww8p1"));
        assert!(children.windows(2).all(|pair| pair[0] < pair[1]));
        // Known neighbor table for "ww8p" (east = ww8r? verified via grid
        // math): the 8-neighborhood is distinct, same precision, adjacent.
        let neighbors = Geohash::parse("u09t").unwrap().neighbors();
        assert_eq!(neighbors.len(), 8);
        assert!(neighbors.iter().any(|cell| cell.token() == "u09w"));
        // Antimeridian wrap: the east neighbor of the easternmost column
        // is the westernmost one.
        let east_edge = Geohash::from_lonlat(179.9, 0.1, 4);
        let wrapped = east_edge.neighbor(1, 0).unwrap();
        assert!(wrapped.bounds().minx() <= -180.0);
        // Pole edge: no neighbor beyond the top row.
        let top = Geohash::from_lonlat(0.0, 89.9, 4);
        assert!(top.neighbor(0, 1).is_none());
    }

    #[test]
    fn fixed_point_discretization_is_exact_at_cell_boundaries() {
        let axes = [(-180.0, 180.0), (-90.0, 90.0)];
        for precision in 1..=GEOHASH_MAX_PRECISION {
            let (lon_bits, lat_bits) = axis_counts(precision);
            let counts: [u32; 2] = (lon_bits, lat_bits).into();
            for ((min, max), bits) in axes.into_iter().zip(counts) {
                let cells = 1_u64 << bits;
                let span = max - min;
                let mut indices = vec![
                    1,
                    2,
                    cells / 4,
                    cells / 2,
                    3 * cells / 4,
                    cells - 2,
                    cells - 1,
                ];
                for numerator in 1..7 {
                    let center = cells * numerator / 7;
                    for index in [center.saturating_sub(1), center, center.saturating_add(1)] {
                        if (1..cells).contains(&index) {
                            indices.push(index);
                        }
                    }
                }
                indices.sort_unstable();
                indices.dedup();
                for index in indices {
                    let boundary = min + (index as f64 * span) / cells as f64;
                    for value in [boundary.next_down(), boundary, boundary.next_up()] {
                        assert_eq!(
                            discretize_axis(value, min, max, bits),
                            discretize_axis_reference(value, min, max, bits),
                            "precision={precision} bits={bits} boundary={index} value={value:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn packed_order_matches_token_order_after_fixed_point_discretization() {
        let mut cells = Vec::new();
        for longitude in (-179..=179).step_by(7) {
            for latitude in (-89..=89).step_by(11) {
                cells.push(Geohash::from_lonlat(
                    f64::from(longitude),
                    f64::from(latitude),
                    12,
                ));
            }
        }
        let mut by_id = cells.clone();
        by_id.sort_unstable();
        let mut by_token = cells;
        by_token.sort_unstable_by_key(|cell| cell.token());
        assert_eq!(by_id, by_token);
    }

    #[test]
    fn uncompact_rejects_over_budget() {
        let root = Geohash::parse("u").unwrap();
        let err = uncompact(&[root], 5).expect_err("precision 1 → 5 exceeds budget");
        assert!(err.estimated > crate::grid::UNCOMPACT_MAX_CELLS);
    }
}
