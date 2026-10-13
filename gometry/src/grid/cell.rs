//! Shared `GridCell` trait for the four DGGS kernels (H3, S2, geohash, tile).
//!
//! Rectangular grids blanket-implement from [`RectGridCell`]; H3 and S2 hand-implement
//! in `src/py/cells/{h3,s2}/` where their PyO3-facing kernels live.

use crate::geographic::{GeographicFrame, lonlat as geographic};
use crate::geometry::{Point, Shape};
use crate::grid::cell_set;
use crate::grid::coverer::RectCell;
use crate::grid::geohash::{GEOHASH_MAX_PRECISION, Geohash};
use crate::grid::tile::{TILE_MAX_ZOOM, Tile};

/// Argument carried by a cell's `__reduce__` tuple.
pub(crate) enum CellPickleArg {
    U64(u64),
    Str(String),
}

/// One cell of a discrete global grid's hierarchy.
///
/// `range_min..=range_max` is an internal leaf-range key. `public_key` is the
/// stable order exposed by cell arrays and set algebra; keeping the two keys
/// separate matters for tiles, whose public identity is a packed Morton id
/// while hierarchy subtraction walks a descendant range.
pub(crate) trait HierarchicalCell: Copy + Eq {
    fn depth(self) -> u8;
    fn max_depth() -> u8;
    fn public_key(self) -> u64;
    fn range_min(self) -> u64;
    fn range_max(self) -> u64;
    fn parent(self) -> Option<Self>;
    fn children(self) -> impl ExactSizeIterator<Item = Self>;

    fn child_count(self) -> usize {
        self.children().len()
    }

    /// Exact number of descendants at `target_depth`, including this cell when
    /// the target equals its own depth. Non-uniform grids override this.
    fn descendant_count(self, target_depth: u8) -> u64 {
        let mut count = 1_u64;
        for _ in self.depth()..target_depth {
            count = count
                .checked_mul(self.child_count() as u64)
                .expect("hierarchical descendant count fits u64");
        }
        count
    }

    fn contains(self, other: Self) -> bool {
        self.range_min() <= other.range_min() && other.range_max() <= self.range_max()
    }
}

/// One cell's scalar Python-facing operations, layered over the common
/// [`HierarchicalCell`] contract shared by all four systems.
pub(crate) trait GridCell: HierarchicalCell + Copy + Eq + Ord + Send + Sync {
    const MIN_DEPTH: u8;
    const MAX_DEPTH: u8;
    const DEPTH_NAME: &'static str;

    fn hash_key(self) -> u64;
    fn token(self) -> String;
    fn center_point(self) -> Point;
    fn boundary_shape(self) -> crate::error::Result<Shape>;
    fn area_m2(self) -> f64;
    fn neighbors(self) -> Vec<Self>;
    fn parent_at(self, depth: u8) -> Option<Self>;
    fn children_to(self, depth: u8) -> crate::error::Result<Vec<Self>> {
        cell_set::uncompact(&[self], depth).map(cell_set::CanonicalCellSet::into_public_vec)
    }
    fn contains_cell(self, other: Self) -> bool {
        HierarchicalCell::contains(self, other)
    }
    fn pickle_arg(self) -> CellPickleArg;
}

/// Rectangular lon/lat grid cells — per-system metadata plus hierarchy hooks
/// that [`GridCell`] blanket-implements from [`RectCell`]'s bounds geometry.
pub(crate) trait RectGridCell: RectCell + Send + Sync {
    const MIN_DEPTH: u8;
    const MAX_DEPTH: u8;
    const DEPTH_NAME: &'static str;
    fn hash_key(self) -> u64;
    fn range_min(self) -> u64;
    fn range_max(self) -> u64;
    fn grid_token(self) -> String;
    fn grid_neighbors(self) -> Vec<Self>;
    fn grid_parent_at(self, depth: u8) -> Self;
    fn pickle_arg(self) -> CellPickleArg;
}

impl<T: RectGridCell> HierarchicalCell for T {
    fn depth(self) -> u8 {
        RectCell::depth(self)
    }

    fn max_depth() -> u8 {
        T::MAX_DEPTH
    }

    fn public_key(self) -> u64 {
        <Self as RectGridCell>::hash_key(self)
    }

    fn range_min(self) -> u64 {
        <Self as RectGridCell>::range_min(self)
    }

    fn range_max(self) -> u64 {
        <Self as RectGridCell>::range_max(self)
    }

    fn parent(self) -> Option<Self> {
        let depth = self.depth();
        (depth > T::MIN_DEPTH).then(|| {
            let parent_depth = depth.checked_sub(1).expect("parent depth is below child");
            <Self as RectGridCell>::grid_parent_at(self, parent_depth)
        })
    }

    fn children(self) -> impl ExactSizeIterator<Item = Self> {
        RectCell::children(self)
    }
}

impl<T: RectGridCell> GridCell for T {
    const MIN_DEPTH: u8 = T::MIN_DEPTH;
    const MAX_DEPTH: u8 = T::MAX_DEPTH;
    const DEPTH_NAME: &'static str = T::DEPTH_NAME;
    fn hash_key(self) -> u64 {
        <Self as RectGridCell>::hash_key(self)
    }

    fn token(self) -> String {
        <Self as RectGridCell>::grid_token(self)
    }

    fn center_point(self) -> Point {
        let bounds = self.bounds();
        let lon = f64::midpoint(bounds.minx(), bounds.maxx());
        let lat = f64::midpoint(bounds.miny(), bounds.maxy());
        Point::new(lon, lat).expect("rect cell centers are finite")
    }

    fn boundary_shape(self) -> crate::error::Result<Shape> {
        let bounds = self.bounds();
        let frame = GeographicFrame::crs84();
        crate::geometry::geographic_bounds_to_shape(bounds, frame)
    }

    fn area_m2(self) -> f64 {
        geographic::lonlat_rect_area_m2(self.bounds())
    }

    fn neighbors(self) -> Vec<Self> {
        <Self as RectGridCell>::grid_neighbors(self)
    }

    fn parent_at(self, depth: u8) -> Option<Self> {
        (depth <= self.depth()).then(|| <Self as RectGridCell>::grid_parent_at(self, depth))
    }

    fn pickle_arg(self) -> CellPickleArg {
        <Self as RectGridCell>::pickle_arg(self)
    }
}

impl RectGridCell for Geohash {
    const MIN_DEPTH: u8 = 1;
    const MAX_DEPTH: u8 = GEOHASH_MAX_PRECISION;
    const DEPTH_NAME: &'static str = "precision";

    fn hash_key(self) -> u64 {
        self.identity_key()
    }

    fn range_min(self) -> u64 {
        self.bits
    }

    fn range_max(self) -> u64 {
        self.bits | (u64::MAX >> (5 * u32::from(self.precision)))
    }

    fn grid_token(self) -> String {
        Self::token(self)
    }

    fn grid_neighbors(self) -> Vec<Self> {
        Self::neighbors(self)
    }

    fn grid_parent_at(self, depth: u8) -> Self {
        self.parent_at(depth)
    }

    fn pickle_arg(self) -> CellPickleArg {
        CellPickleArg::Str(self.grid_token())
    }
}

impl RectGridCell for Tile {
    const MIN_DEPTH: u8 = 0;
    const MAX_DEPTH: u8 = TILE_MAX_ZOOM;
    const DEPTH_NAME: &'static str = "zoom";

    fn hash_key(self) -> u64 {
        self.id()
    }

    fn range_min(self) -> u64 {
        crate::curves::morton_interleave(self.x, self.y).wrapping_shl(tile_range_shift(self.z))
    }

    fn range_max(self) -> u64 {
        let mask = 1_u64.wrapping_shl(tile_range_shift(self.z)).wrapping_sub(1);
        <Self as RectGridCell>::range_min(self) | mask
    }

    fn grid_token(self) -> String {
        self.quadkey()
    }

    fn grid_neighbors(self) -> Vec<Self> {
        Self::neighbors(self)
    }

    fn grid_parent_at(self, depth: u8) -> Self {
        self.parent_at(depth)
    }

    fn pickle_arg(self) -> CellPickleArg {
        CellPickleArg::U64(self.id())
    }
}

fn tile_range_shift(zoom: u8) -> u32 {
    58_u32
        .checked_sub(
            u32::from(zoom)
                .checked_mul(2)
                .expect("Tile zoom fits the packed Morton key"),
        )
        .expect("Tile zoom fits the packed Morton key")
}
