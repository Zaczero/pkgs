#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Shared `GridCell` trait for the four DGGS kernels (H3, S2, geohash, tile).
//!
//! Rectangular grids blanket-implement from [`RectGridCell`]; H3 and S2 hand-implement
//! in `src/py/cells/{h3,s2}/` where their PyO3-facing kernels live.

use crate::geometry::{Point, Shape};
use crate::grid::coverer::RectCell;

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
    fn boundary_shape(self) -> Shape;
    fn area_m2(self) -> f64;
    fn neighbors(self) -> crate::error::Result<Vec<Self>>;
    fn parent_at(self, depth: u8) -> Option<Self>;
    fn children_to(self, depth: u8) -> crate::error::Result<Vec<Self>> {
        crate::grid::cell_set::uncompact(&[self], depth).map(|cells| cells.into_public_vec())
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
    fn grid_neighbors(self) -> crate::error::Result<Vec<Self>>;
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
        (self.depth() > T::MIN_DEPTH)
            .then(|| <Self as RectGridCell>::grid_parent_at(self, self.depth() - 1))
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

    fn boundary_shape(self) -> Shape {
        let bounds = self.bounds();
        crate::box_polygon(bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy())
            .map(Shape::Polygon)
            .expect("rect cell bounds are finite")
    }

    fn area_m2(self) -> f64 {
        crate::boundary::geographic::lonlat_rect_area_m2(self.bounds())
    }

    fn neighbors(self) -> crate::error::Result<Vec<Self>> {
        <Self as RectGridCell>::grid_neighbors(self)
    }

    fn parent_at(self, depth: u8) -> Option<Self> {
        (depth <= self.depth()).then(|| <Self as RectGridCell>::grid_parent_at(self, depth))
    }

    fn pickle_arg(self) -> CellPickleArg {
        <Self as RectGridCell>::pickle_arg(self)
    }
}

impl RectGridCell for crate::grid::geohash::Geohash {
    const MIN_DEPTH: u8 = 1;
    const MAX_DEPTH: u8 = crate::grid::geohash::GEOHASH_MAX_PRECISION;
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

    fn grid_neighbors(self) -> crate::error::Result<Vec<Self>> {
        Self::neighbors(self)
    }

    fn grid_parent_at(self, depth: u8) -> Self {
        self.parent_at(depth)
    }

    fn pickle_arg(self) -> CellPickleArg {
        CellPickleArg::Str(self.grid_token())
    }
}

impl RectGridCell for crate::grid::tile::Tile {
    const MIN_DEPTH: u8 = 0;
    const MAX_DEPTH: u8 = crate::grid::tile::TILE_MAX_ZOOM;
    const DEPTH_NAME: &'static str = "zoom";

    fn hash_key(self) -> u64 {
        self.id()
    }

    fn range_min(self) -> u64 {
        crate::curves::morton_interleave(self.x, self.y) << (58 - 2 * u32::from(self.z))
    }

    fn range_max(self) -> u64 {
        <Self as RectGridCell>::range_min(self) | ((1_u64 << (58 - 2 * u32::from(self.z))) - 1)
    }

    fn grid_token(self) -> String {
        self.quadkey()
    }

    fn grid_neighbors(self) -> crate::error::Result<Vec<Self>> {
        Self::neighbors(self)
    }

    fn grid_parent_at(self, depth: u8) -> Self {
        self.parent_at(depth)
    }

    fn pickle_arg(self) -> CellPickleArg {
        CellPickleArg::U64(self.id())
    }
}
