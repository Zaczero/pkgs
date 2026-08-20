#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Shared `GridCell` trait for the four DGGS kernels (H3, S2, geohash, tile).
//!
//! Rectangular grids blanket-implement from [`RectGridCell`]; H3 and S2 hand-implement
//! in `src/py/cells/{h3,s2}/` where their PyO3-facing kernels live.

use crate::geometry::{Point, Shape};
use crate::grid::UncompactBudgetExceeded;
use crate::grid::coverer::RectCell;

/// Argument carried by a cell's `__reduce__` tuple.
pub(crate) enum CellPickleArg {
    U64(u64),
    Str(String),
}

/// One cell of a discrete global grid — the kernel-side contract shared by all
/// four systems.
pub(crate) trait GridCell: Copy + Eq + Ord + Send + Sync {
    const MIN_DEPTH: u8;
    const MAX_DEPTH: u8;
    const DEPTH_NAME: &'static str;

    /// Children per cell per level for grids with a uniform hierarchy (S2 and
    /// tiles quadtree = 4, geohash base-32 = 32). H3 is non-uniform — pentagons
    /// have one fewer child — so it leaves this at its nominal hexagon value
    /// (7) and overrides [`GridCell::descendant_count`] with an exact count.
    const BRANCHING: u64;

    fn depth(self) -> u8;
    fn hash_key(self) -> u64;
    fn token(self) -> String;
    fn center_point(self) -> Point;
    fn boundary_shape(self) -> Shape;
    fn area_m2(self) -> f64;
    fn neighbors(self) -> Vec<Self>;
    fn parent_at(self, depth: u8) -> Option<Self>;
    fn children_to(self, depth: u8) -> std::result::Result<Vec<Self>, UncompactBudgetExceeded>;
    fn contains_cell(self, other: Self) -> bool;
    fn pickle_arg(self) -> CellPickleArg;

    /// Exact number of descendants `target_depth` levels finer, computed
    /// closed-form without materializing them. `target_depth == self.depth()`
    /// counts the cell itself (1). Callers must pass a validated
    /// `target_depth >= self.depth()` within the grid's range.
    fn descendant_count(self, target_depth: u8) -> u64 {
        Self::BRANCHING.pow(u32::from(target_depth - self.depth()))
    }
}

/// Rectangular lon/lat grid cells — per-system metadata plus hierarchy hooks
/// that [`GridCell`] blanket-implements from [`RectCell`]'s bounds geometry.
pub(crate) trait RectGridCell: RectCell + Send + Sync {
    const MIN_DEPTH: u8;
    const MAX_DEPTH: u8;
    const DEPTH_NAME: &'static str;
    /// Uniform branching factor — see [`GridCell::BRANCHING`].
    const BRANCHING: u64;

    fn hash_key(self) -> u64;
    fn grid_token(self) -> String;
    fn grid_neighbors(self) -> Vec<Self>;
    fn grid_parent_at(self, depth: u8) -> Self;
    fn grid_uncompact(self, depth: u8) -> std::result::Result<Vec<Self>, UncompactBudgetExceeded>;
    fn pickle_arg(self) -> CellPickleArg;
}

impl<T: RectGridCell> GridCell for T {
    const MIN_DEPTH: u8 = T::MIN_DEPTH;
    const MAX_DEPTH: u8 = T::MAX_DEPTH;
    const DEPTH_NAME: &'static str = T::DEPTH_NAME;
    const BRANCHING: u64 = <T as RectGridCell>::BRANCHING;

    fn depth(self) -> u8 {
        RectCell::depth(self)
    }

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

    fn neighbors(self) -> Vec<Self> {
        <Self as RectGridCell>::grid_neighbors(self)
    }

    fn parent_at(self, depth: u8) -> Option<Self> {
        (depth <= self.depth()).then(|| <Self as RectGridCell>::grid_parent_at(self, depth))
    }

    fn children_to(self, depth: u8) -> std::result::Result<Vec<Self>, UncompactBudgetExceeded> {
        <Self as RectGridCell>::grid_uncompact(self, depth)
    }

    fn contains_cell(self, other: Self) -> bool {
        other.depth() >= self.depth()
            && <Self as RectGridCell>::grid_parent_at(other, self.depth()) == self
    }

    fn pickle_arg(self) -> CellPickleArg {
        <Self as RectGridCell>::pickle_arg(self)
    }
}

impl RectGridCell for crate::grid::geohash::Geohash {
    const MIN_DEPTH: u8 = 1;
    const MAX_DEPTH: u8 = crate::grid::geohash::GEOHASH_MAX_PRECISION;
    const DEPTH_NAME: &'static str = "precision";
    // Geohash refines base-32: each character adds five bits → 32 children.
    const BRANCHING: u64 = 32;

    fn hash_key(self) -> u64 {
        self.identity_key()
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

    fn grid_uncompact(self, depth: u8) -> std::result::Result<Vec<Self>, UncompactBudgetExceeded> {
        crate::grid::geohash::uncompact(&[self], depth)
    }

    fn pickle_arg(self) -> CellPickleArg {
        CellPickleArg::Str(self.grid_token())
    }
}

impl RectGridCell for crate::grid::tile::Tile {
    const MIN_DEPTH: u8 = 0;
    const MAX_DEPTH: u8 = crate::grid::tile::TILE_MAX_ZOOM;
    const DEPTH_NAME: &'static str = "zoom";
    // The web-mercator quadtree splits every tile into four at the next zoom.
    const BRANCHING: u64 = 4;

    fn hash_key(self) -> u64 {
        self.id()
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

    fn grid_uncompact(self, depth: u8) -> std::result::Result<Vec<Self>, UncompactBudgetExceeded> {
        crate::grid::cell_set::uncompact(&[self], depth)
    }

    fn pickle_arg(self) -> CellPickleArg {
        CellPickleArg::U64(self.id())
    }
}
