use crate::geometry::{Point, Shape};
use crate::grid::UncompactBudgetExceeded;
use crate::grid::cell::{CellPickleArg, GridCell};
use crate::grid::s2::cell::Cell as S2GeomCell;
use crate::grid::s2::cell_set;
use crate::grid::s2::cellid::CellId;
use crate::grid::s2::projection::MAX_LEVEL as S2_MAX_LEVEL;

impl GridCell for CellId {
    const MIN_DEPTH: u8 = 0;
    const MAX_DEPTH: u8 = S2_MAX_LEVEL;
    const DEPTH_NAME: &'static str = "level";
    // Each S2 cell subdivides into four at the next level.
    const BRANCHING: u64 = 4;

    fn depth(self) -> u8 {
        self.level()
    }

    fn hash_key(self) -> u64 {
        self.raw()
    }

    fn token(self) -> String {
        Self::token(self)
    }

    fn center_point(self) -> Point {
        S2GeomCell::from_id(self).center_lonlat()
    }

    fn boundary_shape(self) -> Shape {
        S2GeomCell::from_id(self).boundary_shape()
    }

    fn area_m2(self) -> f64 {
        S2GeomCell::from_id(self).area_m2()
    }

    fn neighbors(self) -> Vec<Self> {
        Self::edge_neighbors(self).to_vec()
    }

    fn parent_at(self, depth: u8) -> Option<Self> {
        Self::parent(self, depth)
    }

    fn children_to(self, depth: u8) -> std::result::Result<Vec<Self>, UncompactBudgetExceeded> {
        cell_set::uncompact(&[self], depth)
    }

    fn contains_cell(self, other: Self) -> bool {
        Self::contains(self, other)
    }

    fn pickle_arg(self) -> CellPickleArg {
        CellPickleArg::U64(self.raw())
    }
}
