use h3o::{CellIndex, LatLng, Resolution};

use crate::H3_MAX_RESOLUTION;
use crate::geometry::{Point, Shape};
use crate::grid::UncompactBudgetExceeded;
use crate::grid::cell::{CellPickleArg, GridCell};
use crate::py::cells::h3::cell::h3_grid_ring_cells;
use crate::py::cells::h3::tile::h3_cell_shape;

fn h3_kernel_area_m2(cell: CellIndex) -> f64 {
    cell.area_m2()
}

impl GridCell for CellIndex {
    const MIN_DEPTH: u8 = 0;
    const MAX_DEPTH: u8 = H3_MAX_RESOLUTION;
    const DEPTH_NAME: &'static str = "resolution";
    // Nominal hexagon fan-out; `descendant_count` below is exact (pentagons
    // have one fewer child), so this value is never used for H3 counting.
    const BRANCHING: u64 = 7;

    fn depth(self) -> u8 {
        u8::from(self.resolution())
    }

    fn descendant_count(self, target_depth: u8) -> u64 {
        let resolution = Resolution::try_from(target_depth)
            .expect("resolution validated before descendant_count");
        self.children_count(resolution)
    }

    fn hash_key(self) -> u64 {
        u64::from(self)
    }

    fn token(self) -> String {
        self.to_string()
    }

    fn center_point(self) -> Point {
        let latlng = LatLng::from(self);
        Point::new_unchecked_xy(latlng.lng(), latlng.lat())
    }

    fn boundary_shape(self) -> Shape {
        h3_cell_shape(self)
    }

    fn area_m2(self) -> f64 {
        h3_kernel_area_m2(self)
    }

    fn neighbors(self) -> Vec<Self> {
        h3_grid_ring_cells(self, 1)
    }

    fn parent_at(self, depth: u8) -> Option<Self> {
        let resolution = Resolution::try_from(depth).ok()?;
        self.parent(resolution)
    }

    fn children_to(self, depth: u8) -> std::result::Result<Vec<Self>, UncompactBudgetExceeded> {
        let resolution = Resolution::try_from(depth).expect("depth validated before children_to");
        let estimated = usize::try_from(self.children_count(resolution)).unwrap_or(usize::MAX);
        crate::grid::ensure_uncompact_budget(estimated)?;
        let mut children = Vec::with_capacity(estimated.min(1 << 20));
        children.extend(self.children(resolution));
        Ok(children)
    }

    fn contains_cell(self, other: Self) -> bool {
        other
            .parent(self.resolution())
            .is_some_and(|parent| parent == self)
    }

    fn pickle_arg(self) -> CellPickleArg {
        CellPickleArg::U64(u64::from(self))
    }
}
