use h3o::{CellIndex, LatLng, Resolution};

use crate::H3_MAX_RESOLUTION;
use crate::geometry::{Point, Shape};
use crate::grid::cell::{CellPickleArg, GridCell, HierarchicalCell};
use crate::py::cells::h3::cell::h3_immediate_neighbors;
use crate::py::cells::h3::tile::h3_cell_shape;

fn h3_kernel_area_m2(cell: CellIndex) -> f64 {
    cell.area_m2()
}

struct H3ImmediateChildren {
    cell: CellIndex,
    finer: Option<Resolution>,
    position: u64,
    remaining: u64,
}

impl Iterator for H3ImmediateChildren {
    type Item = CellIndex;

    fn next(&mut self) -> Option<Self::Item> {
        let finer = self.finer?;
        if self.remaining == 0 {
            return None;
        }
        let child = self.cell.child_at(self.position, finer)?;
        self.position += 1;
        self.remaining -= 1;
        Some(child)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::try_from(self.remaining).unwrap_or(usize::MAX);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for H3ImmediateChildren {
    fn len(&self) -> usize {
        usize::try_from(self.remaining).unwrap_or(usize::MAX)
    }
}

fn h3_unused_digit_mask(resolution: u8) -> u64 {
    let unused = 3 * (15 - u32::from(resolution));
    if unused == 0 { 0 } else { (1 << unused) - 1 }
}

impl HierarchicalCell for CellIndex {
    fn depth(self) -> u8 {
        self.resolution().into()
    }

    fn max_depth() -> u8 {
        H3_MAX_RESOLUTION
    }

    fn public_key(self) -> u64 {
        u64::from(self)
    }

    fn range_min(self) -> u64 {
        let depth = self.depth();
        let raw = u64::from(self);
        raw & !h3_unused_digit_mask(depth) & ((1 << 52) - 1)
    }

    fn range_max(self) -> u64 {
        self.range_min() | h3_unused_digit_mask(self.depth())
    }

    fn parent(self) -> Option<Self> {
        let depth = self.depth();
        (depth > 0).then(|| {
            self.parent(Resolution::try_from(depth - 1).expect("coarser resolution"))
                .expect("non-zero resolution has a parent")
        })
    }

    fn children(self) -> impl ExactSizeIterator<Item = Self> {
        let depth = self.depth();
        let finer = Resolution::try_from(depth + 1).ok();
        let remaining = finer.map_or(0, |resolution| self.children_count(resolution));
        H3ImmediateChildren {
            cell: self,
            finer,
            position: 0,
            remaining,
        }
    }

    fn descendant_count(self, target_depth: u8) -> u64 {
        self.children_count(Resolution::try_from(target_depth).expect("validated depth"))
    }
}

impl GridCell for CellIndex {
    const MIN_DEPTH: u8 = 0;
    const MAX_DEPTH: u8 = H3_MAX_RESOLUTION;
    const DEPTH_NAME: &'static str = "resolution";
    fn hash_key(self) -> u64 {
        u64::from(self)
    }

    fn token(self) -> String {
        self.to_string()
    }

    fn center_point(self) -> Point {
        let latlng = LatLng::from(self);
        Point::new_raw_xy(latlng.lng(), latlng.lat())
    }

    fn boundary_shape(self) -> Shape {
        h3_cell_shape(self)
    }

    fn area_m2(self) -> f64 {
        h3_kernel_area_m2(self)
    }

    fn neighbors(self) -> Vec<Self> {
        h3_immediate_neighbors(self)
    }

    fn parent_at(self, depth: u8) -> Option<Self> {
        let resolution = Resolution::try_from(depth).ok()?;
        self.parent(resolution)
    }

    fn pickle_arg(self) -> CellPickleArg {
        CellPickleArg::U64(u64::from(self))
    }
}
