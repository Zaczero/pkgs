use crate::geometry::{Point, Shape};
use crate::grid::cell::{CellPickleArg, GridCell};
use crate::grid::s2::cell::{Cell as S2GeomCell, area_m2 as s2_area_m2};
use crate::grid::s2::cellid::CellId;
use crate::grid::s2::projection::MAX_LEVEL as S2_MAX_LEVEL;

impl GridCell for CellId {
    const MIN_DEPTH: u8 = 0;
    const MAX_DEPTH: u8 = S2_MAX_LEVEL;
    const DEPTH_NAME: &'static str = "level";
    fn hash_key(self) -> u64 {
        self.raw()
    }

    fn token(self) -> String {
        Self::token(self)
    }

    fn center_point(self) -> Point {
        S2GeomCell::from_id(self).center_lonlat()
    }

    fn boundary_shape(self) -> crate::error::Result<Shape> {
        S2GeomCell::from_id(self).boundary_shape()
    }

    fn area_m2(self) -> f64 {
        s2_area_m2(self)
    }

    fn neighbors(self) -> crate::error::Result<Vec<Self>> {
        let mut cells = Vec::new();
        crate::try_reserve_vec(&mut cells, 4)?;
        cells.extend(Self::edge_neighbors(self));
        Ok(cells)
    }

    fn parent_at(self, depth: u8) -> Option<Self> {
        Self::parent(self, depth)
    }

    fn pickle_arg(self) -> CellPickleArg {
        CellPickleArg::U64(self.raw())
    }
}
