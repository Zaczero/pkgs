#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Discrete global grid-system kernels: rectangular geohash + XYZ web-mercator
//! tiles (this module) and the spherical S2 core ([`s2`]). H3 is the external
//! `h3o` crate.
//!
//! Pure cell math (ids, tokens, bounds, hierarchy, neighbors) shared by the
//! top-level `geohash_*`/`tile_*`/`s2_*` functions and the grid
//! coverages; the Python surfaces live under `src/py/cells/`.

use thiserror::Error;

/// Default cell-output budget for covering factories when `max_cells` is
/// omitted. Secure-by-default against runaway coverings of untrusted boxes;
/// callers pass a larger value or `max_cells=None` (unlimited) for deliberate
/// large work. Non-coverage collectors (`grid_disk`, set algebra, …) still use
/// this as their fixed hard limit.
pub(crate) const GRID_MAX_CELLS: usize = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("{operation} would exceed the {limit}-cell limit")]
pub(crate) struct CellLimitExceeded {
    operation: &'static str,
    limit: usize,
}

impl CellLimitExceeded {
    pub(crate) const fn new(operation: &'static str) -> Self {
        Self {
            operation,
            limit: GRID_MAX_CELLS,
        }
    }
}

/// Bounded flat collector for operations that can expand a small grid input.
pub(crate) struct CellCollector<T> {
    operation: &'static str,
    values: Vec<T>,
}

impl<T> CellCollector<T> {
    pub(crate) const fn new(operation: &'static str) -> Self {
        Self {
            operation,
            values: Vec::new(),
        }
    }

    pub(crate) fn with_estimate(operation: &'static str, estimate: usize) -> Self {
        Self {
            operation,
            values: Vec::with_capacity(estimate.min(GRID_MAX_CELLS)),
        }
    }

    pub(crate) fn push(&mut self, value: T) -> Result<(), CellLimitExceeded> {
        if self.values.len() == GRID_MAX_CELLS {
            return Err(CellLimitExceeded::new(self.operation));
        }
        self.values.push(value);
        Ok(())
    }

    pub(crate) fn extend(
        &mut self,
        values: impl IntoIterator<Item = T>,
    ) -> Result<(), CellLimitExceeded> {
        for value in values {
            self.push(value)?;
        }
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.values.clear();
    }

    pub(crate) fn into_vec(self) -> Vec<T> {
        self.values
    }

    pub(crate) const fn len(&self) -> usize {
        self.values.len()
    }
}

/// Direct CSR builder for ragged cell fan-outs. Rows stream into one bounded
/// flat column instead of first allocating `Vec<Vec<_>>`.
pub(crate) struct CellGroupsBuilder<T> {
    cells: CellCollector<T>,
    offsets: Vec<i64>,
}

impl<T> CellGroupsBuilder<T> {
    pub(crate) fn new(operation: &'static str) -> Self {
        Self {
            cells: CellCollector::new(operation),
            offsets: vec![0],
        }
    }

    pub(crate) fn push_row(
        &mut self,
        values: impl IntoIterator<Item = T>,
    ) -> Result<(), CellLimitExceeded> {
        self.cells.extend(values)?;
        self.offsets.push(self.cells.len() as i64);
        Ok(())
    }

    pub(crate) fn finish(self) -> (Vec<T>, Vec<i64>) {
        (self.cells.into_vec(), self.offsets)
    }
}

/// Maximum number of cells an `uncompact` may materialize (checked from an
/// estimate before allocation). Aliases the shared [`GRID_MAX_CELLS`] budget.
pub(crate) const UNCOMPACT_MAX_CELLS: usize = GRID_MAX_CELLS;

/// A covering whose running cell count would exceed the caller-supplied
/// `max_cells` budget. The error message names `max_cells` so callers learn
/// the override knob (`None` = unlimited).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error(
    "covering would exceed max_cells={limit}; raise max_cells or pass max_cells=None for unlimited"
)]
pub(crate) struct CoverBudgetExceeded {
    pub limit: usize,
}

impl CoverBudgetExceeded {
    pub(crate) const fn new(limit: usize) -> Self {
        Self { limit }
    }
}

/// Reject a covering once its running cell count crosses `max_cells`.
///
/// `None` = unlimited (adult escape hatch; bounded only by memory). Checked at
/// every cell emission so the kernel fails before the next allocation rather
/// than after flooding.
pub(crate) const fn ensure_cover_budget(
    produced: usize,
    max_cells: Option<usize>,
) -> Result<(), CoverBudgetExceeded> {
    match max_cells {
        Some(limit) if produced > limit => Err(CoverBudgetExceeded::new(limit)),
        _ => Ok(()),
    }
}

/// `uncompact` budget exceeded — estimated output cell count is too large.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("uncompact would produce {estimated} cells, exceeding the limit of {limit}")]
pub(crate) struct UncompactBudgetExceeded {
    pub estimated: usize,
    limit: usize,
}

impl UncompactBudgetExceeded {
    pub(crate) const fn new(estimated: usize) -> Self {
        Self {
            estimated,
            limit: UNCOMPACT_MAX_CELLS,
        }
    }
}

/// Reject `uncompact` when the estimated output exceeds
/// [`UNCOMPACT_MAX_CELLS`].
pub(crate) const fn ensure_uncompact_budget(
    estimated: usize,
) -> Result<(), UncompactBudgetExceeded> {
    if estimated > UNCOMPACT_MAX_CELLS {
        Err(UncompactBudgetExceeded::new(estimated))
    } else {
        Ok(())
    }
}

pub(crate) mod cell;
pub(crate) mod cell_set;
pub(crate) mod coverer;
pub(crate) mod geohash;
pub(crate) mod s2;
pub(crate) mod tile;

/// One-step toroidal wrap of `value` into `0..cells` for grid neighbour math.
/// Identical to `value.rem_euclid(cells)` for a single neighbour step
/// (`|delta| <= cells`, the only use), but branchless — the compiler lowers it
/// to `cmov`, avoiding the `idiv` that `rem_euclid` emits (hot in `neighbors`).
/// Shared by both [`geohash`] and [`tile`] so the wrap is defined once.
pub(crate) const fn wrap_axis(value: i64, cells: i64) -> i64 {
    if value < 0 {
        value + cells
    } else if value >= cells {
        value - cells
    } else {
        value
    }
}

/// The distinct cells of the 3×3 ring around `center`, walked `dx` in
/// `[-1, 0, 1]` within each `dy` of `dy_order` (the only axis whose polarity
/// differs between grids: lat grows north for [`geohash`], tile rows grow
/// south). `neighbor` returns the offset cell or `None` past an edge; cells
/// equal to `center` or already collected (antimeridian wrap can collide at low
/// resolution) are dropped, so the output is row-major and de-duplicated.
/// Shared by both [`geohash`] and [`tile`] so the ring walk is defined once.
pub(crate) fn ring_neighbors<C: Copy + PartialEq>(
    center: C,
    dy_order: [i64; 3],
    neighbor: impl Fn(C, i64, i64) -> Option<C>,
) -> Vec<C> {
    let mut cells = Vec::with_capacity(8);
    for dy in dy_order {
        for dx in [-1, 0, 1] {
            if dx == 0 && dy == 0 {
                continue;
            }
            if let Some(cell) = neighbor(center, dx, dy)
                && cell != center
                && !cells.contains(&cell)
            {
                cells.push(cell);
            }
        }
    }
    cells
}

#[cfg(test)]
mod budget_tests {
    use super::*;

    #[test]
    fn flat_collector_accepts_exact_limit_and_rejects_next_cell() {
        let mut cells = CellCollector::with_estimate("test", GRID_MAX_CELLS);
        cells
            .extend(std::iter::repeat_n(0_u8, GRID_MAX_CELLS))
            .unwrap();
        assert_eq!(cells.len(), GRID_MAX_CELLS);
        cells.push(1).unwrap_err();
    }

    #[test]
    fn groups_builder_applies_one_cumulative_limit() {
        let mut groups = CellGroupsBuilder::new("test groups");
        groups
            .push_row(std::iter::repeat_n(0_u8, GRID_MAX_CELLS - 1))
            .unwrap();
        groups.push_row([1]).unwrap();
        groups.push_row([2]).unwrap_err();
        let (cells, offsets) = groups.finish();
        assert_eq!(cells.len(), GRID_MAX_CELLS);
        assert_eq!(offsets, [
            0,
            (GRID_MAX_CELLS - 1) as i64,
            GRID_MAX_CELLS as i64
        ]);
    }
}
