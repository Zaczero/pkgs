//! Discrete global grid-system kernels: rectangular geohash + XYZ web-mercator
//! tiles (this module) and the spherical S2 core ([`s2`]). H3 is the external
//! `h3o` crate.
//!
//! Pure cell math (ids, tokens, bounds, hierarchy, neighbors) shared by the
//! top-level `geohash_*`/`tile_*`/`s2_*` functions and the grid
//! coverages; the Python surfaces live under `src/py/cells/`.

use thiserror::Error;

use crate::geographic::lonlat as geographic;

/// Default cell-output budget for covering factories when `max_cells` is
/// omitted. Secure-by-default against runaway coverings of untrusted boxes;
/// callers pass a larger value or `max_cells=None` (unlimited) for deliberate
/// large work. H3 traversal methods use the same default and escape hatch.
pub(crate) const GRID_MAX_CELLS: i64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error(
    "covering would exceed max_cells={limit}; raise max_cells or pass max_cells=None for unlimited"
)]
pub(crate) struct CoverBudgetExceeded {
    pub limit: usize,
}

#[derive(Debug, Error)]
pub(crate) enum CoverError {
    #[error(transparent)]
    Preparation(#[from] crate::error::Error),
    #[error(transparent)]
    Budget(#[from] CoverBudgetExceeded),
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

pub(crate) mod affine_source;
pub(crate) mod cell;
pub(crate) mod cell_set;
pub(crate) mod coverer;
pub(crate) mod geohash;
pub(crate) mod h3_coverer;
pub(crate) mod s2;
pub(crate) mod spherical_arc;
pub(crate) mod tile;

/// Canonical degree spelling for one physical longitude/latitude location.
///
/// Grid identifiers are half-open in longitude, so the antimeridian owner is
/// always `-180`. At either pole longitude has no physical meaning; using the
/// same representative prevents backend projection roundoff from choosing an
/// identifier from the caller's arbitrary pole spelling.
pub(crate) fn canonical_lonlat(lon: f64, lat: f64) -> (f64, f64) {
    let lat = geographic::normalize_accepted_latitude(lat);
    let mut lon = lon % 360.0;
    if lon < -180.0 {
        lon += 360.0;
    } else if lon >= 180.0 {
        lon -= 360.0;
    }
    if lon == 0.0 {
        lon = 0.0;
    }
    if geographic::normalized_geographic_pole(lat).is_some() {
        lon = -180.0;
    }
    (lon, lat)
}

/// One-step toroidal wrap of `value` into `0..cells` for grid neighbour math.
/// Identical to `value.rem_euclid(cells)` for a single neighbour step
/// (`|delta| <= cells`, the only use), but branchless — the compiler lowers it
/// to `cmov`, avoiding the `idiv` that `rem_euclid` emits (hot in `neighbors`).
/// Shared by both [`geohash`] and [`tile`] so the wrap is defined once.
pub(crate) const fn wrap_axis(value: i64, cells: i64) -> i64 {
    if value < 0 {
        value.wrapping_add(cells)
    } else if value >= cells {
        value.wrapping_sub(cells)
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
    fn h3_periodic_seam_and_poles_have_canonical_owners() {
        let resolution = h3o::Resolution::Nine;
        let cell = |lon, lat| {
            let (lon, lat) = canonical_lonlat(lon, lat);
            h3o::LatLng::new(lat, lon).unwrap().to_cell(resolution)
        };
        for lat in [-45.0, 0.0, 45.0] {
            let owner = cell(-180.0, lat);
            for lon in [180.0, 540.0, -540.0] {
                assert_eq!(cell(lon, lat), owner);
            }
            assert_eq!(cell((-180.0_f64).next_up(), lat), owner);
            let east_neighbor = 180.0_f64.next_down();
            assert_eq!(canonical_lonlat(east_neighbor, lat).0, east_neighbor);
            let _ = cell(east_neighbor, lat);
        }
        for lat in [-90.0, 90.0] {
            let owner = cell(-180.0, lat);
            for lon in [-179.0, 0.0, 73.0, 180.0, 540.0] {
                assert_eq!(cell(lon, lat), owner);
            }
        }
    }
}
