use h3o::error::CompactionError;
use h3o::{CellIndex, Resolution};

use crate::py::cells::{Bound, GeometryError, H3_MAX_RESOLUTION, PyAny, PyResult, py_i64_required};
/// Compact H3 `cells` without merging coarser than `min_resolution`: cells at
/// or coarser than the floor pass through unchanged, complete sibling groups
/// merge recursively (mixed-resolution input is grouped per resolution and
/// merged finest-first, since the backend compactor requires homogeneous
/// input), and any merge result landing coarser than the floor is re-expanded
/// to it (an ancestor's expansion is exactly the sibling set that merged into
/// it, so the covered area is unchanged).
///
/// Default-floor input tries h3o directly (no one-key BTreeMap + pre-sort).
/// `DuplicateInput` retries on the already-sorted/deduped vector;
/// `HeterogeneousResolution` falls through to the multi-resolution path
/// without a second pre-scan.
pub(super) fn h3_compact_with_floor(
    mut cells: Vec<CellIndex>,
    min_resolution: Resolution,
) -> PyResult<Vec<CellIndex>> {
    // Fast path: default floor tries h3o first. Homogeneous input finishes
    // here; mixed input falls through with cells still unsorted (h3o rejects
    // before its sort).
    if min_resolution == Resolution::Zero {
        match CellIndex::compact(&mut cells) {
            Ok(()) => return Ok(cells),
            Err(CompactionError::DuplicateInput) => {
                // Sorted+deduped in place before the error — one retry.
                CellIndex::compact(&mut cells)
                    .map_err(|error| GeometryError::new_err(error.to_string()))?;
                return Ok(cells);
            },
            // Heterogeneous (and any future non-exhaustive variant): fall
            // through to the multi-resolution floor path. h3o rejects before
            // sorting, so `cells` is still the original order.
            Err(CompactionError::HeterogeneousResolution | _) => {},
        }
    }

    let (mut out, rest): (Vec<_>, Vec<_>) = cells
        .into_iter()
        .partition(|cell| cell.resolution() <= min_resolution);
    let mut buckets: std::collections::BTreeMap<Resolution, Vec<CellIndex>> =
        std::collections::BTreeMap::new();
    for cell in rest {
        buckets.entry(cell.resolution()).or_default().push(cell);
    }
    // Finest first: merged parents re-enter the next-coarser bucket so chains
    // of complete sibling groups keep collapsing.
    while let Some((resolution, mut bucket)) = buckets.pop_last() {
        // Merged parents re-entering a bucket can duplicate cells the caller
        // already supplied (e.g. `with_parents` output); compaction requires
        // unique cells.
        bucket.sort_unstable();
        bucket.dedup();
        CellIndex::compact(&mut bucket)
            .map_err(|error| GeometryError::new_err(error.to_string()))?;
        for cell in bucket {
            if cell.resolution() < resolution && cell.resolution() > min_resolution {
                buckets.entry(cell.resolution()).or_default().push(cell);
            } else if cell.resolution() < min_resolution {
                out.extend(cell.children(min_resolution));
            } else {
                out.push(cell);
            }
        }
    }
    Ok(out)
}

pub(super) fn parse_h3_grid_k(value: &Bound<'_, PyAny>) -> PyResult<u32> {
    let k = py_i64_required("H3 grid distance", value)?;
    if k < 0 {
        return Err(GeometryError::new_err(
            "H3 grid distance must be non-negative",
        ));
    }
    u32::try_from(k).map_err(|_| GeometryError::new_err("H3 grid distance is too large"))
}

pub(super) fn h3_floor(min_resolution: i64) -> PyResult<u8> {
    super::super::checked_depth(
        min_resolution,
        "H3 min_resolution",
        "min_resolution",
        0,
        i64::from(H3_MAX_RESOLUTION),
    )
}
