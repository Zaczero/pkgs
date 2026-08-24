use h3o::error::CompactionError;
use h3o::{CellIndex, Resolution};

use crate::py::cells::{Bound, GeometryError, PyAny, PyResult, py_i64_required};
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

/// Compact validated H3 ids without wrapping every row in `CellIndex`.
///
/// `CellArray` ids have already crossed the H3 validation boundary. The packed
/// path mirrors h3o's sorted cursor algorithm over the stable H3 wire layout;
/// mixed-resolution input falls back to the CellIndex path above.
pub(in crate::py::cells) fn h3_compact_ids_with_floor(
    mut ids: Vec<u64>,
    min_resolution: Resolution,
) -> PyResult<Vec<u64>> {
    let Some(&first) = ids.first() else {
        return Ok(ids);
    };
    let resolution = ((first >> 52) & 0x0F) as u8;
    if ids
        .iter()
        .any(|&id| ((id >> 52) & 0x0F) as u8 != resolution)
    {
        let cells = ids
            .into_iter()
            .map(|id| CellIndex::try_from(id).expect("CellArray ids are validated"))
            .collect();
        return h3_compact_with_floor(cells, min_resolution)
            .map(|cells| cells.into_iter().map(u64::from).collect());
    }

    ids.sort_unstable();
    ids.dedup();
    if resolution == 0 || resolution <= u8::from(min_resolution) {
        return Ok(ids);
    }

    let mut read = 0;
    let mut write = 0;
    while read < ids.len() {
        let cell = ids[read];
        let mut merged = false;
        if direction(cell, resolution) == 0 {
            for target in u8::from(min_resolution)..resolution {
                let parent = parent(cell, target);
                let count = children_count(parent, target, resolution);
                if ids
                    .get(read + count - 1)
                    .is_some_and(|&last| last == last_sibling(cell, target, resolution))
                {
                    read += count;
                    ids[write] = parent;
                    write += 1;
                    merged = true;
                    break;
                }
            }
        }
        if !merged {
            ids[write] = cell;
            write += 1;
            read += 1;
        }
    }
    ids.truncate(write);
    Ok(ids)
}

const RESOLUTION_MASK: u64 = 0x00F0_0000_0000_0000;
const BASE_PENTAGONS: u128 = 0x0020_0802_0008_0100_8402_0040_0100_4010;
const LAST_DIRECTION_BITS: u64 = 0x0000_1B6D_B6DB_6DB6;
const HEXAGON_CHILDREN_COUNTS: [usize; 16] = [
    1,
    7,
    49,
    343,
    2_401,
    16_807,
    117_649,
    823_543,
    5_764_801,
    40_353_607,
    282_475_249,
    1_977_326_743,
    13_841_287_201,
    96_889_010_407,
    678_223_072_849,
    4_747_561_509_943,
];
const PENTAGON_CHILDREN_COUNTS: [usize; 16] = [
    1,
    6,
    41,
    286,
    2_001,
    14_006,
    98_041,
    686_286,
    4_804_001,
    33_628_006,
    235_396_041,
    1_647_772_286,
    11_534_406_001,
    80_740_842_006,
    565_185_894_041,
    3_956_301_258_286,
];

#[inline]
fn direction(id: u64, resolution: u8) -> u8 {
    let offset = usize::from(15 - resolution) * 3;
    ((id >> offset) & 0b111) as u8
}

#[inline]
fn parent(id: u64, resolution: u8) -> u64 {
    let offset = usize::from(15 - resolution) * 3;
    let unused = if offset == 0 {
        0
    } else {
        (1_u64 << offset) - 1
    };
    (id & !RESOLUTION_MASK | (u64::from(resolution) << 52)) | unused
}

#[inline]
fn is_pentagon(id: u64, resolution: u8) -> bool {
    let base = (id >> 45) & 0x7F;
    if BASE_PENTAGONS & (1_u128 << base) == 0 {
        return false;
    }
    if resolution == 0 {
        return true;
    }
    let offset = usize::from(15 - resolution) * 3;
    let width = usize::from(resolution) * 3;
    (id >> offset) & ((1_u64 << width) - 1) == 0
}

#[inline]
fn children_count(parent: u64, parent_resolution: u8, resolution: u8) -> usize {
    let delta = usize::from(resolution - parent_resolution);
    if is_pentagon(parent, parent_resolution) {
        PENTAGON_CHILDREN_COUNTS[delta]
    } else {
        HEXAGON_CHILDREN_COUNTS[delta]
    }
}

#[inline]
fn last_sibling(cell: u64, target: u8, resolution: u8) -> u64 {
    let diff = usize::from(resolution - target) * 3;
    let mask = (1_u64 << diff) - 1;
    let offset = usize::from(15 - resolution) * 3;
    (cell & !(mask << offset)) | ((LAST_DIRECTION_BITS & mask) << offset)
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

#[cfg(test)]
mod tests {
    use h3o::{CellIndex, Resolution};

    use super::{h3_compact_ids_with_floor, h3_compact_with_floor};

    fn children(id: u64, resolution: u8) -> Vec<u64> {
        CellIndex::try_from(id)
            .expect("cell")
            .children(Resolution::try_from(resolution).expect("resolution"))
            .map(u64::from)
            .collect()
    }

    #[test]
    fn packed_compaction_matches_backend_for_complete_and_incomplete_groups() {
        let parent = 0x85283473FFFFFFF;
        let complete = children(parent, 6);
        let packed =
            h3_compact_ids_with_floor(complete.clone(), Resolution::Zero).expect("compact");
        let backend = h3_compact_with_floor(
            complete
                .iter()
                .copied()
                .map(|id| CellIndex::try_from(id).expect("cell"))
                .collect(),
            Resolution::Zero,
        )
        .expect("compact")
        .into_iter()
        .map(u64::from)
        .collect::<Vec<_>>();
        assert_eq!(packed, backend);

        let incomplete = children(parent, 6);
        let incomplete = &incomplete[..incomplete.len() - 1];
        let packed =
            h3_compact_ids_with_floor(incomplete.to_vec(), Resolution::Zero).expect("compact");
        assert_eq!(packed.len(), incomplete.len());
        assert!(packed.iter().all(|&id| id != parent));
    }

    #[test]
    fn packed_compaction_matches_pentagons_and_floor() {
        let base_cells = CellIndex::base_cells().map(u64::from).collect::<Vec<_>>();
        let ids = base_cells
            .iter()
            .flat_map(|&id| children(id, 2))
            .collect::<Vec<_>>();
        let packed = h3_compact_ids_with_floor(ids.clone(), Resolution::Zero).expect("compact");
        let backend = h3_compact_with_floor(
            ids.into_iter()
                .map(|id| CellIndex::try_from(id).expect("cell"))
                .collect(),
            Resolution::Zero,
        )
        .expect("compact")
        .into_iter()
        .map(u64::from)
        .collect::<Vec<_>>();
        assert_eq!(packed, backend);

        let parent = 0x85283473FFFFFFF;
        let complete = children(parent, 6);
        assert_eq!(
            h3_compact_ids_with_floor(complete.clone(), Resolution::Five).expect("compact"),
            vec![parent]
        );
        assert_eq!(
            h3_compact_ids_with_floor(complete, Resolution::Six)
                .expect("compact")
                .len(),
            7
        );
    }
}
