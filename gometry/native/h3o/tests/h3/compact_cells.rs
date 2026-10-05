use h3o::{CellIndex, Resolution};

use super::h3api;

fn assert_compact(mut cells: Vec<CellIndex>) {
    let reference = h3api::compact_cells(&cells).map(|mut cells| {
        cells.sort_unstable();
        cells
    });
    let result = CellIndex::compact(&mut cells).ok().map(|()| {
        cells.sort_unstable();
        cells
    });

    assert_eq!(result, reference);
}

#[test]
fn grid_disk() {
    let index = CellIndex::try_from(0x89283470C27FFFF).expect("cell index");
    let cells = index.grid_disk::<Vec<_>>(9);

    assert_compact(cells);
}

#[test]
fn res0() {
    let cells = CellIndex::base_cells().collect::<Vec<_>>();

    assert_compact(cells);
}

#[test]
fn res0_children() {
    let index = CellIndex::try_from(0x8001FFFFFFFFFFF).expect("cell index");
    let cells = index.children(Resolution::One).collect::<Vec<_>>();

    assert_compact(cells);
}

#[test]
fn children() {
    let index = CellIndex::try_from(0x89283470803FFFF).expect("cell index");
    let cells = index.children(Resolution::Twelve).collect::<Vec<_>>();

    assert_compact(cells);
}

#[test]
fn uncompactable() {
    let cells = [0x89283470803FFFF, 0x8928347081BFFFF, 0x8928347080BFFFF]
        .into_iter()
        .map(|hex| CellIndex::try_from(hex).expect("cell index"))
        .collect();

    assert_compact(cells);
}

#[test]
fn duplicate() {
    let cells = vec![CellIndex::try_from(0x8500924BFFFFFFF).expect("cell index"); 10];

    assert_compact(cells);
}

#[test]
fn duplicate_minimum() {
    let index = CellIndex::try_from(0x8A0092492497FFF).expect("cell index");
    let mut cells = index.children(Resolution::Eleven).collect::<Vec<_>>();
    let cell = cells[0];
    cells.push(cell);

    assert_compact(cells);
}

#[test]
fn duplicate_pentagon_limit() {
    let index = CellIndex::try_from(0x8A0800000007FFF).expect("cell index");
    let mut cells = index.children(Resolution::Eleven).collect::<Vec<_>>();
    let cell = cells[0];
    cells.push(cell);

    assert_compact(cells);
}

#[test]
fn duplicate_minimum_bis() {
    let index = CellIndex::try_from(0x8A0092492497FFF).expect("cell index");
    let mut cells = index.children(Resolution::Eleven).collect::<Vec<_>>();
    let cell = cells[0];
    if let Some(last) = cells.last_mut() {
        *last = cell;
    }

    // This duplicate is not detected by H3 (see their test `compactCells_duplicateIgnored`).
    // But we do.
    assert!(CellIndex::compact(&mut cells).is_err());
}

#[test]
fn empty() {
    let cells = Vec::new();

    assert_compact(cells);
}

#[test]
fn disparate() {
    let cells = [
        0x81003FFFFFFFFFF,
        0x81023FFFFFFFFFF,
        0x81043FFFFFFFFFF,
        0x81063FFFFFFFFFF,
        0x81083FFFFFFFFFF,
        0x810A3FFFFFFFFFF,
        0x810C3FFFFFFFFFF,
    ]
    .into_iter()
    .map(|hex| CellIndex::try_from(hex).expect("cell index"))
    .collect();

    assert_compact(cells);
}

#[test]
fn resolution_mismatch() {
    let mut cells = [0x81003FFFFFFFFFF, 0x8500924BFFFFFFF, 0x89283470803FFFF]
        .into_iter()
        .map(|hex| CellIndex::try_from(hex).expect("cell index"))
        .collect::<Vec<_>>();

    // This duplicate is not detected by H3, but we do.
    let result = CellIndex::compact(&mut cells);

    assert!(result.is_err());
}
