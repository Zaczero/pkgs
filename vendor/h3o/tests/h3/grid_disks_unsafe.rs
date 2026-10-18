use h3o::CellIndex;

use super::h3api;

#[test]
fn identity() {
    let indexes = [CellIndex::try_from(0x8B1FB46622DCFFF).expect("cell index")];
    let result = CellIndex::grid_disks_fast(indexes.iter().copied(), 0).collect::<Option<Vec<_>>>();
    let reference = h3api::grid_disks_unsafe(indexes.iter().copied(), 0);

    assert_eq!(result, reference);
}

#[test]
fn ring1of1() {
    let indexes = [
        CellIndex::try_from(0x89283080DDBFFFF).expect("cell index"),
        CellIndex::try_from(0x89283080C37FFFF).expect("cell index"),
        CellIndex::try_from(0x89283080C27FFFF).expect("cell index"),
        CellIndex::try_from(0x89283080D53FFFF).expect("cell index"),
        CellIndex::try_from(0x89283080DCFFFFF).expect("cell index"),
        CellIndex::try_from(0x89283080DC3FFFF).expect("cell index"),
    ];
    let result = CellIndex::grid_disks_fast(indexes.iter().copied(), 1).collect::<Option<Vec<_>>>();
    let reference = h3api::grid_disks_unsafe(indexes.iter().copied(), 1);

    assert_eq!(result, reference);
}

#[test]
fn ring2of1() {
    let indexes = [
        CellIndex::try_from(0x89283080DDBFFFF).expect("cell index"),
        CellIndex::try_from(0x89283080C37FFFF).expect("cell index"),
        CellIndex::try_from(0x89283080C27FFFF).expect("cell index"),
        CellIndex::try_from(0x89283080D53FFFF).expect("cell index"),
        CellIndex::try_from(0x89283080DCFFFFF).expect("cell index"),
        CellIndex::try_from(0x89283080DC3FFFF).expect("cell index"),
    ];
    let result = CellIndex::grid_disks_fast(indexes.iter().copied(), 2).collect::<Option<Vec<_>>>();
    let reference = h3api::grid_disks_unsafe(indexes.iter().copied(), 2);

    assert_eq!(result, reference);
}

#[test]
fn failed() {
    let indexes = [
        CellIndex::try_from(0x8029FFFFFFFFFFF).expect("cell index"),
        CellIndex::try_from(0x801DFFFFFFFFFFF).expect("cell index"),
    ];
    let result = CellIndex::grid_disks_fast(indexes.iter().copied(), 2).collect::<Option<Vec<_>>>();
    let reference = h3api::grid_disks_unsafe(indexes.iter().copied(), 2);

    assert_eq!(result, reference);
}
