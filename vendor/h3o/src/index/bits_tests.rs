use super::*;
use crate::{CellIndex, Resolution};

#[test]
fn first_axe_none() {
    let index = CellIndex::try_from(0x8F2800000000000).expect("valid cell index");
    let result = first_axe(index.into());
    assert_eq!(result, None, "zero til the end");

    let index = CellIndex::try_from(0x8029FFFFFFFFFFF).expect("valid cell index");
    let result = first_axe(index.into());
    assert_eq!(result, None, "no cell");

    let index = CellIndex::try_from(0x832800FFFFFFFFF).expect("valid cell index");
    let result = first_axe(index.into());
    assert_eq!(result, None, "some zero then unused");
}

#[test]
fn first_axe_some() {
    let resolutions = Resolution::range(Resolution::One, Resolution::Fifteen);
    let indexes = [
        0x8F287478AB9C2AB, // 20-1-6-4-3-6-1-2-5-6-3-4-1-2-5-3
        0x8F283478AB9C2AB, // 20-0-6-4-3-6-1-2-5-6-3-4-1-2-5-3
        0x8F280478AB9C2AB, // 20-0-0-4-3-6-1-2-5-6-3-4-1-2-5-3
        0x8F280078AB9C2AB, // 20-0-0-0-3-6-1-2-5-6-3-4-1-2-5-3
        0x8F280018AB9C2AB, // 20-0-0-0-0-6-1-2-5-6-3-4-1-2-5-3
        0x8F280000AB9C2AB, // 20-0-0-0-0-0-1-2-5-6-3-4-1-2-5-3
        0x8F2800002B9C2AB, // 20-0-0-0-0-0-0-2-5-6-3-4-1-2-5-3
        0x8F2800000B9C2AB, // 20-0-0-0-0-0-0-0-5-6-3-4-1-2-5-3
        0x8F280000019C2AB, // 20-0-0-0-0-0-0-0-0-6-3-4-1-2-5-3
        0x8F280000001C2AB, // 20-0-0-0-0-0-0-0-0-0-3-4-1-2-5-3
        0x8F28000000042AB, // 20-0-0-0-0-0-0-0-0-0-0-4-1-2-5-3
        0x8F28000000002AB, // 20-0-0-0-0-0-0-0-0-0-0-0-1-2-5-3
        0x8F28000000000AB, // 20-0-0-0-0-0-0-0-0-0-0-0-0-2-5-3
        0x8F280000000002B, // 20-0-0-0-0-0-0-0-0-0-0-0-0-0-5-3
        0x8F2800000000003, // 20-0-0-0-0-0-0-0-0-0-0-0-0-0-0-3
    ];

    for (resolution, value) in resolutions.zip(indexes) {
        let index = CellIndex::try_from(value).expect("valid cells");
        let expected = index.direction_at(resolution).and_then(Direction::axe);

        let result = first_axe(index.into());

        assert_eq!(result, expected, "resolution {resolution:?}");
    }
}
