use h3o::{CellIndex, Resolution};

use super::h3api;

macro_rules! test {
    ($name:ident, $compacted:expr, $resolution:literal) => {
        #[test]
        fn $name() {
            let compacted = $compacted
                .into_iter()
                .map(|value| CellIndex::try_from(value).expect("cell index"))
                .collect::<Vec<_>>();
            let resolution = Resolution::try_from($resolution).expect("index resolution");
            let result = CellIndex::uncompact_size(compacted.iter().copied(), resolution);
            let reference = h3api::uncompact_cells_size(&compacted, resolution);

            assert_eq!(result, reference);
        }
    };
}

test!(single_hexagon, vec![0x802BFFFFFFFFFFF], 5);
test!(single_pentagon, vec![0x820807FFFFFFFFF], 5);
test!(
    mix,
    vec![0x802BFFFFFFFFFFF, 0x820807FFFFFFFFF, 0x83734EFFFFFFFFF],
    5
);
