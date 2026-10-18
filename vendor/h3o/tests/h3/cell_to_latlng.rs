use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use h3o::{CellIndex, LatLng};

use super::h3api;

macro_rules! test {
    ($name:ident, $index:literal) => {
        #[test]
        fn $name() {
            let index = CellIndex::try_from($index).expect("cell index");
            let result = LatLng::from(index);
            let reference = h3api::cell_to_latlng(index);

            assert_eq!(result, reference);
        }
    };
}

test!(res0, 0x801FFFFFFFFFFFF);
test!(res1, 0x811FBFFFFFFFFFF);
test!(res2, 0x821FB7FFFFFFFFF);
test!(res3, 0x831FB4FFFFFFFFF);
test!(res4, 0x841FB47FFFFFFFF);
test!(res5, 0x851FB467FFFFFFF);
test!(res6, 0x861FB4667FFFFFF);
test!(res7, 0x871FB4662FFFFFF);
test!(res8, 0x881FB46623FFFFF);
test!(res9, 0x891FB46622FFFFF);
test!(res10, 0x8A1FB46622DFFFF);
test!(res11, 0x8B1FB46622D8FFF);
test!(res12, 0x8C1FB46622D85FF);
test!(res13, 0x8D1FB46622D85BF);
test!(res14, 0x8E1FB46622D8597);
test!(res15, 0x8F1FB46622D8591);

// Those ones triggered a bug where longitude weren't in the right bounds.
test!(bound_bug1, 0x8400481FFFFFFFF);
test!(bound_bug2, 0x8471921FFFFFFFF);

// This one triggered a bug where an invalid CellIndex was used.
test!(index_invariant_bug, 0x840800BFFFFFFFF);

// # How to generate/update `cellToLatLng.txt`
//
// First copy test data from the H3 repository
//
// ```
// mkdir data
// cp /path/to/h3/repo/tests/inputfiles/*ic.txt data
// ```
//
// Then, extract H3 index only with `cut -f1 -d' ' data/* > cellToLatLng.tmp`
//
// Finally, sort and remove dups with `sort -u cellToLatLng.tmp > cellToLatLng.txt`
//
// And voilà!
#[test]
fn h3_testinput() {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("dataset/cellToLatLng.txt");

    let file = File::open(path).expect("open test dataset");
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line.expect("test input");
        let index = line.parse::<CellIndex>().expect("cell index");
        let result = LatLng::from(index);
        let reference = h3api::cell_to_latlng(index);

        assert_eq!(result, reference, "cell {index}");
    }
}
