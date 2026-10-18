use h3o::CellIndex;

use super::h3api;

#[test]
fn value() {
    let result = CellIndex::base_cells().collect::<Vec<_>>();
    let reference = h3api::get_res0_cells();

    assert_eq!(result, reference);
}
