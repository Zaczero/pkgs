use h3o::BaseCell;

use super::h3api;

#[test]
fn value() {
    let result = BaseCell::count();
    let reference = h3api::res0_cell_count();

    assert_eq!(result, reference);
}
