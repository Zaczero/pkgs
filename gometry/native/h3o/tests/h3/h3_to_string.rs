use h3o::{CellIndex, DirectedEdgeIndex, VertexIndex};

use super::h3api;

macro_rules! test {
    ($name:ident, $index:expr) => {
        #[test]
        fn $name() {
            let index = $index.expect("H3 index");
            let result = index.to_string();
            let reference = h3api::h3_to_string(index);

            assert_eq!(result, reference);
        }
    };
}

test!(cell_index_res0, CellIndex::try_from(0x802BFFFFFFFFFFF));
test!(cell_index_res12, CellIndex::try_from(0x8C2BAE305336BFF));
test!(cell_index_res15, CellIndex::try_from(0x8F2834782B9C2AB));
test!(
    edge_index_res0,
    DirectedEdgeIndex::try_from(0x1302BFFFFFFFFFFF)
);
test!(
    edge_index_res12,
    DirectedEdgeIndex::try_from(0x13C2BAE305336BFF)
);
test!(
    edge_index_res15,
    DirectedEdgeIndex::try_from(0x15F2834782B9C2AB)
);
test!(vertex_index_res0, VertexIndex::try_from(0x2302BFFFFFFFFFFF));
test!(
    vertex_index_res12,
    VertexIndex::try_from(0x23F2834782B9C2A8)
);
test!(
    vertex_index_res15,
    VertexIndex::try_from(0x21C2BAE305330DFF)
);
