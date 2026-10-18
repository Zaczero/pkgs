use h3o::DirectedEdgeIndex;

use super::h3api;

macro_rules! test {
    ($name:ident, $index:literal) => {
        #[test]
        fn $name() {
            let index = DirectedEdgeIndex::try_from($index).expect("edge index");
            let result = index.origin();
            let reference = h3api::get_directed_edge_origin(index);

            assert_eq!(result, reference);
        }
    };
}

test!(hexagon, 0x13F2834782B9C2AB);
test!(pentagon, 0x1370800000FFFFFF);
