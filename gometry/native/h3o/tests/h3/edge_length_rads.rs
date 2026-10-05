use float_eq::assert_float_eq;
use h3o::CellIndex;

use super::h3api;

macro_rules! test {
    ($name:ident, $index:literal) => {
        #[test]
        fn $name() {
            let index = CellIndex::try_from($index).expect("edge index");
            for edge in index.edges() {
                let result = edge.length_rads();
                let reference = h3api::edge_length_rads(edge);

                assert_float_eq!(
                    result,
                    reference,
                    r2nd <= f64::from(f32::EPSILON),
                    "edge {edge}",
                );
            }
        }
    };
}

test!(hexagon_res0, 0x8019FFFFFFFFFFF);
test!(hexagon_res1, 0x811FBFFFFFFFFFF);
test!(hexagon_res2, 0x821FB7FFFFFFFFF);
test!(hexagon_res3, 0x831FB4FFFFFFFFF);
test!(hexagon_res4, 0x841FB47FFFFFFFF);
test!(hexagon_res5, 0x851FB467FFFFFFF);
test!(hexagon_res6, 0x861FB4667FFFFFF);
test!(hexagon_res7, 0x871FB4662FFFFFF);
test!(hexagon_res8, 0x881FB46623FFFFF);
test!(hexagon_res9, 0x891FB46622FFFFF);
test!(hexagon_res10, 0x8A1FB46622DFFFF);
test!(hexagon_res11, 0x8B1FB46622DCFFF);
test!(hexagon_res12, 0x8C1FB46622DC9FF);
test!(hexagon_res13, 0x8D1FB46622DC83F);
test!(hexagon_res14, 0x8E1FB46622DC81F);
test!(hexagon_res15, 0x8F1FB46622DC81B);

test!(pentagon_res0, 0x8031FFFFFFFFFFF);
test!(pentagon_res1, 0x81303FFFFFFFFFF);
test!(pentagon_res2, 0x823007FFFFFFFFF);
test!(pentagon_res3, 0x833000FFFFFFFFF);
test!(pentagon_res4, 0x8430001FFFFFFFF);
test!(pentagon_res5, 0x85300003FFFFFFF);
test!(pentagon_res6, 0x863000007FFFFFF);
test!(pentagon_res7, 0x873000000FFFFFF);
test!(pentagon_res8, 0x8830000001FFFFF);
test!(pentagon_res9, 0x89300000003FFFF);
test!(pentagon_res10, 0x8A3000000007FFF);
test!(pentagon_res11, 0x8B3000000000FFF);
test!(pentagon_res12, 0x8C30000000001FF);
test!(pentagon_res13, 0x8D300000000003F);
test!(pentagon_res14, 0x8E3000000000007);
test!(pentagon_res15, 0x8F3000000000000);
