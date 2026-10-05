use float_eq::assert_float_eq;
use h3o::CellIndex;

use super::h3api;

macro_rules! test {
    ($name:ident, $index:literal) => {
        #[test]
        fn $name() {
            let index = CellIndex::try_from($index).expect("cell index");
            let result = index.area_m2();
            let reference = h3api::cell_area_m2(index);

            assert_float_eq!(result, reference, r2nd <= f64::from(f32::EPSILON));
        }
    };
}

test!(hexagon_res0, 0x8009FFFFFFFFFFF);
test!(hexagon_res1, 0x81083FFFFFFFFFF);
test!(hexagon_res2, 0x820807FFFFFFFFF);
test!(hexagon_res3, 0x830800FFFFFFFFF);
test!(hexagon_res4, 0x8408001FFFFFFFF);
test!(hexagon_res5, 0x85080003FFFFFFF);
test!(hexagon_res6, 0x860800007FFFFFF);
test!(hexagon_res7, 0x870800000FFFFFF);
test!(hexagon_res8, 0x8808000001FFFFF);
test!(hexagon_res9, 0x89080000003FFFF);
test!(hexagon_res10, 0x8A0800000007FFF);
test!(hexagon_res11, 0x8B0800000000FFF);
test!(hexagon_res12, 0x8C08000000001FF);
test!(hexagon_res13, 0x8D080000000003F);
test!(hexagon_res14, 0x8E0800000000007);
test!(hexagon_res15, 0x8F0800000000000);

test!(pentagon_res0, 0x8073FFFFFFFFFFF);
test!(pentagon_res1, 0x81737FFFFFFFFFF);
test!(pentagon_res2, 0x82734FFFFFFFFFF);
test!(pentagon_res3, 0x83734EFFFFFFFFF);
test!(pentagon_res4, 0x84734A9FFFFFFFF);
test!(pentagon_res5, 0x85734E67FFFFFFF);
test!(pentagon_res6, 0x86734E64FFFFFFF);
test!(pentagon_res7, 0x87734E64DFFFFFF);
test!(pentagon_res8, 0x88734E6499FFFFF);
test!(pentagon_res9, 0x89734E64993FFFF);
test!(pentagon_res10, 0x8A734E64992FFFF);
test!(pentagon_res11, 0x8B734E649929FFF);
test!(pentagon_res12, 0x8C734E649929DFF);
test!(pentagon_res13, 0x8D734E64992D6FF);
test!(pentagon_res14, 0x8E734E64992D6DF);
test!(pentagon_res15, 0x8F734E64992D6D8);
