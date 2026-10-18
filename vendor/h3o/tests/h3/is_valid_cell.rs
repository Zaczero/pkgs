use h3o::CellIndex;

use super::h3api;

macro_rules! test {
    ($name:ident, $index:literal) => {
        #[test]
        fn $name() {
            let result = CellIndex::try_from($index).is_ok();
            let reference = h3api::is_valid_cell($index);

            assert_eq!(result, reference);
        }
    };
}

test!(res0, 0x8075FFFFFFFFFFF);
test!(res1, 0x81757FFFFFFFFFF);
test!(res2, 0x82754FFFFFFFFFF);
test!(res3, 0x83754EFFFFFFFFF);
test!(res4, 0x84754A9FFFFFFFF);
test!(res5, 0x85754E67FFFFFFF);
test!(res6, 0x86754E64FFFFFFF);
test!(res7, 0x87754E64DFFFFFF);
test!(res8, 0x88754E6499FFFFF);
test!(res9, 0x89754E64993FFFF);
test!(res10, 0x8A754E64992FFFF);
test!(res11, 0x8B754E649929FFF);
test!(res12, 0x8C754E649929DFF);
test!(res13, 0x8D754E64992D6FF);
test!(res14, 0x8E754E64992D6DF);
test!(res15, 0x8F754E64992D6D8);

test!(high_bit_set, 0x88C2BAE305336BFF);
test!(invalid_mode, 0x28C2BAE305336BFF);
test!(tainted_reserved_bits, 0xAC2BAE305336BFF);
test!(invalid_base_cell, 0x80FFFFFFFFFFFFF);

test!(unexpected_unused_first, 0x8C2BEE305336BFF);
test!(unexpected_unused_middle, 0x8C2BAE33D336BFF);
test!(unexpected_unused_last, 0x8C2BAE305336FFF);

test!(missing_unused_first, 0x8C0FAE305336AFF);
test!(missing_unused_middle, 0x8C0FAE305336FEF);
test!(missing_unused_last, 0x81757FFFFFFFFFE);

test!(deleted_subsequence_hexagon1, 0x81887FFFFFFFFFF);
test!(deleted_subsequence_pentagon1, 0x81087FFFFFFFFFF);
test!(deleted_subsequence_hexagon2, 0x8804000011FFFFF);
test!(deleted_subsequence_pentagon2, 0x8808000011FFFFF);
