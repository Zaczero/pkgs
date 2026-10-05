use h3o::DirectedEdgeIndex;

use super::h3api;

macro_rules! test {
    ($name:ident, $index:literal) => {
        #[test]
        fn $name() {
            let result = DirectedEdgeIndex::try_from($index).is_ok();
            let reference = h3api::is_valid_directed_edge($index);

            assert_eq!(result, reference);
        }
    };
}

test!(res0, 0x15075FFFFFFFFFFF);
test!(res1, 0x151757FFFFFFFFFF);
test!(res2, 0x152754FFFFFFFFFF);
test!(res3, 0x153754EFFFFFFFFF);
test!(res4, 0x154754A9FFFFFFFF);
test!(res5, 0x155754E67FFFFFFF);
test!(res6, 0x156754E64FFFFFFF);
test!(res7, 0x157754E64DFFFFFF);
test!(res8, 0x158754E6499FFFFF);
test!(res9, 0x159754E64993FFFF);
test!(res10, 0x15A754E64992FFFF);
test!(res11, 0x15B754E649929FFF);
test!(res12, 0x15C754E649929DFF);
test!(res13, 0x15D754E64992D6FF);
test!(res14, 0x15E754E64992D6DF);
test!(res15, 0x15F754E64992D6D8);

test!(high_bit_set, 0x95C2BAE305336BFF);
test!(invalid_mode, 0x1DC2BAE305336BFF);
test!(invalid_edge, 0x10C2BAE305336BFF);
test!(deleted_pentagon_edge, 0x111083FFFFFFFFFF);
test!(invalid_base_cell, 0x150FFFFFFFFFFFFF);

test!(unexpected_unused_first, 0x15C2BEE305336BFF);
test!(unexpected_unused_middle, 0x15C2BAE33D336BFF);
test!(unexpected_unused_last, 0x15C2BAE305336FFF);

test!(missing_unused_first, 0x15C0FAE305336AFF);
test!(missing_unused_middle, 0x15C0FAE305336FEF);
test!(missing_unused_last, 0x151757FFFFFFFFFE);

test!(deleted_subsequence_hexagon1, 0x151887FFFFFFFFFF);
test!(deleted_subsequence_pentagon1, 0x151087FFFFFFFFFF);
test!(deleted_subsequence_hexagon2, 0x15804000011FFFFF);
test!(deleted_subsequence_pentagon2, 0x15808000011FFFFF);
