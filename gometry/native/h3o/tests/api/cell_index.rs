use h3o::{BaseCell, CellIndex, Direction, Resolution, error};

#[test]
fn is_neighbor_with() {
    let src = CellIndex::try_from(0x8A1FB46622DFFFF).expect("src");
    let dst = CellIndex::try_from(0x8A1FB46622D7FFF).expect("dst");
    assert_eq!(src.is_neighbor_with(dst), Ok(true));

    assert_eq!(
        src.is_neighbor_with(dst.parent(Resolution::Six).expect("parent")),
        Err(error::ResolutionMismatch)
    );

    let dst = CellIndex::try_from(0x8A1FB4644937FFF).expect("dst2");
    assert_eq!(src.is_neighbor_with(dst), Ok(false));
}

#[test]
fn to_local_ij() {
    let anchor = CellIndex::try_from(0x823147FFFFFFFFF).expect("anchor");
    let index = CellIndex::try_from(0x8230E7FFFFFFFFF).expect("index");
    let result = index.to_local_ij(anchor).expect("localij");

    assert_eq!(result.anchor, anchor);
    assert_eq!(result.coord.i, -1);
    assert_eq!(result.coord.j, -2);

    let result = index.to_local_ij(anchor.parent(Resolution::One).expect("parent"));

    assert!(result.is_err());
}

#[test]
fn try_from_str() {
    let result = "8a1fb46622dffff".parse::<CellIndex>();
    let expected = CellIndex::try_from(0x8A1FB46622DFFFF);
    assert_eq!(result, expected, "valid string");

    let result = "no bueno".parse::<CellIndex>();
    assert!(result.is_err(), "invalid string");
}

// Resolutions are displayed as numerical value.
#[test]
fn display() {
    let index = CellIndex::try_from(0x8A1FB46622DFFFF).expect("index");

    // Default display is the lower hex one.
    let result = index.to_string();
    let expected = "8a1fb46622dffff".to_owned();
    assert_eq!(result, expected, "default display");

    // Upper hex.
    let result = format!("{index:X}");
    let expected = "8A1FB46622DFFFF".to_owned();
    assert_eq!(result, expected, "upper hex");

    // Octal.
    let result = format!("{index:o}");
    let expected = "42417664314213377777".to_owned();
    assert_eq!(result, expected, "octal");

    // Binary.
    let result = format!("{index:b}");
    let expected = "100010100001111110110100011001100010001011011111111111111111".to_owned();
    assert_eq!(result, expected, "binary");
}

#[test]
fn child_position() {
    let index = CellIndex::try_from(0x8A1FB46622DFFFF).expect("index");

    assert_eq!(index.child_position(Resolution::Eight), Some(24));
    assert_eq!(index.child_position(Resolution::Twelve), None);
}

#[test]
fn child_at() {
    let index = CellIndex::try_from(0x881FB46623FFFFF).expect("index");

    assert_eq!(
        index.child_at(24, Resolution::Ten),
        CellIndex::try_from(0x8A1FB46622DFFFF).ok(),
    );
    assert_eq!(index.child_at(24, Resolution::Five), None);
}

#[test]
fn child_position_roundtrip() {
    let res = Resolution::Zero;
    let child = CellIndex::try_from(0x8FC3B0804200001).expect("child");
    let parent = child.parent(res).expect("parent");

    let position = child.child_position(res).expect("position");
    let cell = parent.child_at(position, child.resolution());

    assert_eq!(cell, Some(child));
}

#[test]
fn max_grid_ring_size() {
    assert_eq!(h3o::max_grid_ring_size(2), 12);
}

#[test]
fn max_grid_ring_size_identity() {
    assert_eq!(h3o::max_grid_ring_size(0), 1);
}

#[test]
fn grid_ring_identity() {
    let index = CellIndex::try_from(0x891FB46741BFFFF).expect("index");

    let result = index.grid_ring::<Vec<_>>(0);
    assert_eq!(result, vec![index]);
}

#[test]
fn grid_ring_1() {
    let index = CellIndex::try_from(0x89283080DCBFFFF).expect("index");
    let mut expected = [
        0x89283080DDBFFFF,
        0x89283080C37FFFF,
        0x89283080C27FFFF,
        0x89283080D53FFFF,
        0x89283080DCFFFFF,
        0x89283080DC3FFFF,
    ]
    .into_iter()
    .map(|value| CellIndex::try_from(value).expect("valid cell index"))
    .collect::<Vec<_>>();
    let mut result = index.grid_ring::<Vec<_>>(1);

    result.sort_unstable();
    expected.sort_unstable();
    assert_eq!(result, expected);
}

#[test]
fn grid_ring_2() {
    let index = CellIndex::try_from(0x89283080DCBFFFF).expect("index");
    let mut expected = [
        0x89283080CA7FFFF,
        0x89283080CAFFFFF,
        0x89283080C33FFFF,
        0x89283080C23FFFF,
        0x89283080C2FFFFF,
        0x89283080D5BFFFF,
        0x89283080D43FFFF,
        0x89283080D57FFFF,
        0x89283080D1BFFFF,
        0x89283080DC7FFFF,
        0x89283080DD7FFFF,
        0x89283080DD3FFFF,
    ]
    .into_iter()
    .map(|value| CellIndex::try_from(value).expect("valid cell index"))
    .collect::<Vec<_>>();
    let mut result = index.grid_ring::<Vec<_>>(2);

    result.sort_unstable();
    expected.sort_unstable();
    assert_eq!(result, expected);
}

#[test]
fn grid_ring_polar_pentagon_res0() {
    let index = CellIndex::try_from(0x8009FFFFFFFFFFF).expect("index");
    let mut expected = [
        0x8007FFFFFFFFFFF,
        0x8001FFFFFFFFFFF,
        0x8011FFFFFFFFFFF,
        0x801FFFFFFFFFFFF,
        0x8019FFFFFFFFFFF,
    ]
    .into_iter()
    .map(|value| CellIndex::try_from(value).expect("valid cell index"))
    .collect::<Vec<_>>();
    let mut result = index.grid_ring::<Vec<_>>(1);

    result.sort_unstable();
    expected.sort_unstable();
    assert_eq!(result, expected);
}

#[test]
fn grid_ring_polar_pentagon_res1() {
    let index = CellIndex::try_from(0x81083FFFFFFFFFF).expect("index");
    let mut expected = [
        0x81093FFFFFFFFFF,
        0x81097FFFFFFFFFF,
        0x8108FFFFFFFFFFF,
        0x8108BFFFFFFFFFF,
        0x8109BFFFFFFFFFF,
    ]
    .into_iter()
    .map(|value| CellIndex::try_from(value).expect("valid cell index"))
    .collect::<Vec<_>>();
    let mut result = index.grid_ring::<Vec<_>>(1);

    result.sort_unstable();
    expected.sort_unstable();
    assert_eq!(result, expected);
}

#[test]
fn grid_ring_3_polar_pentagon_res1() {
    let index = CellIndex::try_from(0x81083FFFFFFFFFF).expect("index");
    let mut expected = [
        0x811FBFFFFFFFFFF,
        0x81003FFFFFFFFFF,
        0x81183FFFFFFFFFF,
        0x8111BFFFFFFFFFF,
        0x81067FFFFFFFFFF,
        0x811E7FFFFFFFFFF,
        0x8101BFFFFFFFFFF,
        0x81107FFFFFFFFFF,
        0x81063FFFFFFFFFF,
        0x811E3FFFFFFFFFF,
        0x8119BFFFFFFFFFF,
        0x81103FFFFFFFFFF,
        0x81007FFFFFFFFFF,
        0x81187FFFFFFFFFF,
        0x8107BFFFFFFFFFF,
    ]
    .into_iter()
    .map(|value| CellIndex::try_from(value).expect("valid cell index"))
    .collect::<Vec<_>>();
    let mut result = index.grid_ring::<Vec<_>>(3);

    result.sort_unstable();
    expected.sort_unstable();
    assert_eq!(result, expected);
}

#[test]
fn grid_ring_4_polar_pentagon_res1() {
    let index = CellIndex::try_from(0x811C3FFFFFFFFFF).expect("index");
    let mut expected = [
        0x81227FFFFFFFFFF,
        0x81293FFFFFFFFFF,
        0x8136BFFFFFFFFFF,
        0x81167FFFFFFFFFF,
        0x81477FFFFFFFFFF,
        0x810DBFFFFFFFFFF,
        0x81473FFFFFFFFFF,
        0x81237FFFFFFFFFF,
        0x81127FFFFFFFFFF,
        0x8126BFFFFFFFFFF,
        0x81177FFFFFFFFFF,
        0x810D3FFFFFFFFFF,
        0x8150FFFFFFFFFFF,
        0x8102FFFFFFFFFFF,
        0x8129BFFFFFFFFFF,
        0x8102BFFFFFFFFFF,
        0x81507FFFFFFFFFF,
        0x8136FFFFFFFFFFF,
        0x8127BFFFFFFFFFF,
        0x81137FFFFFFFFFF,
    ]
    .into_iter()
    .map(|value| CellIndex::try_from(value).expect("valid cell index"))
    .collect::<Vec<_>>();
    let mut result = index.grid_ring::<Vec<_>>(4);

    result.sort_unstable();
    expected.sort_unstable();
    assert_eq!(result, expected);
}

#[test]
fn succ() {
    let index = CellIndex::try_from(0o42417664314213377777).expect("index");
    let expected = CellIndex::try_from(0o42417664314213477777).ok();
    assert_eq!(index.succ(), expected, "base case");

    let index = CellIndex::try_from(0o42417664314213677777).expect("index");
    let expected = CellIndex::try_from(0o42417664314214077777).ok();
    assert_eq!(index.succ(), expected, "single carry");

    let index = CellIndex::try_from(0o42417664314666677777).expect("index");
    let expected = CellIndex::try_from(0o42417664315000077777).ok();
    assert_eq!(index.succ(), expected, "cascade carry");

    let index = CellIndex::try_from(0o42466666666666677777).expect("index");
    let expected = CellIndex::try_from(0o42467000000000077777).ok();
    assert_eq!(index.succ(), expected, "cascade to base cell");

    let index = CellIndex::try_from(0o42571666666666677777).expect("index");
    assert!(index.succ().is_none(), "last");

    let index = CellIndex::try_from(0o42404000000000077777).expect("index");
    let expected = CellIndex::try_from(0o42404000000000277777).ok();
    assert_eq!(index.succ(), expected, "deleted subsequence");

    let index = CellIndex::try_from(0x8009FFFFFFFFFFF).expect("index");
    let expected = CellIndex::try_from(0x800BFFFFFFFFFFF).ok();
    assert_eq!(index.succ(), expected, "base cell");
}

#[test]
fn pred() {
    let index = CellIndex::try_from(0o42417664314213477777).expect("index");
    let expected = CellIndex::try_from(0o42417664314213377777).ok();
    assert_eq!(index.pred(), expected, "base case");

    let index = CellIndex::try_from(0o42417664314213077777).expect("index");
    let expected = CellIndex::try_from(0o42417664314212677777).ok();
    assert_eq!(index.pred(), expected, "single carry");

    let index = CellIndex::try_from(0o42417664314000077777).expect("index");
    let expected = CellIndex::try_from(0o42417664313666677777).ok();
    assert_eq!(index.pred(), expected, "cascade carry");

    let index = CellIndex::try_from(0o42500000000000077777).expect("index");
    let expected = CellIndex::try_from(0o42477666666666677777).ok();
    assert_eq!(index.pred(), expected, "cascade to base cell");

    let index = CellIndex::try_from(0o42400000000000077777).expect("index");
    assert!(index.pred().is_none(), "last");

    let index = CellIndex::try_from(0o42404000000000277777).expect("index");
    let expected = CellIndex::try_from(0o42404000000000077777).ok();
    assert_eq!(index.pred(), expected, "deleted subsequence");

    let index = CellIndex::try_from(0x800BFFFFFFFFFFF).expect("index");
    let expected = CellIndex::try_from(0x8009FFFFFFFFFFF).ok();
    assert_eq!(index.pred(), expected, "base cell");
}

#[test]
fn first() {
    for resolution in Resolution::range(Resolution::Zero, Resolution::Fifteen) {
        assert!(
            CellIndex::first(resolution).pred().is_none(),
            "res {}",
            resolution
        );
    }
}

#[test]
fn last() {
    for resolution in Resolution::range(Resolution::Zero, Resolution::Fifteen) {
        assert!(
            CellIndex::last(resolution).succ().is_none(),
            "res {}",
            resolution
        );
    }
}

// https://github.com/uber/h3-java/issues/131
#[test]
fn bug_h3_java_131() {
    let mut cells = [
        581487719465615359,
        582222193232969727,
        581193050349371391,
        582613619372457983,
        581927524116725759,
        581584476488859647,
        581241428860993535,
        581289807372615679,
        581681233512103935,
        581729612023726079,
        581434942907482111,
        582169416674836479,
        581483321419104255,
        582217795186458623,
        581188652302860287,
        581923126070214655,
        581580078442348543,
        581237030814482431,
        581285409326104575,
        581676835465592831,
        581725213977214975,
        581087497233104895,
        582165018628325375,
        581478923372593151,
        581184254256349183,
        582604823279435775,
        581918728023703551,
        581575680395837439,
        581232632767971327,
        582015485046947839,
        581672437419081727,
        581720815930703871,
        581426146814459903,
        581083099186593791,
        581474525326082047,
        581179856209838079,
        581571282349326335,
        581228234721460223,
        581716417884192767,
        581078701140082687,
        581470127279570943,
        582204601046925311,
        581566884302815231,
        581223836674949119,
        582006688953925631,
        581663641326059519,
        581712019837681663,
        581025924581949439,
        581074303093571583,
        582200203000414207,
        582248581512036351,
        581219438628438015,
        581659243279548415,
        580973148023816191,
        581707621791170559,
        581461331186548735,
        582244183465525247,
        581215040581926911,
        581654845233037311,
        581311797605171199,
        582389319000391679,
        581017128488927231,
        581945116302770175,
        581650447186526207,
        581307399558660095,
        581698825698148351,
        581012730442416127,
        581452535093526527,
        581940718256259071,
        581646049140015103,
        581303001512148991,
        581694427651637247,
        581008332395905023,
        581448137047015423,
        581201846442393599,
        581936320209747967,
        581593272581881855,
        581641651093503999,
        581298603465637887,
        581690029605126143,
        581003934349393919,
        581443739000504319,
        581100691372638207,
        582178212767858687,
        582226591279480831,
        581197448395882495,
        581931922163236863,
        581588874535370751,
        582323348302725119,
        581637253046992895,
        581294205419126783,
        581685631558615039,
        581734010070237183,
        581439340953993215,
    ]
    .into_iter()
    .map(|value| CellIndex::try_from(value).expect("valid cell index"))
    .collect::<Vec<_>>();

    assert!(CellIndex::compact(&mut cells).is_ok());
}

// https://github.com/uber/h3/issues/915
#[test]
fn bug_h3_915() {
    let expected = CellIndex::base_cells().collect::<Vec<_>>();
    assert_eq!(expected.len(), 122);

    let mut cells = CellIndex::base_cells()
        .flat_map(|index| index.children(Resolution::One))
        .collect::<Vec<_>>();
    assert_eq!(cells.len(), 842);

    CellIndex::compact(&mut cells).expect("compact");
    assert_eq!(cells, expected);
}

#[test]
fn from_raw_parts_roundtrip() {
    for resolution in Resolution::range(Resolution::Zero, Resolution::Four) {
        for base_cell in CellIndex::base_cells() {
            for expected in base_cell.children(resolution) {
                let base = expected.base_cell();
                let directions = Resolution::range(Resolution::One, expected.resolution())
                    .map(|res| expected.direction_at(res).unwrap())
                    .collect::<Vec<_>>();
                let result = CellIndex::from_raw_parts(base, &directions);

                assert_eq!(
                    result,
                    Ok(expected),
                    "from_raw_parts rountrip for {expected}"
                );
            }
        }
    }
}

#[test]
fn from_raw_parts_valid() {
    // Resolution 0.
    let expected = CellIndex::try_from(0x8001FFFFFFFFFFF).unwrap();
    let base_cell = BaseCell::try_from(0).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &[]);
    assert_eq!(result, Ok(expected), "{expected}");

    let expected = CellIndex::try_from(0x8003FFFFFFFFFFF).unwrap();
    let base_cell = BaseCell::try_from(1).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &[]);
    assert_eq!(result, Ok(expected), "{expected}");

    let expected = CellIndex::try_from(0x80F3FFFFFFFFFFF).unwrap();
    let base_cell = BaseCell::try_from(121).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &[]);
    assert_eq!(result, Ok(expected), "{expected}");

    // Resolution 3.
    let expected = CellIndex::try_from(0x839253FFFFFFFFF).unwrap();
    let base_cell = BaseCell::try_from(73).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &[Direction::K, Direction::J, Direction::JK]);
    assert_eq!(result, Ok(expected), "{expected}");

    // Resolution 2.
    let expected = CellIndex::try_from(0x821F67FFFFFFFFF).unwrap();
    let base_cell = BaseCell::try_from(15).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &[Direction::IK, Direction::I]);
    assert_eq!(result, Ok(expected), "{expected}");

    // Resolution 1.
    let expected = CellIndex::try_from(0x8155BFFFFFFFFFF).unwrap();
    let base_cell = BaseCell::try_from(42).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &[Direction::IJ]);
    assert_eq!(result, Ok(expected), "{expected}");

    // Resolution 15.
    let expected = CellIndex::try_from(0x8F754E64992D6D8).unwrap();
    let base_cell = BaseCell::try_from(58).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &[
        Direction::IK,
        Direction::K,
        Direction::IJ,
        Direction::JK,
        Direction::K,
        Direction::K,
        Direction::K,
        Direction::I,
        Direction::I,
        Direction::IK,
        Direction::IK,
        Direction::JK,
        Direction::JK,
        Direction::JK,
        Direction::Center,
    ]);
    assert_eq!(result, Ok(expected), "{expected}");
}

#[test]
fn from_raw_parts_invalid_resolution() {
    let result = CellIndex::from_raw_parts(BaseCell::try_from(58).unwrap(), &[
        Direction::IK,
        Direction::K,
        Direction::IJ,
        Direction::JK,
        Direction::K,
        Direction::K,
        Direction::K,
        Direction::I,
        Direction::I,
        Direction::IK,
        Direction::IK,
        Direction::JK,
        Direction::JK,
        Direction::JK,
        Direction::Center,
        Direction::JK,
    ]);
    assert!(result.is_err(), "too many directions");
}

#[test]
fn from_raw_parts_deleted_subsequence() {
    // 1. Base cell 4 is a pentagon base cell
    let base_cell = BaseCell::try_from(4).unwrap();
    let mut directions = vec![Direction::Center, Direction::Center, Direction::Center];
    // 1A. Before the deleted subsequence: valid!
    let expected = CellIndex::try_from(0x830800FFFFFFFFF).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &directions);
    assert_eq!(result, Ok(expected), "{expected}");
    // 1B. The deleted subsequence should fail.
    directions[2] = Direction::K;
    let result = CellIndex::from_raw_parts(base_cell, &directions);
    assert!(result.is_err(), "deleted subsequence");
    // 1C. After the deleted subsequence: valid!
    directions[2] = Direction::J;
    let expected = CellIndex::try_from(0x830802FFFFFFFFF).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &directions);
    assert_eq!(result, Ok(expected), "{expected}");

    // 2. Base cell 5 is NOT a pentagon base cell.
    let base_cell = BaseCell::try_from(5).unwrap();
    let mut directions = vec![Direction::Center, Direction::Center, Direction::Center];
    // 2A. before the deleted subsequence: valid!
    let expected = CellIndex::try_from(0x830A00FFFFFFFFF).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &directions);
    assert_eq!(result, Ok(expected), "{expected}");
    // 2B. No deleted subsequence for non-pentagonal cell: valid!
    directions[2] = Direction::K;
    let expected = CellIndex::try_from(0x830A01FFFFFFFFF).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &directions);
    assert_eq!(result, Ok(expected), "{expected}");
    // 2C. After the deleted subsequence: valid!
    directions[2] = Direction::J;
    let expected = CellIndex::try_from(0x830A02FFFFFFFFF).unwrap();
    let result = CellIndex::from_raw_parts(base_cell, &directions);
    assert_eq!(result, Ok(expected), "{expected}");
}
