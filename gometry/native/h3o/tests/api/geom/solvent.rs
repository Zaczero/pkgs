use ahash::HashSet;
use approx::{assert_relative_eq, relative_eq};
use geo::{Area, BooleanOps, LineString, MultiPolygon, Polygon, polygon};
use h3o::geom::{SolventBuilder, TilerBuilder};
use h3o::{CellIndex, Resolution};

use super::utils::load_polygon;

#[test]
fn hole_in_center() {
    let index = CellIndex::try_from(0x89283470803FFFF).expect("index");
    let mut cells = index.children(Resolution::Ten).collect::<Vec<_>>();

    // Remove the center cell.
    let center = index.center_child(Resolution::Ten).expect("center");
    let idx = cells.iter().position(|x| *x == center).expect("idx");
    cells.remove(idx);

    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(cells.iter().copied()).expect("geometry");
    let expected = MultiPolygon::new(vec![polygon!(
        exterior: [
            (x: -122.02796455348616, y: 37.38525281604115 ),
            (x: -122.02732437374608, y: 37.385758270113065),
            (x: -122.02648011977477, y: 37.38558967035685 ),
            (x: -122.02583992481574, y: 37.38609511818443 ),
            (x: -122.02604398797318, y: 37.386769168218684),
            (x: -122.02540378194031, y: 37.38727461225182 ),
            (x: -122.02560784515092, y: 37.38794865717242 ),
            (x: -122.02645212137664, y: 37.38811725429045 ),
            (x: -122.02665619162275, y: 37.38879129032762 ),
            (x: -122.02750047073862, y: 37.38895987611164 ),
            (x: -122.02814066848063, y: 37.38845442717775 ),
            (x: -122.02898493935817, y: 37.38862300294707 ),
            (x: -122.0296251218798,  y: 37.38811754776844 ),
            (x: -122.02942103767036, y: 37.38744351927073 ),
            (x: -122.03006120911812, y: 37.38693806029814 ),
            (x: -122.02985712496266, y: 37.386264026686845),
            (x: -122.0290128763404,  y: 37.38609544827806 ),
            (x: -122.02880879921976, y: 37.38542140578344 )
        ],
        interiors: [
            [
                (x: -122.02752844388534, y: 37.386432316377665),
                (x: -122.02837270074619, y: 37.38660090480038 ),
                (x: -122.02857677792056, y: 37.38727494218174 ),
                (x: -122.0279365912526,  y: 37.38778039491016 ),
                (x: -122.02709232326434, y: 37.387611807806856),
                (x: -122.0268882530716,  y: 37.386937766655734)
            ],
        ],
    )]);

    assert_multipolygon_equivalent(&result, &expected, 1e-6);
    assert_hetero_equal_homo(cells, Resolution::Ten, &result);
}

// -----------------------------------------------------------------------------

#[test]
fn duplicate_simple() {
    let set = [0x89283082813FFFF, 0x89283082817FFFF, 0x89283082813FFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"));
    let result = SolventBuilder::default().build().dissolve(set);

    assert!(result.is_err())
}

#[test]
fn consecutive_duplicate() {
    let set = [0x89283082813FFFF, 0x89283082817FFFF, 0x89283082817FFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();

    let result = SolventBuilder::new().build().dissolve(set.iter().copied());
    assert!(result.is_err(), "homogeneous detection");

    let result = SolventBuilder::new()
        .enable_heterogeneous_support(Resolution::Nine)
        .build()
        .dissolve(set);
    assert!(result.is_err(), "heterogeneous detection");
}

#[test]
fn duplicate_heterogeneous() {
    let set = [0x89283082813FFFF, 0x89283082817FFFF, 0x8A283082810FFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"));
    let result = SolventBuilder::new()
        .enable_heterogeneous_support(Resolution::Ten)
        .build()
        .dissolve(set);

    assert!(result.is_err())
}

#[test]
fn heterogeneous_resolution() {
    let set = [0x89283082813FFFF, 0x8828308299FFFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let result = SolventBuilder::new().build().dissolve(set.iter().copied());

    assert!(result.is_err(), "no heterogeneous support");

    // But this is fine when heterogeneous support is enabled.
    let result = SolventBuilder::new()
        .enable_heterogeneous_support(Resolution::Nine)
        .build()
        .dissolve(set);
    assert!(result.is_ok(), "heterogeneous support");
}

#[test]
fn unsupported_resolution() {
    let set = [0x89283082813FFFF, 0x8828308299FFFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();

    let result = SolventBuilder::new()
        .enable_heterogeneous_support(Resolution::Eight)
        .build()
        .dissolve(set.iter().copied());
    assert!(result.is_err(), "by dup-check");
}

#[test]
fn empty() {
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(std::iter::empty()).expect("geometry");
    assert!(result.0.is_empty());

    let solvent = SolventBuilder::new()
        .enable_heterogeneous_support(Resolution::Nine)
        .build();
    let result = solvent.dissolve(std::iter::empty()).expect("geometry");
    assert!(result.0.is_empty());
}

#[test]
fn hexagon() {
    //  __
    // /  \
    // \__/
    let set = [CellIndex::try_from(0x890DAB6220BFFFF).expect("cell index")];
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 1 polygon.
    assert_eq!(result.0.len(), 1);
    // With a 6-edge outer ring.
    assert_eq!(result.0[0].exterior().lines().count(), 6);
    // And no hole.
    assert!(result.0[0].interiors().is_empty());

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn test_two_contiguous_cells() {
    //     __
    //  __/  \
    // /  \__/
    // \__/
    let set = [0x8928308291BFFFF, 0x89283082957FFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 1 polygon.
    assert_eq!(result.0.len(), 1);
    // With a 10-edge outer ring (6 + 6 - 2 shared edges).
    assert_eq!(result.0[0].exterior().lines().count(), 10);
    // And no hole.
    assert!(result.0[0].interiors().is_empty());

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn two_non_contiguous_cells() {
    //  __      __
    // /  \    /  \
    // \__/    \__/
    let set = [0x8928308291BFFFF, 0x89283082943FFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 2 polygon.
    assert_eq!(result.0.len(), 2);
    // Both with a 6-edge outer ring.
    assert_eq!(result.0[0].exterior().lines().count(), 6);
    assert_eq!(result.0[1].exterior().lines().count(), 6);
    // And no hole.
    assert!(result.0[0].interiors().is_empty());
    assert!(result.0[1].interiors().is_empty());

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn three_contiguous_cells() {
    //     __
    //  __/  \
    // /  \__/
    // \__/  \
    //    \__/
    let set = [0x8928308288BFFFF, 0x892830828D7FFFF, 0x8928308289BFFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 1 polygon.
    assert_eq!(result.0.len(), 1);
    // With a 10-edge outer ring (3*6 - 3*2 shared edges).
    assert_eq!(result.0[0].exterior().lines().count(), 12);
    // And no hole.
    assert!(result.0[0].interiors().is_empty());

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn hole() {
    //      __
    //   __/  \__
    //  /  \__/  \
    //  \__/  \__/
    //  /  \__/  \
    //  \__/  \__/
    //     \__/
    // Hole in the center.
    #[rustfmt::skip]
    let set = [
        0x892830828c7ffff, 0x892830828d7ffff, 0x8928308289bffff,
        0x89283082813ffff, 0x8928308288fffff, 0x89283082883ffff,
    ]
    .into_iter()
    .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 1 polygon.
    assert_eq!(result.0.len(), 1);
    // With a 18-edge outer ring.
    assert_eq!(result.0[0].exterior().lines().count(), 18);
    // And one 6-edge hole.
    assert_eq!(result.0[0].interiors().len(), 1);
    assert_eq!(result.0[0].interiors()[0].lines().count(), 6);

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn pentagon() {
    let set = [CellIndex::try_from(0x851C0003FFFFFFF).expect("cell index")];
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 1 polygon.
    assert_eq!(result.0.len(), 1);
    // With a 10-edge outer ring (distorted pentagon).
    assert_eq!(result.0[0].exterior().lines().count(), 10);
    // And no hole.
    assert!(result.0[0].interiors().is_empty());

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn two_ring() {
    #[rustfmt::skip]
    let set = [
        0x8930062838bffff, 0x8930062838fffff, 0x89300628383ffff,
        0x8930062839bffff, 0x893006283d7ffff, 0x893006283c7ffff,
        0x89300628313ffff, 0x89300628317ffff, 0x893006283bbffff,
        0x89300628387ffff, 0x89300628397ffff, 0x89300628393ffff,
        0x89300628067ffff, 0x8930062806fffff, 0x893006283d3ffff,
        0x893006283c3ffff, 0x893006283cfffff, 0x8930062831bffff,
        0x89300628303ffff,
    ]
    .into_iter()
    .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 1 polygon.
    assert_eq!(result.0.len(), 1);
    // With the expected number of edges on the outer ring.
    assert_eq!(result.0[0].exterior().lines().count(), 6 * (2 * 2 + 1));
    // And no hole.
    assert!(result.0[0].interiors().is_empty());

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn two_ring_unordered() {
    #[rustfmt::skip]
    let set = [
        0x89300628393ffff, 0x89300628383ffff, 0x89300628397ffff,
        0x89300628067ffff, 0x89300628387ffff, 0x893006283bbffff,
        0x89300628313ffff, 0x893006283cfffff, 0x89300628303ffff,
        0x89300628317ffff, 0x8930062839bffff, 0x8930062838bffff,
        0x8930062806fffff, 0x8930062838fffff, 0x893006283d3ffff,
        0x893006283c3ffff, 0x8930062831bffff, 0x893006283d7ffff,
        0x893006283c7ffff,
    ]
    .into_iter()
    .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 1 polygon.
    assert_eq!(result.0.len(), 1);
    // With the expected number of edges on the outer ring.
    assert_eq!(result.0[0].exterior().lines().count(), 6 * (2 * 2 + 1));
    // And no hole.
    assert!(result.0[0].interiors().is_empty());

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn nested_donut() {
    // Hollow 1-ring + hollow 3-ring around the same hex.
    #[rustfmt::skip]
    let set = [
        0x89283082813ffff, 0x8928308281bffff, 0x8928308280bffff,
        0x8928308280fffff, 0x89283082807ffff, 0x89283082817ffff,
        0x8928308289bffff, 0x892830828d7ffff, 0x892830828c3ffff,
        0x892830828cbffff, 0x89283082853ffff, 0x89283082843ffff,
        0x8928308284fffff, 0x8928308287bffff, 0x89283082863ffff,
        0x89283082867ffff, 0x8928308282bffff, 0x89283082823ffff,
        0x89283082837ffff, 0x892830828afffff, 0x892830828a3ffff,
        0x892830828b3ffff, 0x89283082887ffff, 0x89283082883ffff,
    ]
    .into_iter()
    .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 2 polygon.
    assert_eq!(result.0.len(), 2);

    // One has 42-edge outer ring.
    assert_eq!(result.0[0].exterior().lines().count(), 42);
    // And a single 30-edge hole.
    assert_eq!(result.0[0].interiors().len(), 1);
    assert_eq!(result.0[0].interiors()[0].lines().count(), 30);

    // The other has 18-edge outer ring.
    assert_eq!(result.0[1].exterior().lines().count(), 18);
    // And a single 6-edge hole.
    assert_eq!(result.0[1].interiors().len(), 1);
    assert_eq!(result.0[1].interiors()[0].lines().count(), 6);

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn nested_donut_transmeridian() {
    // Hollow 1-ring + hollow 3-ring around the hex at (0, -180).
    #[rustfmt::skip]
    let set = [
        0x897eb5722c7ffff, 0x897eb5722cfffff, 0x897eb572257ffff,
        0x897eb57220bffff, 0x897eb572203ffff, 0x897eb572213ffff,
        0x897eb57266fffff, 0x897eb5722d3ffff, 0x897eb5722dbffff,
        0x897eb573537ffff, 0x897eb573527ffff, 0x897eb57225bffff,
        0x897eb57224bffff, 0x897eb57224fffff, 0x897eb57227bffff,
        0x897eb572263ffff, 0x897eb572277ffff, 0x897eb57223bffff,
        0x897eb572233ffff, 0x897eb5722abffff, 0x897eb5722bbffff,
        0x897eb572287ffff, 0x897eb572283ffff, 0x897eb57229bffff,
    ]
    .into_iter()
    .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 2 polygon.
    assert_eq!(result.0.len(), 2);

    // One has 42-edge outer ring.
    assert_eq!(result.0[0].exterior().lines().count(), 42);
    // And a single 30-edge hole.
    assert_eq!(result.0[0].interiors().len(), 1);
    assert_eq!(result.0[0].interiors()[0].lines().count(), 30);

    // The other has 18-edge outer ring.
    assert_eq!(result.0[1].exterior().lines().count(), 18);
    // And a single 6-edge hole.
    assert_eq!(result.0[1].interiors().len(), 1);
    assert_eq!(result.0[1].interiors()[0].lines().count(), 6);

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn contiguous_distorted() {
    let set = [0x894CC5365AFFFFF, 0x894CC536537FFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    let result = solvent.dissolve(set.iter().copied()).expect("geometry");

    // 1 polygon.
    assert_eq!(result.0.len(), 1);
    assert_eq!(result.0[0].exterior().lines().count(), 12);
    // And no hole.
    assert!(result.0[0].interiors().is_empty());

    assert_hetero_equal_homo(set, Resolution::Nine, &result);
}

#[test]
fn paris_heterogeneous() {
    let resolution = Resolution::Nine;
    let mut tiler = TilerBuilder::new(resolution).build();
    tiler.add(load_polygon("Paris")).expect("add polygon");
    let cells = tiler.into_coverage().collect::<HashSet<_>>();
    let mut compacted = cells.iter().copied().collect::<Vec<_>>();
    CellIndex::compact(&mut compacted).expect("compact");

    let solvent = SolventBuilder::new().build();
    let expected = solvent.dissolve(cells).expect("homo geom");

    assert_hetero_equal_homo(compacted, resolution, &expected);
}

#[test]
fn rabi_heterogeneous() {
    let resolution = Resolution::Nine;
    let mut tiler = TilerBuilder::new(resolution).build();
    tiler.add(load_polygon("Rabi")).expect("add polygon");
    let cells = tiler.into_coverage().collect::<HashSet<_>>();
    let mut compacted = cells.iter().copied().collect::<Vec<_>>();
    CellIndex::compact(&mut compacted).expect("compact");

    let solvent = SolventBuilder::new().build();
    let expected = solvent.dissolve(cells).expect("homo geom");

    assert_hetero_equal_homo(compacted, resolution, &expected);
}

#[test]
fn holes_heterogeneous() {
    let resolution = Resolution::Five;
    let mut tiler = TilerBuilder::new(resolution).build();
    tiler.add(load_polygon("Holes")).expect("add polygon");
    let cells = tiler.into_coverage().collect::<HashSet<_>>();
    let mut compacted = cells.iter().copied().collect::<Vec<_>>();
    CellIndex::compact(&mut compacted).expect("compact");

    let solvent = SolventBuilder::new().build();
    let expected = solvent.dissolve(cells).expect("homo geom");

    assert_hetero_equal_homo(compacted, resolution, &expected);
}

// See https://github.com/HydroniumLabs/h3o/issues/12
#[test]
fn issue_12() {
    let set = [
        0x8B1F25A91526FFF,
        0x8B1F2506D45DFFF,
        0x8B1F25A901A1FFF,
        0x8B1F25324B98FFF,
    ]
    .into_iter()
    .map(|bits| CellIndex::try_from(bits).expect("cell index"))
    .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    let geom = solvent.dissolve(set.iter().copied()).expect("geometry");

    assert_hetero_equal_homo(set, Resolution::Eleven, &geom);
}

// See https://github.com/uber/h3/issues/917
#[test]
fn issue_917() {
    let set = [
        0x81EA3FFFFFFFFFF,
        0x81EABFFFFFFFFFF,
        0x81EAFFFFFFFFFFF,
        0x81EB3FFFFFFFFFF,
        0x81EB7FFFFFFFFFF,
        0x81EBBFFFFFFFFFF,
    ]
    .into_iter()
    .map(|bits| CellIndex::try_from(bits).expect("cell index"))
    .collect::<Vec<_>>();

    let solvent = SolventBuilder::new().build();
    let geom = solvent.dissolve(set.iter().copied()).expect("geometry");
    assert_eq!(geom.0.len(), 1, "a single polygon");
    assert!(geom.0[0].interiors().is_empty(), "no hole");

    assert_hetero_equal_homo(set, Resolution::One, &geom);
}

// See https://github.com/uber/h3/issues/1046
#[test]
fn issue_1046() {
    let set = [0x8001FFFFFFFFFFF, 0x8003FFFFFFFFFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    assert!(solvent.dissolve(set.iter().copied()).is_ok(), "polar");

    let set = [0x8037FFFFFFFFFFF, 0x8051FFFFFFFFFFF]
        .into_iter()
        .map(|bits| CellIndex::try_from(bits).expect("cell index"))
        .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();
    assert!(
        solvent.dissolve(set.iter().copied()).is_ok(),
        "transmeridian"
    );
}

// See https://github.com/uber/h3/issues/1049
#[test]
fn issue_1049() {
    let set = [
        0x827487FFFFFFFFF,
        0x82748FFFFFFFFFF,
        0x827497FFFFFFFFF,
        0x82749FFFFFFFFFF,
        0x8274AFFFFFFFFFF,
        0x8274C7FFFFFFFFF,
        0x8274CFFFFFFFFFF,
        0x8274D7FFFFFFFFF,
        0x8274E7FFFFFFFFF,
        0x8274EFFFFFFFFFF,
        0x8274F7FFFFFFFFF,
        0x82754FFFFFFFFFF,
        0x827C07FFFFFFFFF,
        0x827C27FFFFFFFFF,
        0x827C2FFFFFFFFFF,
        0x827C37FFFFFFFFF,
        0x827D87FFFFFFFFF,
        0x827D8FFFFFFFFFF,
        0x827D97FFFFFFFFF,
        0x827D9FFFFFFFFFF,
        0x827DA7FFFFFFFFF,
        0x827DAFFFFFFFFFF,
        0x82801FFFFFFFFFF,
        0x8280A7FFFFFFFFF,
        0x8280AFFFFFFFFFF,
        0x8280B7FFFFFFFFF,
        0x828197FFFFFFFFF,
        0x82819FFFFFFFFFF,
        0x8281A7FFFFFFFFF,
        0x8281B7FFFFFFFFF,
        0x828207FFFFFFFFF,
        0x82820FFFFFFFFFF,
        0x828227FFFFFFFFF,
        0x82822FFFFFFFFFF,
        0x8282E7FFFFFFFFF,
        0x828307FFFFFFFFF,
        0x82830FFFFFFFFFF,
        0x82831FFFFFFFFFF,
        0x82832FFFFFFFFFF,
        0x828347FFFFFFFFF,
        0x82834FFFFFFFFFF,
        0x828357FFFFFFFFF,
        0x82835FFFFFFFFFF,
        0x828367FFFFFFFFF,
        0x828377FFFFFFFFF,
        0x82A447FFFFFFFFF,
        0x82A457FFFFFFFFF,
        0x82A45FFFFFFFFFF,
        0x82A467FFFFFFFFF,
        0x82A46FFFFFFFFFF,
        0x82A477FFFFFFFFF,
        0x82A4C7FFFFFFFFF,
        0x82A4CFFFFFFFFFF,
        0x82A4D7FFFFFFFFF,
        0x82A4E7FFFFFFFFF,
        0x82A4EFFFFFFFFFF,
        0x82A4F7FFFFFFFFF,
        0x82A547FFFFFFFFF,
        0x82A54FFFFFFFFFF,
        0x82A557FFFFFFFFF,
        0x82A55FFFFFFFFFF,
        0x82A567FFFFFFFFF,
        0x82A577FFFFFFFFF,
        0x82A837FFFFFFFFF,
        0x82A897FFFFFFFFF,
        0x82A8A7FFFFFFFFF,
        0x82A8B7FFFFFFFFF,
        0x82A917FFFFFFFFF,
        0x82A927FFFFFFFFF,
        0x82A937FFFFFFFFF,
        0x82A987FFFFFFFFF,
        0x82A98FFFFFFFFFF,
        0x82A997FFFFFFFFF,
        0x82A99FFFFFFFFFF,
        0x82A9A7FFFFFFFFF,
        0x82A9AFFFFFFFFFF,
        0x82AC47FFFFFFFFF,
        0x82AC57FFFFFFFFF,
        0x82AC5FFFFFFFFFF,
        0x82AC67FFFFFFFFF,
        0x82AC6FFFFFFFFFF,
        0x82AC77FFFFFFFFF,
        0x82AD47FFFFFFFFF,
        0x82AD4FFFFFFFFFF,
        0x82AD57FFFFFFFFF,
        0x82AD5FFFFFFFFFF,
        0x82AD67FFFFFFFFF,
        0x82AD77FFFFFFFFF,
        0x82C207FFFFFFFFF,
        0x82C217FFFFFFFFF,
        0x82C227FFFFFFFFF,
        0x82C237FFFFFFFFF,
        0x82C287FFFFFFFFF,
        0x82C28FFFFFFFFFF,
        0x82C29FFFFFFFFFF,
        0x82C2A7FFFFFFFFF,
        0x82C2AFFFFFFFFFF,
        0x82C2B7FFFFFFFFF,
        0x82C307FFFFFFFFF,
        0x82C317FFFFFFFFF,
        0x82C31FFFFFFFFFF,
        0x82C337FFFFFFFFF,
        0x82CFB7FFFFFFFFF,
        0x82D0C7FFFFFFFFF,
        0x82D0D7FFFFFFFFF,
        0x82D0DFFFFFFFFFF,
        0x82D0E7FFFFFFFFF,
        0x82D0F7FFFFFFFFF,
        0x82D147FFFFFFFFF,
        0x82D157FFFFFFFFF,
        0x82D15FFFFFFFFFF,
        0x82D167FFFFFFFFF,
        0x82D177FFFFFFFFF,
        0x82D187FFFFFFFFF,
        0x82D18FFFFFFFFFF,
        0x82D197FFFFFFFFF,
        0x82D19FFFFFFFFFF,
        0x82D1A7FFFFFFFFF,
        0x82D1AFFFFFFFFFF,
        0x82DC47FFFFFFFFF,
        0x82DC57FFFFFFFFF,
        0x82DC5FFFFFFFFFF,
        0x82DC67FFFFFFFFF,
        0x82DC6FFFFFFFFFF,
        0x82DC77FFFFFFFFF,
        0x82DCC7FFFFFFFFF,
        0x82DCCFFFFFFFFFF,
        0x82DCD7FFFFFFFFF,
        0x82DCE7FFFFFFFFF,
        0x82DCEFFFFFFFFFF,
        0x82DCF7FFFFFFFFF,
        0x82DD1FFFFFFFFFF,
        0x82DD47FFFFFFFFF,
        0x82DD4FFFFFFFFFF,
        0x82DD57FFFFFFFFF,
        0x82DD5FFFFFFFFFF,
        0x82DD6FFFFFFFFFF,
        0x82DD87FFFFFFFFF,
        0x82DD8FFFFFFFFFF,
        0x82DD97FFFFFFFFF,
        0x82DD9FFFFFFFFFF,
        0x82DDAFFFFFFFFFF,
        0x82DDB7FFFFFFFFF,
        0x82DEC7FFFFFFFFF,
        0x82DECFFFFFFFFFF,
        0x82DED7FFFFFFFFF,
        0x82DEE7FFFFFFFFF,
        0x82DEEFFFFFFFFFF,
        0x82DEF7FFFFFFFFF,
        0x82DF0FFFFFFFFFF,
        0x82DF1FFFFFFFFFF,
        0x82DF47FFFFFFFFF,
        0x82DF4FFFFFFFFFF,
        0x82DF57FFFFFFFFF,
        0x82DF5FFFFFFFFFF,
        0x82DF77FFFFFFFFF,
        0x82DF8FFFFFFFFFF,
        0x82DF9FFFFFFFFFF,
        0x82E6C7FFFFFFFFF,
        0x82E6CFFFFFFFFFF,
        0x82E6D7FFFFFFFFF,
        0x82E6DFFFFFFFFFF,
        0x82E6EFFFFFFFFFF,
        0x82E6F7FFFFFFFFF,
    ]
    .into_iter()
    .map(|bits| CellIndex::try_from(bits).expect("cell index"))
    .collect::<Vec<_>>();
    let solvent = SolventBuilder::new().build();

    assert!(solvent.dissolve(set.iter().copied()).is_ok());
}

// This was a non-deterministic (due to hashing) bug in RingHierarchy.
// The antimeredian handling was triggered outside of legit cases.
#[test]
fn non_deterministic_output() {
    for _ in 0..10 {
        let set = [0x871861318FFFFFF, 0x871861383FFFFFF]
            .into_iter()
            .map(|bits| CellIndex::try_from(bits).expect("cell index"))
            .collect::<Vec<_>>();

        let solvent = SolventBuilder::new().build();
        let geom = solvent.dissolve(set.iter().copied()).expect("geometry");

        assert_eq!(geom.0.len(), 2);

        assert_hetero_equal_homo(set, Resolution::Seven, &geom);
    }
}

macro_rules! grid_disk {
    ($name:ident, $base_cell:literal, $resolution:literal) => {
        #[test]
        fn $name() {
            let resolution = Resolution::try_from($resolution).expect("resolution");
            let base_cell = CellIndex::try_from($base_cell).expect("cell index");
            let origin = base_cell.center_child(resolution).expect("center cell");
            let set = origin.grid_disk::<Vec<_>>(2);
            let solvent = SolventBuilder::new().build();
            let result = solvent.dissolve(set.iter().copied()).expect("geometry");
            // Account for pentagon distortion on class II resolution.
            let expected = if base_cell.is_pentagon() {
                if resolution.is_class3() { 30 } else { 25 }
            } else {
                30
            };

            // 1 polygon.
            assert_eq!(result.0.len(), 1);
            assert_eq!(result.0[0].exterior().lines().count(), expected);
            // And no hole.
            assert!(result.0[0].interiors().is_empty());

            assert_hetero_equal_homo(set, resolution, &result);
        }
    };
}

grid_disk!(grid_disk_res1, 0x8073FFFFFFFFFFF, 1);
grid_disk!(grid_disk_res2, 0x8073FFFFFFFFFFF, 2);
grid_disk!(grid_disk_res3, 0x8073FFFFFFFFFFF, 3);
grid_disk!(grid_disk_res4, 0x8073FFFFFFFFFFF, 4);
grid_disk!(grid_disk_res5, 0x8073FFFFFFFFFFF, 5);
grid_disk!(grid_disk_res6, 0x8073FFFFFFFFFFF, 6);
grid_disk!(grid_disk_res7, 0x8073FFFFFFFFFFF, 7);
grid_disk!(grid_disk_res8, 0x8073FFFFFFFFFFF, 8);
grid_disk!(grid_disk_res9, 0x8073FFFFFFFFFFF, 9);
grid_disk!(grid_disk_res10, 0x8073FFFFFFFFFFF, 10);
grid_disk!(grid_disk_res11, 0x8073FFFFFFFFFFF, 11);
grid_disk!(grid_disk_res12, 0x8073FFFFFFFFFFF, 12);
grid_disk!(grid_disk_res13, 0x8073FFFFFFFFFFF, 13);
grid_disk!(grid_disk_res14, 0x8073FFFFFFFFFFF, 14);
grid_disk!(grid_disk_res15, 0x8073FFFFFFFFFFF, 15);

grid_disk!(grid_disk_pentagon_res1, 0x8031FFFFFFFFFFF, 1);
grid_disk!(grid_disk_pentagon_res2, 0x8031FFFFFFFFFFF, 2);
grid_disk!(grid_disk_pentagon_res3, 0x8031FFFFFFFFFFF, 3);
grid_disk!(grid_disk_pentagon_res4, 0x8031FFFFFFFFFFF, 4);
grid_disk!(grid_disk_pentagon_res5, 0x8031FFFFFFFFFFF, 5);
grid_disk!(grid_disk_pentagon_res6, 0x8031FFFFFFFFFFF, 6);
grid_disk!(grid_disk_pentagon_res7, 0x8031FFFFFFFFFFF, 7);
grid_disk!(grid_disk_pentagon_res8, 0x8031FFFFFFFFFFF, 8);
grid_disk!(grid_disk_pentagon_res9, 0x8031FFFFFFFFFFF, 9);
grid_disk!(grid_disk_pentagon_res10, 0x8031FFFFFFFFFFF, 10);
grid_disk!(grid_disk_pentagon_res11, 0x8031FFFFFFFFFFF, 11);
grid_disk!(grid_disk_pentagon_res12, 0x8031FFFFFFFFFFF, 12);
grid_disk!(grid_disk_pentagon_res13, 0x8031FFFFFFFFFFF, 13);
grid_disk!(grid_disk_pentagon_res14, 0x8031FFFFFFFFFFF, 14);
grid_disk!(grid_disk_pentagon_res15, 0x8031FFFFFFFFFFF, 15);

// -----------------------------------------------------------------------------

/// Returns true if two LineString are equivalent.
///
/// LineString are equivalent if they contains the same point in the same order
/// (but they don't necessarily start at the same point).
fn assert_line_string_equivalent(line1: &LineString, line2: &LineString, epsilon: f64) {
    assert!(line1.is_closed(), "line1 is a LinearRing");
    let mut coords1 = line1.coords().collect::<Vec<_>>();
    coords1.pop(); // Remove the duplicated coord that close the ring

    assert!(line2.is_closed(), "line2 is a LinearRing");
    let mut coords2 = line2.coords().collect::<Vec<_>>();
    coords2.pop(); // Remove the duplicated coord that close the ring

    assert_eq!(coords1.len(), coords2.len(), "size mismatch");
    let offset = coords2
        .iter()
        .position(|&coord| relative_eq!(coord, coords1[0], epsilon = epsilon))
        .expect("lines are different");
    for i in 0..coords1.len() {
        assert_relative_eq!(
            coords1[i],
            coords2[(i + offset) % coords2.len()],
            epsilon = epsilon
        );
    }
}

fn assert_polygon_equivalent(poly1: &Polygon, poly2: &Polygon, epsilon: f64) {
    let holes1 = poly1.interiors();
    let holes2 = poly2.interiors();

    assert_eq!(holes1.len(), holes2.len(), "holes count mismatch");
    for (hole1, hole2) in holes1.iter().zip(holes2.iter()) {
        assert_line_string_equivalent(hole1, hole2, epsilon);
    }
    assert_line_string_equivalent(poly1.exterior(), poly2.exterior(), epsilon);
}

fn assert_multipolygon_equivalent(mpoly1: &MultiPolygon, mpoly2: &MultiPolygon, epsilon: f64) {
    assert_eq!(mpoly1.0.len(), mpoly2.0.len(), "polygon count mismatch");
    for (poly1, poly2) in mpoly1.0.iter().zip(mpoly2.0.iter()) {
        assert_polygon_equivalent(poly1, poly2, epsilon);
    }
}

fn assert_hetero_equal_homo(
    cells: impl IntoIterator<Item = CellIndex>,
    resolution: Resolution,
    expected: &MultiPolygon,
) {
    let solvent = SolventBuilder::new()
        .enable_heterogeneous_support(resolution)
        .build();
    let result = solvent.dissolve(cells).expect("geometry");
    let union_area = result.union(expected).unsigned_area();
    let intersection_area = result.union(expected).unsigned_area();
    assert_eq!(intersection_area / union_area, 1., "geom mismatch")
}
