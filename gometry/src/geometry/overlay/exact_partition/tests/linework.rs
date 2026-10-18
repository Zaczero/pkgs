fn assert_ring_xy(ring: &Ring, xs: &[f64], ys: &[f64]) {
    assert_eq!(ring.coords().xs(), xs);
    assert_eq!(ring.coords().ys(), ys);
}

fn polygon_with_shell_xy<'a>(polygons: &'a [Shape], xs: &[f64], ys: &[f64]) -> &'a Polygon {
    polygons
        .iter()
        .filter_map(polygon_output)
        .find(|polygon| polygon.shell.coords().xs() == xs && polygon.shell.coords().ys() == ys)
        .expect("polygon with the literal shell is emitted")
}

fn polygon_output(shape: &Shape) -> Option<&Polygon> {
    match shape {
        Shape::Polygon(polygon) => Some(polygon),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => None,
    }
}

fn line_string_output(shape: &Shape) -> Option<&LineSeq> {
    match shape {
        Shape::LineString(line) => Some(line),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => None,
    }
}

#[test]
fn linework_selection_keeps_minimal_faces_and_disconnected_support() {
    let crossing = [
        seg(0.0, 0.0, 2.0, 2.0),
        seg(0.0, 2.0, 2.0, 0.0),
        seg(0.0, 0.0, 2.0, 0.0),
        seg(2.0, 0.0, 2.0, 2.0),
        seg(2.0, 2.0, 0.0, 2.0),
        seg(0.0, 2.0, 0.0, 0.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&crossing).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "partition");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let faces = inventory
        .linework_polygons(
            &mut partition,
            false,
            "test",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(faces.len(), 4);
    for (xs, ys) in [
        (&[0.0, 2.0, 1.0, 0.0][..], &[0.0, 0.0, 1.0, 0.0][..]),
        (&[1.0, 2.0, 2.0, 1.0][..], &[1.0, 0.0, 2.0, 1.0][..]),
        (&[0.0, 1.0, 2.0, 0.0][..], &[2.0, 1.0, 2.0, 2.0][..]),
        (&[0.0, 1.0, 0.0, 0.0][..], &[0.0, 1.0, 2.0, 0.0][..]),
    ] {
        let polygon = polygon_with_shell_xy(&faces, xs, ys);
        assert!(
            polygon.holes.is_empty(),
            "each crossing-square cell is a simple triangle"
        );
    }

    let disconnected = [
        seg(0.0, 0.0, 2.0, 2.0),
        seg(2.0, 2.0, 0.0, 2.0),
        seg(0.0, 2.0, 2.0, 0.0),
        seg(2.0, 0.0, 0.0, 0.0),
        seg(5.0, 0.0, 6.0, 0.0),
        seg(6.0, 0.0, 6.0, 1.0),
        seg(6.0, 1.0, 5.0, 1.0),
        seg(5.0, 1.0, 5.0, 0.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&disconnected).unwrap();
    let mut budget = ExpansionBudget::new("build_area", "partition");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let parts = inventory
        .linework_polygons(
            &mut partition,
            true,
            "test",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(parts.len(), 3);
    approx::assert_relative_eq!(
        parts.iter().map(Shape::area).sum::<f64>(),
        3.0,
        max_relative = 64.0 * f64::EPSILON,
    );
}

#[test]
fn polygonize_emission_preserves_hosts_across_disconnected_cycles() {
    let segments = [
        seg(0.0, 0.0, 8.0, 0.0),
        seg(8.0, 0.0, 8.0, 8.0),
        seg(8.0, 8.0, 0.0, 8.0),
        seg(0.0, 8.0, 0.0, 0.0),
        seg(2.0, 2.0, 4.0, 2.0),
        seg(4.0, 2.0, 4.0, 4.0),
        seg(4.0, 4.0, 2.0, 4.0),
        seg(2.0, 4.0, 2.0, 2.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "partition");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 2);
    let annulus = polygons
        .first()
        .and_then(polygon_output)
        .expect("the annulus face is emitted");
    let inner_face = polygons
        .get(1)
        .and_then(polygon_output)
        .expect("the inner face is emitted");
    assert_ring_xy(&annulus.shell, &[0.0, 8.0, 8.0, 0.0, 0.0], &[
        0.0, 0.0, 8.0, 8.0, 0.0,
    ]);
    assert_eq!(annulus.holes.len(), 1);
    let hole = annulus.holes.first().expect("the annulus has its hole");
    assert_ring_xy(hole, &[2.0, 2.0, 4.0, 4.0, 2.0], &[2.0, 4.0, 4.0, 2.0, 2.0]);
    assert_ring_xy(&inner_face.shell, &[2.0, 4.0, 4.0, 2.0, 2.0], &[
        2.0, 2.0, 4.0, 4.0, 2.0,
    ]);
    assert!(inner_face.holes.is_empty());

    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let emission = emission::polygonize_emission(&partition, &topology, &edges, &global).unwrap();
    let emissions = [&emission];

    let nearest = embedding::Embedding::nearest(&partition);
    let nearest_segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(nearest.coordinates(), item))
        .collect();
    let nearest_segment_views = [nearest_segments.as_slice()];
    let mut nearest_witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &nearest,
            &emissions,
            &nearest_segment_views,
            &mut nearest_witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(nearest_witnesses.0.is_empty());

    let mut coordinates = partition.points.rounded.clone();
    for (from, to) in [
        (XY::new(2.0, 2.0), XY::new(10.0, 10.0)),
        (XY::new(4.0, 2.0), XY::new(12.0, 10.0)),
        (XY::new(4.0, 4.0), XY::new(12.0, 12.0)),
        (XY::new(2.0, 4.0), XY::new(10.0, 12.0)),
    ] {
        let node = partition.stored_node(from).unwrap();
        coordinates[node.0 as usize] = to;
    }
    let moved = embedding::Embedding::from_owned(coordinates);
    let moved_segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(moved.coordinates(), item))
        .collect();
    let moved_segment_views = [moved_segments.as_slice()];
    let mut moved_witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &moved,
            &emissions,
            &moved_segment_views,
            &mut moved_witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(matches!(moved_witnesses.0.as_slice(), [
        embedding::Witness::Face { .. }
    ]));
}

#[test]
fn polygonize_full_certifies_diagnostic_lines_with_polygon_faces() {
    let segments = [
        seg(0.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 0.0, 0.0),
        seg(-1.0, 2.0, 5.0, 2.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize_full", "partition");
    let mut partition = inventory.partition(&mut budget).unwrap();
    partition.kept_nodes = KeptNodes::All;
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let diagnostic_spans: Vec<_> = (0..partition.spans.len())
        .map(|span| {
            partition
                .owners(span)
                .iter()
                .any(|&(source, _)| source == 4)
        })
        .collect();
    let emission = emission::polygonize_full_emission(
        &partition,
        &topology,
        &edges,
        &global,
        &diagnostic_spans,
    )
    .unwrap();
    assert!(emission.areal_chords > 0);
    assert!(emission.chords > emission.areal_chords);
    assert_eq!(emission.fan_spans.len(), emission.chords);
    assert_eq!(emission.faces.as_ref().map(Vec::len), Some(emission.chords));
    let area_nodes: Vec<_> = emission.items[..emission.areal_chords]
        .iter()
        .flatten()
        .copied()
        .collect();
    assert!(
        emission.items[emission.areal_chords..]
            .iter()
            .flatten()
            .any(|node| area_nodes.contains(node)),
        "diagnostic dangles meet the polygon boundary at emitted nodes"
    );

    let nearest = embedding::Embedding::nearest(&partition);
    let segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(nearest.coordinates(), item))
        .collect();
    let emissions = [&emission];
    let segment_views = [segments.as_slice()];
    let mut witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &nearest,
            &emissions,
            &segment_views,
            &mut witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(witnesses.0.is_empty());

    let full = inventory
        .polygonize_full(
            &mut partition,
            CoordinateAxes::XY,
            &[CoordinateAxes::XY],
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(full.polygons.len(), 2);
    let lower = polygon_output(full.polygons.first().expect("the lower face is emitted"))
        .expect("polygonize_full emits polygon faces");
    let upper = polygon_output(full.polygons.get(1).expect("the upper face is emitted"))
        .expect("polygonize_full emits polygon faces");
    assert_ring_xy(&lower.shell, &[0.0, 4.0, 4.0, 0.0, 0.0], &[
        0.0, 0.0, 2.0, 2.0, 0.0,
    ]);
    assert_ring_xy(&upper.shell, &[0.0, 4.0, 4.0, 0.0, 0.0], &[
        2.0, 2.0, 4.0, 4.0, 2.0,
    ]);
    assert_eq!(full.dangles.len(), 1);
    let row = full
        .dangles
        .first()
        .expect("the source has one diagnostic row");
    assert_eq!(row.len(), 2);
    let right = line_string_output(row.first().expect("the right dangle is emitted"))
        .expect("diagnostic dangles are line strings");
    let left = line_string_output(row.get(1).expect("the left dangle is emitted"))
        .expect("diagnostic dangles are line strings");
    assert_eq!(right.as_coords().xs(), &[4.0, 5.0]);
    assert_eq!(right.as_coords().ys(), &[2.0, 2.0]);
    assert_eq!(left.as_coords().xs(), &[-1.0, 0.0]);
    assert_eq!(left.as_coords().ys(), &[2.0, 2.0]);
}

#[test]
fn polygonize_faces_attach_closed_holes_to_their_literal_hosts() {
    let segments = [
        seg(0.0, 0.0, 8.0, 0.0),
        seg(8.0, 0.0, 8.0, 8.0),
        seg(8.0, 8.0, 0.0, 8.0),
        seg(0.0, 8.0, 0.0, 0.0),
        seg(2.0, 2.0, 4.0, 2.0),
        seg(4.0, 2.0, 4.0, 4.0),
        seg(4.0, 4.0, 2.0, 4.0),
        seg(2.0, 4.0, 2.0, 2.0),
        seg(12.0, 0.0, 20.0, 0.0),
        seg(20.0, 0.0, 20.0, 8.0),
        seg(20.0, 8.0, 12.0, 8.0),
        seg(12.0, 8.0, 12.0, 0.0),
        seg(14.0, 2.0, 17.0, 2.0),
        seg(17.0, 2.0, 17.0, 5.0),
        seg(17.0, 5.0, 14.0, 5.0),
        seg(14.0, 5.0, 14.0, 2.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "two closed hole hosts");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 4);

    let outer_a = polygon_with_shell_xy(&polygons, &[0.0, 8.0, 8.0, 0.0, 0.0], &[
        0.0, 0.0, 8.0, 8.0, 0.0,
    ]);
    assert_eq!(outer_a.holes.len(), 1);
    let hole_a = outer_a
        .holes
        .first()
        .expect("the first annulus has its hole");
    assert_ring_xy(hole_a, &[2.0, 2.0, 4.0, 4.0, 2.0], &[
        2.0, 4.0, 4.0, 2.0, 2.0,
    ]);

    let inner_a = polygon_with_shell_xy(&polygons, &[2.0, 4.0, 4.0, 2.0, 2.0], &[
        2.0, 2.0, 4.0, 4.0, 2.0,
    ]);
    assert!(inner_a.holes.is_empty());

    let outer_b = polygon_with_shell_xy(&polygons, &[12.0, 20.0, 20.0, 12.0, 12.0], &[
        0.0, 0.0, 8.0, 8.0, 0.0,
    ]);
    assert_eq!(outer_b.holes.len(), 1);
    let hole_b = outer_b
        .holes
        .first()
        .expect("the second annulus has its hole");
    assert_ring_xy(hole_b, &[14.0, 14.0, 17.0, 17.0, 14.0], &[
        2.0, 5.0, 5.0, 2.0, 2.0,
    ]);

    let inner_b = polygon_with_shell_xy(&polygons, &[14.0, 17.0, 17.0, 14.0, 14.0], &[
        2.0, 2.0, 5.0, 5.0, 2.0,
    ]);
    assert!(inner_b.holes.is_empty());
}

#[test]
fn polygonize_faces_keep_point_touching_and_tangent_hole_cycles_separate() {
    let tangent_hole = [
        seg(0.0, 0.0, 6.0, 0.0),
        seg(6.0, 0.0, 6.0, 6.0),
        seg(6.0, 6.0, 0.0, 6.0),
        seg(0.0, 6.0, 0.0, 0.0),
        seg(0.0, 3.0, 2.0, 2.0),
        seg(2.0, 2.0, 2.0, 4.0),
        seg(2.0, 4.0, 0.0, 3.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&tangent_hole).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "tangent hole cycles");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 2);
    let outer = polygons
        .iter()
        .filter_map(polygon_output)
        .find(|polygon| polygon.shell.coords().xs().contains(&6.0))
        .expect("the outer bounded face is emitted");
    assert_ring_xy(&outer.shell, &[0.0, 6.0, 6.0, 0.0, 0.0, 0.0], &[
        0.0, 0.0, 6.0, 6.0, 3.0, 0.0,
    ]);
    assert_eq!(outer.holes.len(), 1);
    let hole = outer
        .holes
        .first()
        .expect("the outer face has its tangent hole");
    assert_ring_xy(hole, &[0.0, 2.0, 2.0, 0.0], &[3.0, 4.0, 2.0, 3.0]);
    let triangle = polygons
        .iter()
        .filter_map(polygon_output)
        .find(|polygon| polygon.shell.coords().xs().iter().all(|&x| x <= 2.0))
        .expect("the bounded tangent triangle is emitted independently");
    assert_ring_xy(&triangle.shell, &[0.0, 2.0, 2.0, 0.0], &[
        3.0, 2.0, 4.0, 3.0,
    ]);
    assert!(triangle.holes.is_empty());

    let point_touch = [
        seg(0.0, 0.0, 2.0, 0.0),
        seg(2.0, 0.0, 2.0, 2.0),
        seg(2.0, 2.0, 0.0, 2.0),
        seg(0.0, 2.0, 0.0, 0.0),
        seg(2.0, 2.0, 4.0, 2.0),
        seg(4.0, 2.0, 4.0, 4.0),
        seg(4.0, 4.0, 2.0, 4.0),
        seg(2.0, 4.0, 2.0, 2.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&point_touch).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "point-touching faces");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 2);
    let first = polygons
        .iter()
        .filter_map(polygon_output)
        .find(|polygon| polygon.shell.coords().xs().contains(&0.0))
        .expect("the left point-touching face is emitted");
    assert_ring_xy(&first.shell, &[0.0, 2.0, 2.0, 0.0, 0.0], &[
        0.0, 0.0, 2.0, 2.0, 0.0,
    ]);
    assert!(first.holes.is_empty());
    let second = polygons
        .iter()
        .filter_map(polygon_output)
        .find(|polygon| polygon.shell.coords().xs().contains(&4.0))
        .expect("the right point-touching face is emitted separately");
    assert_ring_xy(&second.shell, &[2.0, 4.0, 4.0, 2.0, 2.0], &[
        2.0, 2.0, 4.0, 4.0, 2.0,
    ]);
    assert!(second.holes.is_empty());
}

#[test]
fn polygonize_keeps_collinear_partition_nodes_in_serialized_and_certified_boundaries() {
    let segments = [
        seg(0.0, 0.0, 1.0, 0.0),
        seg(1.0, 0.0, 2.0, 0.0),
        seg(2.0, 0.0, 2.0, 2.0),
        seg(2.0, 2.0, 0.0, 2.0),
        seg(0.0, 2.0, 0.0, 0.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "collinear partition node");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 1);
    let polygon = polygons
        .first()
        .and_then(polygon_output)
        .expect("the closed linework emits a face");
    assert_ring_xy(&polygon.shell, &[0.0, 1.0, 2.0, 2.0, 0.0, 0.0], &[
        0.0, 0.0, 0.0, 2.0, 2.0, 0.0,
    ]);
    let node = partition
        .stored_node(XY::new(1.0, 0.0))
        .expect("the collinear source endpoint is a partition node");
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let emission = emission::polygonize_emission(&partition, &topology, &edges, &global).unwrap();
    assert!(
        emission.items.iter().any(|item| item.contains(&node)),
        "the embedding certificate includes each serialized boundary node"
    );
}

#[test]
fn polygonize_faces_sort_global_faces_after_dangling_walks() {
    let segments = [
        seg(1.0, 1.0, 2.0, 1.0),
        seg(6.0, 0.0, 7.0, 0.0),
        seg(7.0, 0.0, 7.0, 1.0),
        seg(7.0, 1.0, 6.0, 1.0),
        seg(6.0, 1.0, 6.0, 0.0),
        seg(2.0, 1.0, 2.0, 0.0),
        seg(0.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 0.0, 0.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "source-order face labels");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let walks = global.boundary_walks(&topology, |face, other| face != 0 && face != other);
    let mut shell_faces = Vec::new();
    for walk in &walks {
        let first = *walk.first().expect("a boundary walk is nonempty");
        let local_face = *topology
            .face_of
            .get(first as usize)
            .expect("boundary dart belongs to the topology");
        let face = *global
            .walk_faces
            .get(local_face as usize)
            .expect("local face belongs to the global face map");
        if emission::walk_is_shell(
            &partition,
            &topology,
            walk.len(),
            |step| {
                *walk
                    .get(step)
                    .expect("shell classifier step belongs to the boundary walk")
            },
            |step| {
                *walk
                    .get(step)
                    .expect("shell classifier step belongs to the boundary walk")
            },
        )
        .unwrap()
        {
            shell_faces.push(face);
        }
    }
    assert_eq!(shell_faces, [2, 1], "walk discovery precedes face sorting");

    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 2);
    let a = polygons
        .first()
        .and_then(polygon_output)
        .expect("the first square face is emitted");
    let b = polygons
        .get(1)
        .and_then(polygon_output)
        .expect("the second square face is emitted");
    assert_ring_xy(&a.shell, &[0.0, 2.0, 4.0, 4.0, 0.0, 0.0], &[
        0.0, 0.0, 0.0, 4.0, 4.0, 0.0,
    ]);
    assert!(a.holes.is_empty());
    assert_ring_xy(&b.shell, &[6.0, 7.0, 7.0, 6.0, 6.0], &[
        0.0, 0.0, 1.0, 1.0, 0.0,
    ]);
    assert!(b.holes.is_empty());
}

#[test]
fn polygonize_faces_serialize_the_supplied_certified_node_table() {
    let segments = [
        seg(-2.0, 0.0, 2.0, 0.0),
        seg(-1.0, -2.0, 1.0, 4.0),
        seg(-1.0, 2.0, 2.0, -1.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "certified constructed vertex");
    let partition = inventory.partition(&mut budget).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let emission = emission::polygonize_emission(&partition, &topology, &edges, &global).unwrap();
    let nearest = embedding::Embedding::nearest(&partition);
    let crossing = partition
        .crossings()
        .into_iter()
        .find(|&node| partition.nearest_xy(node).x.to_bits() == 0xBFD5_5555_5555_5555)
        .expect("the left crossing rounds to negative one third");
    let nearest_xy = partition.nearest_xy(crossing);
    assert_eq!(nearest_xy.y.to_bits(), 0_u64);
    let candidate_xy = XY::new(f64::from_bits(0xBFD5_5555_5555_5554), nearest_xy.y);
    let mut coordinates = nearest.coordinates().to_vec();
    *coordinates
        .get_mut(crossing.0 as usize)
        .expect("the constructed node belongs to the embedding table") = candidate_xy;
    let supplied = embedding::Embedding::from_owned(coordinates);
    for point in [
        XY::new(-2.0, 0.0),
        XY::new(2.0, 0.0),
        XY::new(-1.0, -2.0),
        XY::new(1.0, 4.0),
        XY::new(-1.0, 2.0),
        XY::new(2.0, -1.0),
    ] {
        let node = partition
            .stored_node(point)
            .expect("source vertex is stored");
        assert_eq!(
            supplied.coordinates().get(node.0 as usize),
            Some(&point),
            "source vertices stay pinned at {point:?}"
        );
    }
    let emitted_segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(supplied.coordinates(), item))
        .collect();
    let segment_views = [emitted_segments.as_slice()];
    let mut witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &supplied,
            &[&emission],
            &segment_views,
            &mut witnesses,
        )
        .unwrap()
        .is_continue(),
        "the complete supplied coordinate table passes the production certificate"
    );
    assert!(witnesses.0.is_empty());

    let groups =
        emission::polygonize_node_groups(&partition, &topology, &global, &supplied).unwrap();
    let polygons = inventory
        .polygon_shapes_from_node_groups(
            &partition,
            &supplied,
            groups,
            CoordinateAxes::XY,
            false,
            "polygonize",
            &crate::geometry::SourceArcContext::cartesian(),
            None,
        )
        .unwrap();
    assert_eq!(polygons.len(), 1);
    let polygon = polygons
        .first()
        .and_then(polygon_output)
        .expect("the bounded triangle is emitted");
    assert_eq!(
        polygon
            .shell
            .coords()
            .xs()
            .iter()
            .map(|x| x.to_bits())
            .collect::<Vec<_>>(),
        [
            0xBFD5_5555_5555_5554,
            0x3FF0_0000_0000_0000,
            0,
            0xBFD5_5555_5555_5554
        ]
    );
    assert_eq!(polygon.shell.coords().ys(), &[0.0, 0.0, 1.0, 0.0]);
    assert!(polygon.holes.is_empty());
}

#[rstest::rstest]
#[case::empty(&[][..])]
#[case::seg_0_0_0_0_0_0_0_0(&[seg(0.0, 0.0, 0.0, 0.0)][..])]
#[case::seg_0_0_0_0_2_0_0_0(&[seg(0.0, 0.0, 2.0, 0.0)][..])]
fn polygonize_returns_no_polygons_without_a_bounded_face(#[case] segments: &[Segment]) {
    let inventory = ExactSourceInventory::from_segments(segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "linework without a bounded face");
    let mut partition = inventory.partition(&mut budget).unwrap();
    assert!(
        inventory
            .linework_polygons(
                &mut partition,
                false,
                "polygonize",
                CoordinateAxes::XY,
                false,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap()
            .is_empty()
    );
}
