#[test]
fn point_append_admission_rejects_when_count_is_u32_max() {
    let u32_max = usize::try_from(u32::MAX).expect("u32 count fits this supported target");
    let last_id = u32::MAX.wrapping_sub(1);
    assert_eq!(
        construction::point_id_for_append(u32_max.wrapping_sub(1))
            .expect("the maximum supported count can be reached"),
        last_id
    );
    assert!(matches!(
        construction::point_id_for_append(u32_max),
        Err(error)
            if matches!(
                error.kind(),
                crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
            )
    ));
}

#[test]
fn segment_contact_classifies_pairs_and_defers_collinear_spans() {
    let segment = |start: (f64, f64), end: (f64, f64)| Segment {
        start: XY::new(start.0, start.1),
        end: XY::new(end.0, end.1),
    };
    let cases = [
        (
            segment((0.0, 0.0), (1.0, 0.0)),
            segment((3.0, 0.0), (4.0, 1.0)),
            Contact::Disjoint,
            None,
            None,
        ),
        (
            segment((0.0, 0.0), (4.0, 4.0)),
            segment((0.0, 4.0), (4.0, 0.0)),
            Contact::Cross,
            None,
            None,
        ),
        (
            segment((0.0, 0.0), (2.0, 0.0)),
            segment((2.0, 0.0), (2.0, 2.0)),
            Contact::Touch(TouchLocation::Shared(TouchEndpoint::RightStart)),
            None,
            Some(XY::new(2.0, 0.0)),
        ),
        (
            segment((0.0, 0.0), (4.0, 0.0)),
            segment((2.0, -2.0), (2.0, 0.0)),
            Contact::Touch(TouchLocation::LeftInterior(SegmentEndpoint::End)),
            None,
            Some(XY::new(2.0, 0.0)),
        ),
        (
            segment((0.0, 0.0), (4.0, 0.0)),
            segment((2.0, 0.0), (2.0, 2.0)),
            Contact::Touch(TouchLocation::LeftInterior(SegmentEndpoint::Start)),
            None,
            Some(XY::new(2.0, 0.0)),
        ),
        (
            segment((2.0, -2.0), (2.0, 0.0)),
            segment((0.0, 0.0), (4.0, 0.0)),
            Contact::Touch(TouchLocation::RightInterior(SegmentEndpoint::End)),
            None,
            Some(XY::new(2.0, 0.0)),
        ),
        (
            segment((0.0, 0.0), (4.0, 0.0)),
            segment((2.0, 0.0), (6.0, 0.0)),
            Contact::Collinear,
            Some(SharedSpan {
                same_direction: true,
                start: XY::new(2.0, 0.0),
                end: XY::new(4.0, 0.0),
            }),
            None,
        ),
        (
            segment((0.0, 0.0), (2.0, 0.0)),
            segment((2.0, 0.0), (4.0, 0.0)),
            Contact::Collinear,
            Some(SharedSpan {
                same_direction: true,
                start: XY::new(2.0, 0.0),
                end: XY::new(2.0, 0.0),
            }),
            None,
        ),
        (
            segment((2.0, 0.0), (2.0, 0.0)),
            segment((0.0, 0.0), (4.0, 0.0)),
            Contact::Touch(TouchLocation::RightInterior(SegmentEndpoint::Start)),
            None,
            Some(XY::new(2.0, 0.0)),
        ),
        (
            segment((0.0, 0.0), (0.0, 0.0)),
            segment((0.0, 0.0), (4.0, 0.0)),
            Contact::Touch(TouchLocation::Shared(TouchEndpoint::LeftStart)),
            None,
            Some(XY::new(0.0, 0.0)),
        ),
        (
            segment((0.0, 0.0), (4.0, 4.0)),
            segment((4.0, 4.0), (0.0, 0.0)),
            Contact::Collinear,
            Some(SharedSpan {
                same_direction: false,
                start: XY::new(0.0, 0.0),
                end: XY::new(4.0, 4.0),
            }),
            None,
        ),
    ];
    for (index, (left, right, expected, expected_span, expected_touch)) in
        cases.into_iter().enumerate()
    {
        let contact = segment_contact_exact(left, right);
        assert_eq!(contact, expected, "pair {index}");
        let actual_touch = match contact {
            Contact::Touch(endpoint) => Some(endpoint.point(left, right)),
            Contact::Disjoint | Contact::Cross | Contact::Collinear => None,
        };
        assert_eq!(actual_touch, expected_touch, "touch point for pair {index}");
        assert_eq!(
            contact.shared_span(left, right),
            expected_span,
            "shared span for pair {index}",
        );
    }
}

#[test]
fn partition_projection_matches_contact_for_seeded_degenerate_pairs() {
    fn next(state: &mut u64) -> u64 {
        *state = (*state).wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = *state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn point(state: &mut u64) -> (i32, i32) {
        let x = (next(state) % 17) as i32 - 8;
        let y = (next(state) % 17) as i32 - 8;
        (x, y)
    }

    fn offset(origin: (i32, i32), delta: (i32, i32), scale: i32) -> (i32, i32) {
        (origin.0 + delta.0 * scale, origin.1 + delta.1 * scale)
    }

    let segment = |start: (i32, i32), end: (i32, i32)| Segment {
        start: XY::new(f64::from(start.0), f64::from(start.1)),
        end: XY::new(f64::from(end.0), f64::from(end.1)),
    };
    let mut pairs = vec![
        (segment((0, 0), (2, 0)), segment((4, 0), (4, 1))),
        (segment((0, 0), (4, 4)), segment((0, 4), (4, 0))),
        (segment((0, 0), (2, 0)), segment((2, 0), (2, 2))),
        (segment((0, 0), (4, 0)), segment((2, -2), (2, 0))),
        (segment((2, -2), (2, 0)), segment((0, 0), (4, 0))),
        (segment((0, 0), (4, 0)), segment((2, 0), (6, 0))),
        (segment((0, 0), (2, 0)), segment((2, 0), (4, 0))),
        (segment((0, 0), (0, 0)), segment((0, 0), (4, 0))),
        (segment((2, 0), (2, 0)), segment((0, 0), (4, 0))),
        (segment((0, 1), (0, 1)), segment((0, 0), (4, 0))),
    ];

    let mut seed = 0x7A31_09D5_C462_EF81;
    for index in 0..512 {
        let left_start = point(&mut seed);
        let left_end = if index % 17 == 0 {
            left_start
        } else {
            point(&mut seed)
        };
        let right_start = match index % 5 {
            0 => left_start,
            1 => left_end,
            _ => point(&mut seed),
        };
        let right_end = if index % 19 == 0 {
            right_start
        } else if index % 11 == 0 {
            left_start
        } else {
            point(&mut seed)
        };
        pairs.push((
            segment(left_start, left_end),
            segment(right_start, right_end),
        ));
    }
    for _ in 0..128 {
        let origin = point(&mut seed);
        let mut delta = (
            (next(&mut seed) % 7) as i32 - 3,
            (next(&mut seed) % 7) as i32 - 3,
        );
        if delta == (0, 0) {
            delta.0 = 1;
        }
        let left = segment(origin, offset(origin, delta, 4));
        let right = segment(
            offset(origin, delta, (next(&mut seed) % 8) as i32 - 2),
            offset(origin, delta, (next(&mut seed) % 8) as i32 - 2),
        );
        pairs.push((left, right));
    }
    for _ in 0..64 {
        let origin = point(&mut seed);
        let mut delta = (
            (next(&mut seed) % 5) as i32 - 2,
            (next(&mut seed) % 5) as i32 - 2,
        );
        if delta == (0, 0) {
            delta.0 = 1;
        }
        let middle = offset(origin, delta, 2);
        let perpendicular = (-delta.1, delta.0);
        pairs.push((
            segment(origin, offset(origin, delta, 4)),
            segment(middle, offset(middle, perpendicular, 2)),
        ));
    }

    let mut saw_disjoint = false;
    let mut saw_shared = false;
    let mut saw_left_interior = false;
    let mut saw_right_interior = false;
    let mut saw_collinear = false;
    let mut saw_cross = false;

    for (pair_index, (left, right)) in pairs.into_iter().enumerate() {
        let expected = segment_contact_exact(left, right);
        let unique = [left, right];
        let mut forest = construction::Forest::with_capacity(&unique, 4);
        let mut start_handles = Vec::with_capacity(unique.len());
        let mut end_handles = Vec::with_capacity(unique.len());
        for segment in &unique {
            start_handles.push(
                forest
                    .push_stored(segment.start)
                    .expect("stored start handle admits"),
            );
            end_handles.push(
                forest
                    .push_stored(segment.end)
                    .expect("stored end handle admits"),
            );
        }
        let handles =
            construction::EndpointHandles::from_columns(&start_handles, &end_handles, &[]);
        let mut contacts = Vec::new();
        let _ = construction::take_partition_projection_effect();
        let has_overlap = construction::classify_pair_into(
            &unique,
            0,
            1,
            &handles,
            &mut contacts,
            &mut forest,
            None,
        )
        .expect("small finite integer pairs stay representable");
        let effect = construction::take_partition_projection_effect()
            .expect("partition projection records its selected effect");
        let projected = match effect {
            construction::PartitionProjectionEffect::Disjoint => {
                saw_disjoint = true;
                assert!(
                    contacts.is_empty(),
                    "disjoint pair {pair_index} emitted a cut"
                );
                Contact::Disjoint
            },
            construction::PartitionProjectionEffect::Shared(endpoint) => {
                saw_shared = true;
                assert!(
                    contacts.is_empty(),
                    "shared endpoint pair {pair_index} emitted a cut"
                );
                Contact::Touch(TouchLocation::Shared(endpoint))
            },
            construction::PartitionProjectionEffect::LeftInterior(endpoint) => {
                saw_left_interior = true;
                assert_eq!(contacts.len(), 1, "left-interior pair {pair_index}");
                let Some(&(source, handle)) = contacts.first() else {
                    unreachable!("the asserted single contact is present")
                };
                assert_eq!(source, 0, "left-interior cut source for pair {pair_index}");
                let expected_handle = match endpoint {
                    SegmentEndpoint::Start => *start_handles.get(1).expect("right start handle"),
                    SegmentEndpoint::End => *end_handles.get(1).expect("right end handle"),
                };
                assert_eq!(
                    handle, expected_handle,
                    "left-interior cut for pair {pair_index}"
                );
                Contact::Touch(TouchLocation::LeftInterior(endpoint))
            },
            construction::PartitionProjectionEffect::RightInterior(endpoint) => {
                saw_right_interior = true;
                assert_eq!(contacts.len(), 1, "right-interior pair {pair_index}");
                let Some(&(source, handle)) = contacts.first() else {
                    unreachable!("the asserted single contact is present")
                };
                assert_eq!(source, 1, "right-interior cut source for pair {pair_index}");
                let expected_handle = match endpoint {
                    SegmentEndpoint::Start => *start_handles.first().expect("left start handle"),
                    SegmentEndpoint::End => *end_handles.first().expect("left end handle"),
                };
                assert_eq!(
                    handle, expected_handle,
                    "right-interior cut for pair {pair_index}"
                );
                Contact::Touch(TouchLocation::RightInterior(endpoint))
            },
            construction::PartitionProjectionEffect::Collinear => {
                saw_collinear = true;
                let expected_cuts = [
                    (
                        left,
                        right.start,
                        0,
                        *start_handles.get(1).expect("right start handle"),
                    ),
                    (
                        left,
                        right.end,
                        0,
                        *end_handles.get(1).expect("right end handle"),
                    ),
                    (
                        right,
                        left.start,
                        1,
                        *start_handles.first().expect("left start handle"),
                    ),
                    (
                        right,
                        left.end,
                        1,
                        *end_handles.first().expect("left end handle"),
                    ),
                ]
                .into_iter()
                .filter(|(segment, point, ..)| {
                    *point != segment.start
                        && *point != segment.end
                        && point.x >= segment.start.x.min(segment.end.x)
                        && point.x <= segment.start.x.max(segment.end.x)
                        && point.y >= segment.start.y.min(segment.end.y)
                        && point.y <= segment.start.y.max(segment.end.y)
                })
                .map(|(_, _, source, handle)| (source, handle))
                .collect::<Vec<_>>();
                assert_eq!(
                    contacts, expected_cuts,
                    "collinear interior cuts for pair {pair_index}"
                );
                Contact::Collinear
            },
            construction::PartitionProjectionEffect::Cross => {
                saw_cross = true;
                assert_eq!(contacts.len(), 2, "crossing pair {pair_index}");
                let [(left_source, left_handle), (right_source, right_handle)] =
                    contacts.as_slice()
                else {
                    unreachable!("the asserted two crossing contacts are present")
                };
                assert_eq!((*left_source, *right_source), (0, 1));
                assert_eq!(left_handle, right_handle);
                Contact::Cross
            },
        };
        assert_eq!(
            projected, expected,
            "projection mismatch for pair {pair_index}"
        );
        assert_eq!(
            has_overlap,
            matches!(expected, Contact::Collinear),
            "overlap result for pair {pair_index}"
        );
    }

    assert!(
        saw_disjoint
            && saw_shared
            && saw_left_interior
            && saw_right_interior
            && saw_collinear
            && saw_cross,
        "the corpus must exercise every classifier projection arm"
    );
}

fn crossing_node(partition: &ExactPartition<'_>) -> NodeId {
    let mut crossings =
        (0..partition.node_count() as u32).filter(|&node| !partition.is_stored(NodeId(node)));
    let node = crossings.next().expect("expected a crossing node");
    assert_eq!(crossings.next(), None, "expected one crossing node");
    NodeId(node)
}

/// The node's exact point is `(x[0] / x[1], y[0] / y[1])`.
fn assert_exact_at(
    partition: &ExactPartition<'_>,
    node: NodeId,
    x: [&ExactDyadic; 2],
    y: [&ExactDyadic; 2],
) {
    let point = partition.exact(node).unwrap();
    let [px, py, pw] = &*point;
    assert_eq!(compare_ratios(px, pw, x[0], x[1]).unwrap(), Ordering::Equal);
    assert_eq!(compare_ratios(py, pw, y[0], y[1]).unwrap(), Ordering::Equal);
}

fn constructed_count(partition: &ExactPartition) -> usize {
    partition.crossings().len()
}

fn component_count(partition: &ExactPartition) -> usize {
    let n = partition.node_count();
    if n == 0 {
        return 0;
    }
    let mut parent: Vec<u32> = (0..n as u32).collect();
    let find = |parent: &mut [u32], mut node: u32| -> u32 {
        while parent[node as usize] != node {
            parent[node as usize] = parent[parent[node as usize] as usize];
            node = parent[node as usize];
        }
        node
    };
    for span in &partition.spans {
        let a = find(&mut parent, span.start.0);
        let b = find(&mut parent, span.end.0);
        if a != b {
            parent[b as usize] = a;
        }
    }
    let mut roots = Vec::new();
    for node in 0..n as u32 {
        let root = find(&mut parent, node);
        if !roots.contains(&root) {
            roots.push(root);
        }
    }
    roots.len()
}

#[test]
fn public_entry_matches_inner_on_empty() {
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let (unique, provenance) = dedup_undirected_segments_sourced(Vec::<Segment>::new()).unwrap();
    let partition =
        partition_unique_segments::<true>(&unique, &provenance, &[], &mut budget).unwrap();
    assert_eq!(partition.node_count(), 0);
}

#[test]
fn generated_t_has_seven_nodes_five_spans_two_components() {
    let rn_sixth = 1.0_f64 / 6.0;
    let case = run(&[
        seg(0.0, 0.0, 3.0, 1.0),
        seg(1.0, -1.0, 1.0, 1.0),
        seg(0.5, 0.0, 0.5, rn_sixth),
    ]);
    let partition = case.partition();
    assert_eq!(partition.unique.len(), 3);
    assert_eq!(partition.node_count(), 7);
    assert_eq!(partition.spans.len(), 5);
    assert_eq!(constructed_count(&partition), 1);
    assert_eq!(component_count(&partition), 2);
    let c_end = partition.stored_node(XY::new(0.5, rn_sixth)).unwrap();
    let on_a = partition
        .spans
        .iter()
        .any(|span| span.unique == 0 && (span.start == c_end || span.end == c_end));
    assert!(!on_a, "C's endpoint must not node onto A");
    let (one, three) = (dyadic(1.0), dyadic(3.0));
    assert_exact_at(&partition, crossing_node(&partition), [&one, &one], [
        &one, &three,
    ]);
}

#[test]
fn endpoint_near_miss_five_nodes_four_spans() {
    let nd = 1.0_f64.next_down();
    let case = run(&[seg(0.0, 0.0, 1.0, 1.0), seg(1.0, nd, 0.0, 2.0)]);
    let partition = case.partition();
    assert_eq!(partition.node_count(), 5);
    assert_eq!(partition.spans.len(), 4);
    assert_eq!(constructed_count(&partition), 1);
    let q_num = ExactDyadic::from_f64(1.0);
    let two = ExactDyadic::from_f64(2.0);
    let p54 = {
        let mut p = ExactDyadic::from_f64(1.0);
        for _ in 0..54 {
            p = p.try_product(&two).unwrap();
        }
        p
    };
    let den = p54.clone().try_add(q_num).unwrap();
    // The crossing lies on y = x at 2^54 / (2^54 + 1).
    assert_exact_at(&partition, crossing_node(&partition), [&p54, &den], [
        &p54, &den,
    ]);
}

#[test]
fn concurrent_three_sources_one_constructed_node() {
    let case = run(&[
        seg(0.0, 0.0, 3.0, 1.0),
        seg(1.0, -1.0, 1.0, 1.0),
        seg(0.0, 1.0, 3.0, -1.0),
    ]);
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 1);
    let (one, three) = (dyadic(1.0), dyadic(3.0));
    assert_exact_at(&partition, crossing_node(&partition), [&one, &one], [
        &one, &three,
    ]);
}

#[test]
fn overlap_reversed_duplicate_keeps_reverse_owner() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 0.0),
        seg(1.0, 0.0, 3.0, 0.0),
        seg(2.0, -1.0, 2.0, 1.0),
        seg(4.0, 0.0, 0.0, 0.0),
    ]);
    let partition = case.partition();
    let reversed = partition.spans.iter().enumerate().any(|(index, _)| {
        partition
            .owners(index)
            .iter()
            .any(|&(ordinal, reversed)| ordinal == 3 && reversed)
    });
    assert!(reversed, "reversed duplicate must keep the reverse flag");
    let overlap = partition.spans.iter().enumerate().filter(|(index, _)| {
        let owners = partition.owners(*index);
        owners.iter().any(|(ordinal, _)| *ordinal == 0)
            && owners.iter().any(|(ordinal, _)| *ordinal == 1)
    });
    assert!(
        overlap.count() > 0,
        "overlap of A and B must be one aggregated atom"
    );
}

#[test]
fn exact_partition_orders_extreme_horizontal_cuts_and_keeps_owner() {
    for scale in [2.0_f64.powi(-600), 2.0_f64.powi(600)] {
        let segments = std::iter::once(seg(0.0, 0.0, 4.0 * scale, 0.0))
            .chain(
                [3.0, 1.0, 2.0]
                    .map(|multiple| seg(multiple * scale, -scale, multiple * scale, scale)),
            )
            .collect::<Vec<_>>();
        let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
        let mut budget = ExpansionBudget::new("exact_partition", "extreme horizontal cuts");
        let partition = inventory.partition(&mut budget).unwrap();
        let mut source_atoms = Vec::new();
        for (index, span) in partition.spans.iter().enumerate() {
            let owners = partition.owners(index);
            if owners.iter().any(|&(source, _)| source == 0) {
                assert_eq!(owners, &[(0, false)]);
                let start = partition.nearest_xy(span.start);
                let end = partition.nearest_xy(span.end);
                assert_eq!((start.y, end.y), (0.0, 0.0));
                source_atoms.push((start.x.min(end.x), start.x.max(end.x)));
            }
        }
        source_atoms.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
        assert_eq!(source_atoms, vec![
            (0.0, scale),
            (scale, 2.0 * scale),
            (2.0 * scale, 3.0 * scale),
            (3.0 * scale, 4.0 * scale),
        ]);
    }
}

#[test]
fn exact_partition_preserves_reversed_partial_overlap_atom_owners() {
    let segments = [
        seg(0.0, 0.0, 4.0, 0.0),
        seg(3.0, 0.0, 1.0, 0.0),
        seg(1.0, 0.0, 3.0, 0.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("exact_partition", "reversed partial overlap");
    let partition = inventory.partition(&mut budget).unwrap();
    let mut horizontal = Vec::new();
    for (index, span) in partition.spans.iter().enumerate() {
        let start = partition.nearest_xy(span.start);
        let end = partition.nearest_xy(span.end);
        if start.y == 0.0 && end.y == 0.0 {
            horizontal.push((
                start.x.min(end.x),
                start.x.max(end.x),
                partition.owners(index).to_vec(),
            ));
        }
    }
    horizontal.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
    assert_eq!(horizontal, vec![
        (0.0, 1.0, vec![(0, false)]),
        (1.0, 3.0, vec![(0, false), (1, true), (2, false)]),
        (3.0, 4.0, vec![(0, false)]),
    ]);
}

#[test]
fn overlap_aggregate_owners_are_one_atom() {
    let case = run(&[seg(0.0, 0.0, 4.0, 0.0), seg(1.0, 0.0, 3.0, 0.0)]);
    let partition = case.partition();
    let shared: Vec<_> = partition
        .spans
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            let owners = partition.owners(*index);
            owners.iter().any(|(ordinal, _)| *ordinal == 0)
                && owners.iter().any(|(ordinal, _)| *ordinal == 1)
        })
        .collect();
    assert_eq!(shared.len(), 1);
    assert_eq!(partition.owners(shared[0].0).len(), 2);
}

#[test]
fn sourced_dedup_unique_segments_keep_identity_owner_rows() {
    let (unique, provenance) = dedup_undirected_segments_sourced([
        seg(0.0, 0.0, 1.0, 0.0),
        seg(0.0, 1.0, 1.0, 1.0),
        seg(0.0, 2.0, 1.0, 2.0),
    ])
    .unwrap();

    assert_eq!(
        unique
            .iter()
            .map(|segment| (segment.start, segment.end))
            .collect::<Vec<_>>(),
        vec![
            (XY::new(0.0, 0.0), XY::new(1.0, 0.0)),
            (XY::new(0.0, 1.0), XY::new(1.0, 1.0)),
            (XY::new(0.0, 2.0), XY::new(1.0, 2.0)),
        ]
    );
    assert_eq!(provenance.owners(0), &[(0, false)]);
    assert_eq!(provenance.owners(1), &[(1, false)]);
    assert_eq!(provenance.owners(2), &[(2, false)]);
}

#[test]
fn sourced_dedup_first_late_duplicate_keeps_every_ordered_owner() {
    let (unique, provenance) = dedup_undirected_segments_sourced([
        seg(0.0, 0.0, 4.0, 0.0),
        seg(0.0, 2.0, 4.0, 2.0),
        seg(0.0, 4.0, 4.0, 4.0),
        seg(4.0, 2.0, 0.0, 2.0),
        seg(0.0, 0.0, 4.0, 0.0),
        seg(0.0, 6.0, 4.0, 6.0),
        seg(0.0, 2.0, 4.0, 2.0),
        seg(4.0, 0.0, 0.0, 0.0),
    ])
    .unwrap();

    assert_eq!(
        unique
            .iter()
            .map(|segment| (segment.start, segment.end))
            .collect::<Vec<_>>(),
        vec![
            (XY::new(0.0, 0.0), XY::new(4.0, 0.0)),
            (XY::new(0.0, 2.0), XY::new(4.0, 2.0)),
            (XY::new(0.0, 4.0), XY::new(4.0, 4.0)),
            (XY::new(0.0, 6.0), XY::new(4.0, 6.0)),
        ]
    );
    assert_eq!(provenance.owners(0), &[(0, false), (4, false), (7, true)]);
    assert_eq!(provenance.owners(1), &[(1, false), (3, true), (6, false)]);
    assert_eq!(provenance.owners(2), &[(2, false)]);
    assert_eq!(provenance.owners(3), &[(5, false)]);
}

#[test]
fn sourced_dedup_preserves_signed_zero_and_degenerate_owner_direction() {
    let (unique, provenance) = dedup_undirected_segments_sourced([
        seg(-0.0, 0.0, 2.0, 0.0),
        seg(2.0, 0.0, 0.0, 0.0),
        seg(0.0, 1.0, -0.0, 1.0),
        seg(-0.0, 1.0, 0.0, 1.0),
    ])
    .unwrap();

    assert_eq!(unique.len(), 2);
    assert_eq!(unique[0].start.x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(unique[0].end.x.to_bits(), 2.0_f64.to_bits());
    assert_eq!(unique[1].start.x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(unique[1].end.x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(provenance.owners(0), &[(0, false), (1, true)]);
    assert_eq!(provenance.owners(1), &[(2, false), (3, false)]);
}

#[test]
fn segment_provenance_preserves_empty_direct_csr_rows_and_iteration() {
    let default = SegmentProvenance::default();
    let (empty_unique, empty) = dedup_undirected_segments_sourced(Vec::<Segment>::new()).unwrap();
    assert!(empty_unique.is_empty());
    for provenance in [&default, &empty] {
        assert_eq!(provenance.len(), 0);
        assert_eq!(provenance.iter().count(), 0);
    }

    let provenance =
        SegmentProvenance::from_csr(vec![0, 2, 2, 3], vec![(7, false), (2, true), (9, false)]);
    assert_eq!(provenance.len(), 3);
    assert_eq!(provenance.owners(0), &[(7, false), (2, true)]);
    assert_eq!(&provenance[0], &[(7, false), (2, true)]);
    assert!(provenance[1].is_empty());
    assert_eq!(provenance.owners(2), &[(9, false)]);

    let rows = provenance.iter().collect::<Vec<_>>();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], &[(7, false), (2, true)]);
    assert!(rows[1].is_empty());
    assert_eq!(rows[2], &[(9, false)]);
}

#[test]
#[should_panic(expected = "index out of bounds")]
fn segment_provenance_index_out_of_range_keeps_slice_panic() {
    let provenance = SegmentProvenance::from_csr(vec![0, 0], Vec::new());
    let _ = provenance[1];
}

#[test]
fn degenerate_point_is_isolated_node() {
    let case = run(&[seg(0.0, 0.0, 2.0, 0.0), seg(1.0, 1.0, 1.0, 1.0)]);
    let partition = case.partition();
    assert_eq!(partition.isolated.len(), 1);
    assert_eq!(
        partition.stored_node(XY::new(1.0, 1.0)),
        Some(partition.isolated[0])
    );
}

#[test]
fn explicit_point_occurrences_retain_incident_and_isolated_ownership() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 0.0),
        seg(2.0, 0.0, 2.0, 0.0),
        seg(0.0, 0.0, 0.0, 0.0),
        seg(2.0, 0.0, 2.0, 0.0),
        seg(7.0, 3.0, 7.0, 3.0),
    ]);
    let partition = case.partition();
    let middle = partition.stored_node(XY::new(2.0, 0.0)).unwrap();
    let endpoint = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let isolated = partition.stored_node(XY::new(7.0, 3.0)).unwrap();
    assert_eq!(partition.point_nodes, [middle, endpoint, middle, isolated]);
    assert_eq!(partition.isolated, [isolated]);
    assert_eq!(partition.spans.len(), 2);
}

#[test]
fn isolated_nodes_preserve_point_sources_and_occurrence_order_in_both_lanes() {
    let (unique, provenance) = dedup_undirected_segments_sourced([
        seg(0.0, 0.0, 4.0, 0.0),
        seg(8.0, 1.0, 10.0, 1.0),
        seg(20.0, 2.0, 20.0, 2.0),
    ])
    .unwrap();
    let isolated = [
        XY::new(4.0, 0.0),
        XY::new(30.0, 3.0),
        XY::new(40.0, 4.0),
        XY::new(30.0, 3.0),
    ];
    for ordinates in [false, true] {
        for budgeted in [false, true] {
            let mut budget = ExpansionBudget::new("exact_partition", "items");
            let partition = match (ordinates, budgeted) {
                (false, false) => {
                    partition_unique_segments_unbudgeted::<false>(&unique, &provenance, &isolated)
                },
                (true, false) => {
                    partition_unique_segments_unbudgeted::<true>(&unique, &provenance, &isolated)
                },
                (false, true) => {
                    partition_unique_segments::<false>(&unique, &provenance, &isolated, &mut budget)
                },
                (true, true) => {
                    partition_unique_segments::<true>(&unique, &provenance, &isolated, &mut budget)
                },
            }
            .unwrap();
            assert_eq!(partition.node_count(), 7);
            assert_eq!(partition.spans.len(), 2);
            assert_eq!(partition.isolated, [NodeId(4), NodeId(5), NodeId(6)]);
            assert_eq!(partition.point_nodes, [
                NodeId(1),
                NodeId(5),
                NodeId(6),
                NodeId(5)
            ]);
            assert_eq!(partition.point_sources, [(NodeId(4), 2)]);
            assert_eq!(budget.used(), 0);
        }
    }
}

#[test]
fn constructed_crossing_splits_every_overlapping_source_in_both_orders() {
    let mut sources = [
        seg(0.0, 0.0, 4.0, 0.0),
        seg(1.0, 0.0, 3.0, 0.0),
        seg(0.0, -1.0, 4.0, 2.0),
        seg(4.0, 0.0, 0.0, 0.0),
        seg(10.0, 10.0, 11.0, 11.0),
        seg(11.0, 11.0, 12.0, 12.0),
        seg(20.0, 10.0, 21.0, 11.0),
    ];
    for reverse in [false, true] {
        if reverse {
            sources.reverse();
        }
        let case = run(&sources);
        let partition = case.partition();
        let constructed: Vec<_> = partition.crossings().into_iter().collect();
        assert_eq!(constructed.len(), 1);
        let crossing = constructed[0];
        let departures: Vec<_> = [
            XY::new(3.0, 0.0),
            XY::new(4.0, 2.0),
            XY::new(1.0, 0.0),
            XY::new(0.0, -1.0),
        ]
        .into_iter()
        .map(|point| {
            let target = partition.stored_node(point).unwrap();
            partition
                .spans
                .iter()
                .position(|span| {
                    (span.start == crossing && span.end == target)
                        || (span.end == crossing && span.start == target)
                })
                .unwrap()
        })
        .collect();
        for (position, &left) in departures.iter().enumerate() {
            for &right in &departures[position + 1..] {
                assert_eq!(
                    partition.compare_departures(crossing, left, right),
                    Ordering::Less
                );
                assert_eq!(
                    partition.compare_departures(crossing, right, left),
                    Ordering::Greater
                );
            }
        }
        let exact = partition.exact(crossing).unwrap();
        let [x, y, w] = &*exact;
        let (topology, edges) = partition.local_topology().unwrap();
        let row = topology.starts[crossing.0 as usize] as usize
            ..topology.starts[crossing.0 as usize + 1] as usize;
        assert_eq!(row.len(), 4);
        for (&edge, &expected) in edges[row].iter().zip(&departures) {
            assert_eq!(edge as usize, expected);
        }
        let four = ExactDyadic::from_f64(4.0);
        let three = ExactDyadic::from_f64(3.0);
        let zero = ExactDyadic::from_f64(0.0);
        let one = ExactDyadic::from_f64(1.0);
        assert_eq!(
            compare_ratios(x, w, &four, &three).unwrap(),
            Ordering::Equal
        );
        assert_eq!(compare_ratios(y, w, &zero, &one).unwrap(), Ordering::Equal);
        let left = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
        let right = partition.stored_node(XY::new(3.0, 0.0)).unwrap();
        assert_eq!(
            partition.compare_nodes(left, crossing).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            partition.compare_nodes(right, crossing).unwrap(),
            Ordering::Greater
        );
        let mut owner_counts: Vec<_> = partition
            .spans
            .iter()
            .enumerate()
            .filter(|(_, span)| span.start == crossing || span.end == crossing)
            .map(|(index, _)| partition.owners(index).len())
            .collect();
        owner_counts.sort_unstable();
        assert_eq!(owner_counts, [1, 1, 3, 3]);
        assert_eq!(partition.spans.len(), 9);
    }
}

#[test]
fn signed_zero_endpoints_share_a_stored_node() {
    let case = run(&[seg(-0.0, 0.0, 1.0, 0.0), seg(0.0, 0.0, 0.0, 1.0)]);
    let partition = case.partition();
    let a = partition.stored_node(XY::new(-0.0, 0.0)).unwrap();
    let b = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    assert_eq!(a, b);
}

#[test]
fn full_finite_exponent_pair_is_five_nodes_four_spans() {
    let min_sub = f64::from_bits(1);
    let a = seg(0.0, 0.0, f64::MAX, min_sub);
    let b = seg(min_sub, -f64::MAX, f64::MAX, f64::MAX);
    let case = run(&[a, b]);
    let partition = case.partition();
    assert_eq!(partition.node_count(), 5);
    assert_eq!(partition.spans.len(), 4);
    assert_eq!(constructed_count(&partition), 1);
    for point in [a.start, a.end, b.start, b.end] {
        assert!(partition.stored_node(point).is_some());
    }
    let crossing = crossing_node(&partition);
    for endpoint in [a.start, a.end, b.start, b.end] {
        let endpoint = partition.stored_node(endpoint).unwrap();
        assert_ne!(
            partition.compare_nodes(crossing, endpoint).unwrap(),
            Ordering::Equal
        );
    }
}

#[test]
fn isolated_on_line_cuts_the_edge() {
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let (unique, provenance) =
        dedup_undirected_segments_sourced(vec![seg(0.0, 0.0, 2.0, 0.0)]).unwrap();
    let partition =
        partition_unique_segments::<true>(&unique, &provenance, &[XY::new(1.0, 0.0)], &mut budget)
            .unwrap();
    assert_eq!(partition.node_count(), 3);
    assert_eq!(partition.spans.len(), 2);
    assert!(partition.isolated.is_empty());
    let mid = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
    assert!(
        partition
            .spans
            .iter()
            .any(|span| span.start == mid || span.end == mid)
    );
}

#[test]
fn isolated_at_crossing_is_stored_not_constructed() {
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let (unique, provenance) =
        dedup_undirected_segments_sourced(vec![seg(0.0, 0.0, 2.0, 2.0), seg(0.0, 2.0, 2.0, 0.0)])
            .unwrap();
    let partition =
        partition_unique_segments::<true>(&unique, &provenance, &[XY::new(1.0, 1.0)], &mut budget)
            .unwrap();
    assert_eq!(partition.node_count(), 5);
    assert_eq!(partition.spans.len(), 4);
    assert_eq!(constructed_count(&partition), 0);
    assert!(partition.isolated.is_empty());
    assert!(partition.is_stored(partition.stored_node(XY::new(1.0, 1.0)).unwrap()));
}

#[test]
fn stored_endpoint_concurrency_is_six_nodes_five_spans() {
    let case = run(&[
        seg(0.0, 0.0, 2.0, 0.0),
        seg(1.0, 0.0, 1.0, 1.0),
        seg(0.0, 1.0, 2.0, -1.0),
    ]);
    let partition = case.partition();
    assert_eq!(partition.node_count(), 6);
    assert_eq!(partition.spans.len(), 5);
    assert_eq!(constructed_count(&partition), 0);
    let junction = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
    let incident = partition
        .spans
        .iter()
        .filter(|span| span.start == junction || span.end == junction)
        .count();
    assert_eq!(incident, 5);
}

#[test]
fn concurrent_three_sources_recipe_when_vertical_is_first() {
    let case = run(&[
        seg(1.0, -1.0, 1.0, 1.0),
        seg(0.0, 0.0, 3.0, 1.0),
        seg(0.0, 1.0, 3.0, -1.0),
    ]);
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 1);
    let (one, three) = (dyadic(1.0), dyadic(3.0));
    assert_exact_at(&partition, crossing_node(&partition), [&one, &one], [
        &one, &three,
    ]);
}

/// Ordinary crossings are certified in double-double and never build an
/// exact point: the lane is the one that actually runs.
#[test]
fn generic_crossings_round_without_exact_points() {
    let ring = |cx: f64, cy: f64, phase: f64| -> Vec<Segment> {
        let corners: Vec<XY> = (0..7)
            .map(|index| {
                let angle = phase + std::f64::consts::TAU * f64::from(index) / 7.0;
                XY::new(cx + 25.0 * angle.cos(), cy + 25.0 * angle.sin())
            })
            .collect();
        (0..7)
            .map(|index| Segment {
                start: corners[index],
                end: corners[(index + 1) % 7],
            })
            .collect()
    };
    let mut segments = ring(500_000.0, 5_200_000.0, 0.1);
    segments.extend(ring(500_020.0, 5_200_015.0, 0.2));
    reset_partition_builds();
    let case = run(&segments);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    partition.global_faces(&topology, &edges).unwrap();
    let crossings = (0..partition.node_count() as u32)
        .filter(|&node| !partition.is_stored(NodeId(node)))
        .count();
    assert!(crossings >= 2);
    let metrics = partition_metrics();
    assert_eq!(metrics.declined_crossings, 0);
    assert_eq!(metrics.exact_crossings, 0);
}

/// The crossing's exact y is 1.5 + 2^-52 + 2^-53, the midpoint between two
/// doubles: the certificate declines and the exact rounding goes to even.
#[test]
fn rounding_midpoint_crossing_declines_and_rounds_to_even() {
    let x = 1.0 + f64::EPSILON;
    reset_partition_builds();
    let case = run(&[seg(0.0, 0.0, 2.0, 3.0), seg(x, 0.0, x, 3.0)]);
    let partition = case.partition();
    assert_eq!(partition_metrics().declined_crossings, 1);
    assert_eq!(
        partition.nearest_xy(crossing_node(&partition)),
        XY::new(x, 1.5 + 2.0 * f64::EPSILON)
    );
}

/// Three nearly concurrent segments cross at three points that differ
/// below the last bit, two of which round to one double; they stay three
/// nodes, ordered exactly.
#[test]
fn crossings_rounding_to_one_double_stay_distinct() {
    let case = run(&[
        seg(0.0, 0.0, 3.0, 1.0),
        seg(1.0, -1.0, 1.0, 1.0),
        seg(0.0, 1.0, 2.0, -1.0 / 3.0),
    ]);
    let partition = case.partition();
    let crossings: Vec<NodeId> = (0..partition.node_count() as u32)
        .map(NodeId)
        .filter(|&node| !partition.is_stored(node))
        .collect();
    assert_eq!(crossings.len(), 3);
    let mut shared_roundings = 0;
    for (index, &first) in crossings.iter().enumerate() {
        for &second in &crossings[index + 1..] {
            shared_roundings +=
                usize::from(partition.nearest_xy(first) == partition.nearest_xy(second));
            assert_ne!(
                partition.compare_nodes(first, second).unwrap(),
                Ordering::Equal
            );
        }
    }
    assert_eq!(shared_roundings, 1);
}

/// Coordinates outside the certificate window and nearly parallel carriers
/// decline to the exact construction, which still rounds nearest-even.
#[test]
fn out_of_window_and_near_parallel_crossings_decline() {
    for segments in [
        [
            seg(0.0, 0.0, 3e300, 1e300),
            seg(1e300, -1e300, 1e300, 1e300),
        ],
        [seg(0.0, 0.0, 1.0, 1.0), seg(0.0, 1e-17, 1.0, 1.0 - 1e-16)],
    ] {
        reset_partition_builds();
        let case = run(&segments);
        let partition = case.partition();
        assert_eq!(partition_metrics().declined_crossings, 1);
        let crossing = crossing_node(&partition);
        let [first_unique, second_unique] = partition.unique.first_chunk::<2>().unwrap();
        let [x, y, w] = crossing_homogeneous(*first_unique, *second_unique).unwrap();
        assert_eq!(
            partition.nearest_xy(crossing),
            XY::new(round_ratio(&x, &w).unwrap(), round_ratio(&y, &w).unwrap())
        );
    }
}

#[test]
fn star_crossings_keep_one_recipe_per_constructed_node() {
    let mut segments = vec![seg(0.0, 0.0, 5.0, 0.0)];
    for x in 1..=4 {
        segments.push(seg(f64::from(x), -1.0, f64::from(x), 1.0));
    }
    let case = run(&segments);
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 4);
    let (zero, one) = (dyadic(0.0), dyadic(1.0));
    let mut found = Vec::new();
    for node in 0..partition.node_count() as u32 {
        let node = NodeId(node);
        if partition.is_stored(node) {
            continue;
        }
        let point = partition.nearest_xy(node);
        assert_eq!(point.y, 0.0);
        assert_exact_at(&partition, node, [&dyadic(point.x), &one], [&zero, &one]);
        found.push(point.x);
    }
    found.sort_unstable_by(f64::total_cmp);
    assert_eq!(found, [1.0, 2.0, 3.0, 4.0]);
}

#[test]
fn disjoint_thirty_two_thousand_segments_are_not_capped_as_pairs() {
    let mut segments = Vec::with_capacity(32_000);
    for index in 0..32_000 {
        let x = f64::from(index);
        segments.push(seg(x, 0.0, x, 1.0));
    }
    let case = run(&segments);
    let partition = case.partition();
    assert_eq!(partition.node_count(), 64_000);
    assert_eq!(partition.spans.len(), 32_000);
    assert_eq!(constructed_count(&partition), 0);
}

#[test]
fn pairwise_limit_is_not_an_allocation_error() {
    let err =
        ExpansionBudget::pairwise("exact_partition", "pairs", PAIRWISE_WORK_LIMIT, 2).unwrap_err();
    assert!(matches!(
        err.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge { .. })
    ));
}
