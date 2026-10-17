#[test]
fn rounded_chord_contact_with_an_isolated_node_is_repaired() {
    let point = XY::new(0.5, 1.0 / 6.0);
    let case = run(&[
        seg(0.0, 0.0, 3.0, 1.0),
        seg(1.0, -1.0, 1.0, 1.0),
        seg(point.x, point.y, point.x, point.y),
    ]);
    let partition = case.partition();
    let start = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let crossing = partition.crossings()[0];
    let isolated = partition.point_nodes[0];
    assert!(partition.isolated.contains(&isolated));
    assert!(partition.spans.iter().any(|span| {
        (span.start == start && span.end == crossing)
            || (span.end == start && span.start == crossing)
    }));
    let nodes = [start, crossing, isolated];
    let coordinates = partition.round_nodes(&nodes);
    embedding::check_node_injectivity(&nodes, &coordinates).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let mut cells = ArealCells {
        faces: vec![false; faces.count as usize],
        spans: vec![false; partition.spans.len()],
        nodes: vec![false; partition.node_count()],
    };
    for (index, span) in partition.spans.iter().enumerate() {
        cells.spans[index] = (span.start == start && span.end == crossing)
            || (span.end == start && span.start == crossing);
    }
    cells.nodes[isolated.0 as usize] = true;
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &faces)
        .unwrap();
    let emission = plan.emission(&topology, &edges);
    assert_eq!(emission.items.len(), 2);
    let embedding = partition.embed(&[&emission], "test").unwrap();
    assert_eq!(embedding.xy(start), partition.nearest_xy(start));
    assert_eq!(embedding.xy(isolated), partition.nearest_xy(isolated));
    assert_ne!(embedding.xy(crossing), partition.nearest_xy(crossing));
    let segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
        .collect();
    let mut sink = embedding::FirstFailure::default();
    assert!(
        embedding::certify(
            &partition,
            &embedding,
            &[&emission],
            &[&segments],
            &mut sink,
        )
        .unwrap()
        .is_continue()
    );
    let (points, lines, polygons) = plan
        .emit_xy_parts(&partition, &topology, &edges, "test")
        .unwrap();
    assert_eq!(points.len(), 1);
    assert_eq!(lines.len(), 1);
    assert!(polygons.is_empty());
    assert!(!emitted_pair_preserves_incidence(
        [(start, coordinates[0]), (crossing, coordinates[1])],
        [(isolated, coordinates[2]); 2],
    ));
}

#[test]
fn certificate_ignores_unemitted_segment_suffix() {
    let case = run(&[seg(0.0, 0.0, 1.0, 0.0), seg(10.0, 0.0, 11.0, 0.0)]);
    let partition = case.partition();
    let emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    assert_eq!(emission.items.len(), 2);

    let embedding = embedding::Embedding::nearest(&partition);
    let mut segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
        .collect();
    segments.push(*segments.first().expect("emission has a segment"));

    let mut witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &embedding,
            &[&emission],
            &[&segments],
            &mut witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(witnesses.0.is_empty());
}

#[test]
fn emitted_chords_allow_only_prescribed_endpoint_contacts() {
    let a = (NodeId(0), XY::new(0.0, 0.0));
    let b = (NodeId(1), XY::new(2.0, 0.0));
    let c = (NodeId(2), XY::new(2.0, 2.0));
    let d = (NodeId(3), XY::new(4.0, 0.0));
    let middle = (NodeId(4), XY::new(1.0, 0.0));
    for right in [[b, c], [b, d], [b, b], [c, c]] {
        assert!(emitted_pair_preserves_incidence([a, b], right));
        assert!(emitted_pair_preserves_incidence(right, [a, b]));
    }
    for right in [[middle, c], [middle, d], [middle, middle], [b, a]] {
        assert!(!emitted_pair_preserves_incidence([a, b], right));
        assert!(!emitted_pair_preserves_incidence(right, [a, b]));
    }
    let crossing = [
        (NodeId(5), XY::new(1.0, -1.0)),
        (NodeId(6), XY::new(1.0, 1.0)),
    ];
    assert!(!emitted_pair_preserves_incidence([a, b], crossing));
    assert!(!emitted_pair_preserves_incidence(crossing, [a, b]));
}

#[test]
fn live_node_injectivity_distinguishes_repetition_from_rounding_collision() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let case = run(&[seg(0.0, 0.0, 2.0, 0.0)]);
    let mut partition = case.partition();
    // A node at exact (x, 0), as a crossing whose exact point is known.
    let mut push = |x: ExactDyadic| {
        let node = NodeId(partition.node_count() as u32);
        let rounded = round_ratio(&x, &dyadic(1.0)).unwrap();
        let exact = std::cell::OnceCell::new();
        exact.set(Box::new([x, dyadic(0.0), dyadic(1.0)])).unwrap();
        partition
            .points
            .push(
                NodeRecipe::Crossing {
                    segments: [0, 0],
                    exact,
                },
                XY::new(rounded, 0.0),
            )
            .unwrap();
        node
    };
    let first = push(ExactDyadic::from_f64(1.0));
    let second = push(
        ExactDyadic::from_f64(1.0)
            .try_add(ExactDyadic::from_f64(exact_power_of_two(-54)))
            .unwrap(),
    );
    assert_eq!(
        partition.compare_nodes(first, second).unwrap(),
        Ordering::Less
    );
    let nodes = [first, second];
    let coordinates = partition.round_nodes(&nodes);
    assert_eq!(coordinates, [XY::new(1.0, 0.0); 2]);
    assert!(embedding::check_node_injectivity(&nodes, &coordinates).is_err());
    embedding::check_node_injectivity(&[first, first], &coordinates).unwrap();
    let endpoint = partition.stored_node(XY::new(2.0, 0.0)).unwrap();
    let nodes = [first, endpoint];
    embedding::check_node_injectivity(&nodes, &partition.round_nodes(&nodes)).unwrap();
    embedding::check_node_injectivity(&[], &[]).unwrap();
}

#[test]
fn selected_nodes_round_in_requested_order() {
    let case = run(&[seg(0.0, 0.0, 3.0, 1.0), seg(1.0, -1.0, 1.0, 1.0)]);
    let partition = case.partition();
    let crossing = partition.crossings()[0];
    let endpoint = partition.stored_node(XY::new(3.0, 1.0)).unwrap();
    let rounded = partition.round_nodes(&[endpoint, crossing, endpoint]);
    assert_eq!(rounded[0], XY::new(3.0, 1.0));
    assert_eq!(rounded[1].x, 1.0);
    assert_eq!(rounded[1].y.to_bits(), 0x3FD5_5555_5555_5555);
    assert_eq!(rounded[2], XY::new(3.0, 1.0));
    assert!(partition.round_nodes(&[]).is_empty());
}

fn append_constructed_point(
    partition: &mut ExactPartition<'_>,
    x: ExactDyadic,
    y: ExactDyadic,
) -> NodeId {
    let one = dyadic(1.0);
    let rounded = XY::new(
        round_ratio(&x, &one).unwrap(),
        round_ratio(&y, &one).unwrap(),
    );
    let exact = std::cell::OnceCell::new();
    exact
        .set(Box::new([x, y, one]))
        .expect("fresh constructed point stores one exact value");
    let node = NodeId(u32::try_from(partition.node_count()).unwrap());
    partition
        .points
        .push(NodeRecipe::Constructed { exact }, rounded)
        .unwrap();
    node
}

fn point_emission(nodes: &[NodeId]) -> embedding::Emission {
    embedding::Emission {
        items: nodes.iter().map(|&node| [node, node]).collect(),
        chords: 0,
        areal_chords: 0,
        fan_spans: Vec::new(),
        faces: None,
    }
}

fn append_lattice_collision_points(
    partition: &mut ExactPartition<'_>,
    count: usize,
) -> Vec<NodeId> {
    let order: Vec<_> = (0..count).collect();
    append_lattice_collision_points_in_order(partition, count, &order)
}

fn append_lattice_collision_points_in_order(
    partition: &mut ExactPartition<'_>,
    count: usize,
    order: &[usize],
) -> Vec<NodeId> {
    use crate::geometry::numerics::product::exact_power_of_two;
    let offsets = [
        exact_power_of_two(-54),
        exact_power_of_two(-55),
        exact_power_of_two(-56),
        exact_power_of_two(-57),
        exact_power_of_two(-58),
        exact_power_of_two(-59),
        exact_power_of_two(-60),
        exact_power_of_two(-61),
        exact_power_of_two(-62),
        exact_power_of_two(-63),
    ];
    assert_eq!(order.len(), count);
    let one = dyadic(1.0);
    let mut nodes = vec![None; count];
    for &index in order {
        let x_offset = *offsets
            .get(index)
            .expect("fixture has one x offset per node");
        let y_index = count.wrapping_sub(index.wrapping_add(1));
        let y_offset = *offsets
            .get(y_index)
            .expect("fixture has one y offset per node");
        let node = append_constructed_point(
            partition,
            one.clone()
                .try_add(ExactDyadic::from_f64(x_offset))
                .unwrap(),
            one.clone()
                .try_add(ExactDyadic::from_f64(y_offset))
                .unwrap(),
        );
        *nodes
            .get_mut(index)
            .expect("input ordinal belongs to the lattice fixture") = Some(node);
    }
    nodes
        .into_iter()
        .map(|node| node.expect("every lattice point was appended once"))
        .collect()
}

fn lattice_crossing_group(origin: f64, ulp: f64) -> [Segment; 3] {
    [
        seg(origin - ulp, origin - ulp, origin + ulp, origin + ulp),
        seg(origin, origin + ulp, origin + ulp, origin - ulp),
        seg(origin, origin, origin + 2.0 * ulp, origin + ulp),
    ]
}

fn argentina_uruguay_minimal_segments() -> [Segment; 6] {
    [
        seg(
            -58.404_001_6,
            -33.178_818,
            -58.122_989_509_606_21,
            -33.022_490_639_567_15,
        ),
        seg(
            -58.122_989_509_606_21,
            -33.022_490_639_567_15,
            -58.127_583_9,
            -33.022_275,
        ),
        seg(
            -58.114_883_013_439_915,
            -33.017_980_986_099_005,
            -58.113_024_156_144_505,
            -33.016_946_901_572_59,
        ),
        seg(
            -58.113_024_156_144_505,
            -33.016_946_901_572_59,
            -58.100_100_6,
            -32.951_622_7,
        ),
        seg(
            -58.111_802_159_656_57,
            -33.011_057_831_530_63,
            -58.112_953_902_396_03,
            -33.016_907_819_328_67,
        ),
        seg(
            -58.112_953_902_396_03,
            -33.016_907_819_328_67,
            -58.404_001_6,
            -33.178_818,
        ),
    ]
}

fn lattice_group_emission(partition: &ExactPartition<'_>, origin: f64) -> embedding::Emission {
    let span_ids = partition
        .spans
        .iter()
        .enumerate()
        .filter_map(|(index, span)| {
            let start = partition.nearest_xy(span.start);
            let end = partition.nearest_xy(span.end);
            let nearby =
                |point: XY| (point.x - origin).abs() < 0.01 && (point.y - origin).abs() < 0.01;
            (nearby(start) && nearby(end)).then(|| u32::try_from(index).unwrap())
        });
    partition.span_emission(span_ids)
}

fn lattice_group_crossing(partition: &ExactPartition<'_>, origin: f64) -> NodeId {
    partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == XY::new(origin, origin))
        .expect("the lattice group has a constructed crossing in its center cell")
}

fn planar_candidate_values(
    exact_x: ExactDyadic,
    exact_y: ExactDyadic,
    domain: embedding::EmbeddingDomain,
) -> (XY, Vec<XY>, Vec<XY>) {
    let case = run(&[seg(0.0, 0.0, 2.0, 0.0)]);
    let mut partition = case.partition();
    partition.embedding_domain = domain;
    let node = append_constructed_point(&mut partition, exact_x, exact_y);
    let emission = point_emission(&[node]);
    let movable_cache = vec![None; partition.node_count()];
    let problem =
        embedding::PlanarProblem::new(&partition, &[&emission], &[node], movable_cache).unwrap();
    let nearest = partition.nearest_xy(node);
    let mut candidates = Vec::new();
    problem
        .fill_candidates(node, &mut candidates, &mut Unmetered)
        .unwrap();
    let mut ordered = candidates.clone();
    let mut work = Unmetered;
    try_sort_by_with_work(&mut ordered, &mut work, |left, right, work| {
        problem.compare_candidates(node, *left, *right, work)
    })
    .unwrap();
    (nearest, candidates, ordered)
}

fn assert_exact_axis_displacement_bound(
    exact: &ExactDyadic,
    weight: &ExactDyadic,
    emitted: f64,
    nearest: f64,
) {
    let scaled_emitted = dyadic(emitted).try_product(weight).unwrap();
    let delta = scaled_emitted.try_subtract(exact.clone()).unwrap().abs();
    let step = (nearest.next_up() - nearest).max(nearest - nearest.next_down());
    let bound = dyadic(1.5 * step)
        .try_product(&weight.clone().abs())
        .unwrap();
    assert_ne!(
        delta.cmp(&bound),
        Ordering::Greater,
        "nearest={nearest:?}, emitted={emitted:?}, exact={exact:?}, weight={weight:?}, bound={bound:?}"
    );
}

struct TestVerifier<'a, P: Problem<Output = ()>> {
    inner: &'a mut P,
    local_calls: &'a std::cell::Cell<usize>,
    global_calls: &'a std::cell::Cell<usize>,
    node_order: Option<&'a dyn Fn(P::Node, P::Node) -> Ordering>,
    candidate_filter: Option<&'a dyn Fn(P::Node, P::Candidate) -> bool>,
    bypass_local: bool,
    bypass_global: bool,
}

impl<P: Problem<Output = ()>> Problem for TestVerifier<'_, P> {
    type Node = P::Node;
    type Candidate = P::Candidate;
    type Key = P::Key;
    type ClassState = P::ClassState;
    type Output = ();

    fn classes(&self) -> &[Vec<Self::Node>] {
        self.inner.classes()
    }

    fn fixed_keys<W: Work>(
        &self,
        class: usize,
        variables: &[Self::Node],
        work: &mut W,
    ) -> Result<Vec<Self::Key>> {
        self.inner.fixed_keys(class, variables, work)
    }

    fn compare_nodes<W: Work>(
        &self,
        left: Self::Node,
        right: Self::Node,
        work: &mut W,
    ) -> Result<Ordering> {
        self.node_order.map_or_else(
            || self.inner.compare_nodes(left, right, work),
            |compare| Ok(compare(left, right)),
        )
    }

    fn fill_candidates<W: Work>(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
        work: &mut W,
    ) -> Result<()> {
        self.inner.fill_candidates(node, candidates, work)?;
        if let Some(allowed) = self.candidate_filter {
            work.charge_product_into_error(candidates.len(), 1)?;
            candidates.retain(|&candidate| allowed(node, candidate));
        }
        Ok(())
    }

    fn compare_candidates<W: Work>(
        &self,
        node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
        work: &mut W,
    ) -> Result<Ordering> {
        self.inner.compare_candidates(node, left, right, work)
    }

    fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
        self.inner.candidate_key(candidate)
    }

    fn prepare_class<A: SearchAccount>(
        &self,
        class: usize,
        variables: &[Self::Node],
        account: &mut A,
    ) -> Result<Self::ClassState> {
        self.inner.prepare_class(class, variables, account)
    }

    fn check_local<A: SearchAccount>(
        &self,
        class_state: &Self::ClassState,
        class: usize,
        variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        account: &mut A,
    ) -> Result<bool> {
        self.local_calls.set(self.local_calls.get().wrapping_add(1));
        if self.bypass_local {
            Ok(true)
        } else {
            self.inner
                .check_local(class_state, class, variables, rank, assigned, account)
        }
    }

    fn check_global<A: SearchAccount>(
        &mut self,
        assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
        account: &mut A,
    ) -> Result<GlobalCheck<Self::Output>> {
        self.global_calls
            .set(self.global_calls.get().wrapping_add(1));
        if self.bypass_global {
            Ok(GlobalCheck::Pass(()))
        } else {
            Problem::check_global(&mut *self.inner, assignments, account)
        }
    }

    fn initial_evaluations(&self) -> u64 {
        self.inner.initial_evaluations()
    }
}

struct FanBypassVerifier<'a, 'p, 's, 'e> {
    inner: &'a mut embedding::PlanarProblem<'p, 's, 'e>,
    global_calls: &'a std::cell::Cell<usize>,
}

impl Problem for FanBypassVerifier<'_, '_, '_, '_> {
    type Node = NodeId;
    type Candidate = XY;
    type Key = PointKey;
    type ClassState = embedding::PlanarClassState;
    type Output = ();

    fn classes(&self) -> &[Vec<Self::Node>] {
        Problem::classes(self.inner)
    }

    fn fixed_keys<W: Work>(
        &self,
        class: usize,
        variables: &[Self::Node],
        work: &mut W,
    ) -> Result<Vec<Self::Key>> {
        Problem::fixed_keys(self.inner, class, variables, work)
    }

    fn compare_nodes<W: Work>(
        &self,
        left: Self::Node,
        right: Self::Node,
        work: &mut W,
    ) -> Result<Ordering> {
        Problem::compare_nodes(self.inner, left, right, work)
    }

    fn fill_candidates<W: Work>(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
        work: &mut W,
    ) -> Result<()> {
        Problem::fill_candidates(self.inner, node, candidates, work)
    }

    fn compare_candidates<W: Work>(
        &self,
        node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
        work: &mut W,
    ) -> Result<Ordering> {
        Problem::compare_candidates(self.inner, node, left, right, work)
    }

    fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
        Problem::candidate_key(self.inner, candidate)
    }

    fn prepare_class<A: SearchAccount>(
        &self,
        class: usize,
        variables: &[Self::Node],
        account: &mut A,
    ) -> Result<Self::ClassState> {
        Problem::prepare_class(self.inner, class, variables, account)
    }

    fn check_local<A: SearchAccount>(
        &self,
        class_state: &Self::ClassState,
        class: usize,
        variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        account: &mut A,
    ) -> Result<bool> {
        self.inner
            .check_local_without_fans(class_state, class, variables, rank, assigned, account)
    }

    fn check_global<A: SearchAccount>(
        &mut self,
        assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
        account: &mut A,
    ) -> Result<GlobalCheck<Self::Output>> {
        self.global_calls
            .set(self.global_calls.get().wrapping_add(1));
        Problem::check_global(&mut *self.inner, assignments, account)
    }

    fn initial_evaluations(&self) -> u64 {
        Problem::initial_evaluations(self.inner)
    }
}

fn first_certified_assignment(
    partition: &ExactPartition<'_>,
    emission: &embedding::Emission,
    variables: &[NodeId],
    candidates: &[Vec<XY>],
) -> Option<Vec<XY>> {
    fn visit(
        partition: &ExactPartition<'_>,
        emission: &embedding::Emission,
        variables: &[NodeId],
        candidates: &[Vec<XY>],
        rank: usize,
        selected: &mut Vec<XY>,
        answer: &mut Option<Vec<XY>>,
    ) {
        if answer.is_some() {
            return;
        }
        if rank == variables.len() {
            let mut coordinates = embedding::Embedding::nearest(partition)
                .coordinates()
                .to_vec();
            for (&node, &candidate) in variables.iter().zip(selected.iter()) {
                *coordinates
                    .get_mut(node.0 as usize)
                    .expect("oracle variable belongs to the partition") = candidate;
            }
            let embedding = embedding::Embedding::from_owned(coordinates);
            let segments: Vec<_> = emission
                .items
                .iter()
                .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
                .collect();
            let mut sink = embedding::FirstFailure::default();
            if embedding::certify(
                partition,
                &embedding,
                &[emission],
                &[segments.as_slice()],
                &mut sink,
            )
            .expect("the exhaustive certificate runs on one fixture emission")
            .is_continue()
            {
                *answer = Some(selected.clone());
            }
            return;
        }
        for &candidate in candidates
            .get(rank)
            .expect("every class variable has one candidate row")
        {
            selected.push(candidate);
            visit(
                partition,
                emission,
                variables,
                candidates,
                rank.wrapping_add(1),
                selected,
                answer,
            );
            selected.pop();
            if answer.is_some() {
                return;
            }
        }
    }

    let mut answer = None;
    visit(
        partition,
        emission,
        variables,
        candidates,
        0,
        &mut Vec::with_capacity(variables.len()),
        &mut answer,
    );
    answer
}

fn assert_collision_class_matches_search(count: usize, compare_oracle: bool) {
    let case = run(&[seg(0.0, 0.0, 2.0, 0.0)]);
    let mut partition = case.partition();
    let nodes = append_lattice_collision_points(&mut partition, count);
    let emission = point_emission(&nodes);
    assert!(
        nodes
            .iter()
            .all(|&node| partition.nearest_xy(node) == XY::new(1.0, 1.0))
    );
    let seed = *nodes.first().expect("collision fixture has a seed");
    let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[seed], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    let mut variables = classes
        .first()
        .expect("fixture has one proximity class")
        .clone();
    assert_eq!(variables.len(), count);
    let mut work = Unmetered;
    try_sort_by_with_work(&mut variables, &mut work, |left, right, work| {
        problem.compare_nodes(*left, *right, work)
    })
    .unwrap();

    let oracle = compare_oracle.then(|| {
        let fixed = problem.fixed_keys(0, &variables, &mut Unmetered).unwrap();
        let mut candidates = Vec::with_capacity(count);
        for &node in &variables {
            let mut values = Vec::new();
            problem
                .fill_candidates(node, &mut values, &mut Unmetered)
                .unwrap();
            try_sort_by_with_work(&mut values, &mut work, |left, right, work| {
                problem.compare_candidates(node, *left, *right, work)
            })
            .unwrap();
            let mut row = Vec::new();
            for value in values {
                let key = problem.candidate_key(value);
                if !fixed.contains(&key)
                    && !row
                        .iter()
                        .any(|prior: &XY| problem.candidate_key(*prior) == key)
                {
                    row.push(value);
                }
            }
            candidates.push(row);
        }
        first_certified_assignment(&partition, &emission, &variables, &candidates)
            .expect("the exhaustive oracle finds a certified assignment")
    });

    let assignment = Search::new(problem, "embedding_test", count)
        .unwrap()
        .run()
        .unwrap();
    assert_eq!(assignment.classes.len(), 1);
    let solution = assignment
        .classes
        .first()
        .expect("the collision class has one solution");
    assert_eq!(solution.variables, variables);
    if let Some(oracle) = oracle {
        assert_eq!(solution.values, oracle);
    }
    for (index, value) in solution.values.iter().enumerate() {
        assert!(
            !solution
                .values
                .get(..index)
                .expect("prior assignments are in range")
                .contains(value),
            "class members share {value:?}"
        );
    }
}

#[test]
fn small_collision_classes_match_the_exhaustive_production_certificate() {
    for count in [3, 4] {
        assert_collision_class_matches_search(count, true);
    }
}

#[test]
fn five_node_collision_class_has_a_matching_and_distinct_assignment() {
    assert_collision_class_matches_search(5, false);
}

#[test]
fn ten_node_collision_class_is_proved_unrepresentable_by_matching() {
    let case = run(&[seg(0.0, 0.0, 2.0, 0.0)]);
    let mut partition = case.partition();
    let nodes = append_lattice_collision_points(&mut partition, 10);
    let emission = point_emission(&nodes);
    let seed = *nodes.first().expect("collision fixture has a seed");
    let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[seed], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    assert_eq!(classes.first().unwrap().len(), 10);
    let error = Search::new(problem, "embedding_test", 10)
        .unwrap()
        .run()
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "embedding_test",
            constructed: 10
        })
    ));
}

#[test]
fn lattice_crossings_rounding_into_one_stored_vertex_are_repaired() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let ulp = exact_power_of_two(-52);
    let case = run(&lattice_crossing_group(1.0, ulp));
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 2);
    let mut crossings = partition.crossings();
    try_sort_by(&mut crossings, |left, right| {
        partition.compare_nodes(*left, *right)
    })
    .unwrap();
    let third = dyadic(3.0);
    let five = dyadic(5.0);
    let lattice = dyadic(ulp);
    let x_third = third.clone().try_add(lattice.clone()).unwrap();
    let x_fifths = five
        .clone()
        .try_add(lattice.try_product(&dyadic(2.0)).unwrap())
        .unwrap();
    let y_fifth = five.clone().try_add(lattice).unwrap();
    let first = *crossings.first().expect("first exact crossing exists");
    let second = *crossings.get(1).expect("second exact crossing exists");
    assert_exact_at(&partition, first, [&x_third, &third], [&x_third, &third]);
    assert_exact_at(&partition, second, [&x_fifths, &five], [&y_fifth, &five]);
    assert_eq!(partition.nearest_xy(first), XY::new(1.0, 1.0));
    assert_eq!(partition.nearest_xy(second), XY::new(1.0, 1.0));
    let stored = partition.stored_node(XY::new(1.0, 1.0)).unwrap();
    let emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[first], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    assert_eq!(classes.first().unwrap(), &vec![first, second]);

    let embedded = partition.embed(&[&emission], "overlay").unwrap();
    assert_eq!(
        embedded.xy(stored).x.to_bits(),
        partition.nearest_xy(stored).x.to_bits()
    );
    assert_eq!(
        embedded.xy(stored).y.to_bits(),
        partition.nearest_xy(stored).y.to_bits()
    );
    assert_ne!(embedded.xy(first), partition.nearest_xy(first));
    assert_ne!(embedded.xy(second), partition.nearest_xy(second));
    assert_ne!(embedded.xy(first), embedded.xy(second));
    for node in [first, second] {
        let exact = partition.exact(node).unwrap();
        let [x, y, weight] = &*exact;
        let emitted = embedded.xy(node);
        let nearest = partition.nearest_xy(node);
        assert_exact_axis_displacement_bound(x, weight, emitted.x, nearest.x);
        assert_exact_axis_displacement_bound(y, weight, emitted.y, nearest.y);
    }
    let segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedded.coordinates(), item))
        .collect();
    let mut sink = embedding::FirstFailure::default();
    assert!(
        embedding::certify(
            &partition,
            &embedded,
            &[&emission],
            &[segments.as_slice()],
            &mut sink,
        )
        .unwrap()
        .is_continue()
    );
}

#[test]
fn argentina_uruguay_minimal_crossings_match_the_exhaustive_oracle() {
    let case = run(&argentina_uruguay_minimal_segments());
    let partition = case.partition();
    let at_58 = partition
        .stored_node(XY::new(-58.122_989_509_606_21, -33.022_490_639_567_15))
        .unwrap();
    let at_62 = partition
        .stored_node(XY::new(-58.113_024_156_144_505, -33.016_946_901_572_59))
        .unwrap();
    let first = partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == partition.nearest_xy(at_58))
        .expect("the 58/367 crossing rounds onto stored node 58");
    let second = partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == partition.nearest_xy(at_62))
        .expect("the 62/367 crossing rounds onto stored node 62");
    let emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[first, second], vec![
            None;
            partition
                .node_count(
                )
        ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    assert_eq!(classes.first().unwrap().len(), 3);
    let mut variables = classes.first().unwrap().clone();
    let mut work = Unmetered;
    try_sort_by_with_work(&mut variables, &mut work, |left, right, work| {
        problem.compare_nodes(*left, *right, work)
    })
    .unwrap();
    let fixed = problem.fixed_keys(0, &variables, &mut Unmetered).unwrap();
    let mut candidates = Vec::with_capacity(variables.len());
    for &node in &variables {
        let mut values = Vec::new();
        problem
            .fill_candidates(node, &mut values, &mut Unmetered)
            .unwrap();
        try_sort_by_with_work(&mut values, &mut work, |left, right, work| {
            problem.compare_candidates(node, *left, *right, work)
        })
        .unwrap();
        let mut row = Vec::new();
        for value in values {
            let key = problem.candidate_key(value);
            if !fixed.contains(&key)
                && !row
                    .iter()
                    .any(|prior: &XY| problem.candidate_key(*prior) == key)
            {
                row.push(value);
            }
        }
        candidates.push(row);
    }
    let oracle = first_certified_assignment(&partition, &emission, &variables, &candidates)
        .expect("the exact 58/62/367 component has a certified embedding");
    let assignment = Search::new(problem, "overlay", variables.len())
        .unwrap()
        .run()
        .unwrap();
    assert_eq!(assignment.classes.len(), 1);
    let solution = assignment.classes.first().unwrap();
    assert_eq!(solution.variables, variables);
    assert_eq!(solution.values, oracle);

    let embedding = partition.embed(&[&emission], "overlay").unwrap();
    assert_eq!(embedding.xy(at_58), partition.nearest_xy(at_58));
    assert_eq!(embedding.xy(at_62), partition.nearest_xy(at_62));
    for (&node, &expected) in solution.variables.iter().zip(&solution.values) {
        assert_eq!(embedding.xy(node), expected);
        let exact = partition.exact(node).unwrap();
        let [x, y, weight] = &*exact;
        let emitted = embedding.xy(node);
        let nearest = partition.nearest_xy(node);
        assert_exact_axis_displacement_bound(x, weight, emitted.x, nearest.x);
        assert_exact_axis_displacement_bound(y, weight, emitted.y, nearest.y);
    }
}

#[test]
fn full_ar_uy_symmetric_difference_repairs_nearest_embedding() {
    use crate::geometry::TopologyDomain;
    use crate::geometry::overlay::{Strictness, overlay_all};

    const COUNTRIES: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/osm_countries_0_1.geojson"
    ));

    let countries: serde_json::Value = serde_json::from_str(COUNTRIES).unwrap();
    let features = countries["features"].as_array().unwrap();
    let country = |code: &str| {
        let feature = features
            .iter()
            .rev()
            .find(|feature| {
                feature["properties"]["tags"]["ISO3166-1:alpha2"].as_str() == Some(code)
            })
            .unwrap();
        crate::io::parse_geojson(&feature["geometry"]).unwrap()
    };
    let argentina = country("AR");
    let uruguay = country("UY");
    let mut expected_wkb = None;
    for (left, right) in [(&argentina, &uruguay), (&uruguay, &argentina)] {
        let mut operands = [left, right];
        operands.sort_unstable_by(|left, right| crate::geometry::compare_shapes(left, right));
        let inventory = ExactSourceInventory::from_shapes(&operands).unwrap();
        let mut budget = ExpansionBudget::new("symmetric_difference", "AR/UY embedding repair");
        let partition = inventory.partition(&mut budget).unwrap();
        let (topology, edges) = partition.local_topology().unwrap();
        let global = partition.global_faces(&topology, &edges).unwrap();
        let cells = inventory
            .select_cells(
                &partition,
                &topology,
                &edges,
                &global,
                crate::geometry::OverlayOp::SymmetricDifference,
                false,
            )
            .unwrap();
        let plan = cells
            .emission_plan(&partition, &topology, &edges, &global)
            .unwrap();
        let emission = plan.emission(&topology, &edges);
        let emissions = [&emission];
        let nearest = embedding::Embedding::nearest(&partition);
        let nearest_segments: Vec<_> = emission
            .items
            .iter()
            .map(|&item| embedding::Embedding::segment_at(nearest.coordinates(), item))
            .collect();
        let nearest_segment_views = [nearest_segments.as_slice()];
        let mut nearest_failure = embedding::FirstFailure::default();
        assert!(
            embedding::certify(
                &partition,
                &nearest,
                &emissions,
                &nearest_segment_views,
                &mut nearest_failure,
            )
            .unwrap()
            .is_break(),
            "the full country symmetric difference must require embedding repair"
        );

        let repaired = partition.embed(&emissions, "symmetric_difference").unwrap();
        assert!(
            emission.items.iter().flatten().any(|&node| {
                repaired.xy(node) != embedding::Embedding::xy_at(nearest.coordinates(), node)
            }),
            "the full country repair did not move any emitted node"
        );
        let repaired_segments: Vec<_> = emission
            .items
            .iter()
            .map(|&item| embedding::Embedding::segment_at(repaired.coordinates(), item))
            .collect();
        let repaired_segment_views = [repaired_segments.as_slice()];
        let mut repaired_failure = embedding::FirstFailure::default();
        assert!(
            embedding::certify(
                &partition,
                &repaired,
                &emissions,
                &repaired_segment_views,
                &mut repaired_failure,
            )
            .unwrap()
            .is_continue(),
            "the production embedding must satisfy the full output certificate"
        );

        let result = overlay_all(
            &[left, right],
            TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::SymmetricDifference,
            Strictness::Lenient,
        )
        .unwrap();
        assert!(result.validate().unwrap().is_none());
        let wkb = crate::io::to_wkb(&result, None, false).unwrap();
        if let Some(expected) = &expected_wkb {
            assert_eq!(&wkb, expected, "public WKB changed by operand order");
        } else {
            expected_wkb = Some(wkb);
        }
    }
}

#[test]
fn only_free_neighbor_cells_occupied_by_stored_nodes_are_unrepresentable() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let ulp = exact_power_of_two(-52);
    let segments = [
        seg(
            1.0 - 100.0 * ulp,
            1.0 - 100.0 * ulp,
            1.0 + 100.0 * ulp,
            1.0 + 100.0 * ulp,
        ),
        seg(
            1.0 - 100.0 * ulp,
            1.0 + 201.0 * ulp,
            1.0 + 100.0 * ulp,
            1.0 - 199.0 * ulp,
        ),
    ];
    let mut case = run(&segments);
    case.isolated = [1.0_f64.next_down(), 1.0, 1.0_f64.next_up()]
        .into_iter()
        .flat_map(|x| [1.0_f64.next_down(), 1.0, 1.0_f64.next_up()].map(|y| XY::new(x, y)))
        .collect();
    let partition = case.partition();
    let mut emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    for &point in &case.isolated {
        let node = partition
            .stored_node(point)
            .expect("every isolated neighbor is a stored node");
        emission.items.push([node, node]);
    }
    let error = partition.embed(&[&emission], "overlay").unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "overlay",
            ..
        })
    ));
}

#[test]
fn exhaustive_contact_failure_rejects_an_injective_certificate_bypass() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let down = 1.0_f64.next_down();
    let up = 1.0_f64.next_up();
    let case = run(&[
        seg(down, down, up, down),
        seg(down, 1.0, up, 1.0),
        seg(down, up, up, up),
    ]);
    let mut partition = case.partition();
    let ulp = exact_power_of_two(-52);
    let exact = dyadic(1.0).try_add(dyadic(ulp / 4.0)).unwrap();
    let point = append_constructed_point(&mut partition, exact.clone(), exact);
    let mut emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    emission.items.push([point, point]);
    let mut problem = embedding::PlanarProblem::new(&partition, &[&emission], &[point], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    assert_eq!(classes.first().unwrap(), &vec![point]);
    let fixed = problem.fixed_keys(0, &[point], &mut Unmetered).unwrap();
    let mut raw_candidates = Vec::new();
    problem
        .fill_candidates(point, &mut raw_candidates, &mut Unmetered)
        .unwrap();
    assert_eq!(raw_candidates.len(), 9);
    let mut work = Unmetered;
    try_sort_by_with_work(&mut raw_candidates, &mut work, |left, right, work| {
        problem.compare_candidates(point, *left, *right, work)
    })
    .unwrap();
    let mut available = Vec::new();
    for candidate in raw_candidates {
        let key = problem.candidate_key(candidate);
        if !fixed.contains(&key)
            && !available
                .iter()
                .any(|prior: &XY| problem.candidate_key(*prior) == key)
        {
            available.push(candidate);
        }
    }
    assert_eq!(available.len(), 3);
    assert!(
        first_certified_assignment(&partition, &emission, &[point], &[available.clone()],)
            .is_none(),
        "every available candidate touches one of the three unconnected lines"
    );

    let local_calls = std::cell::Cell::new(0);
    let normal_global_calls = std::cell::Cell::new(0);
    let error = Search::new(
        TestVerifier {
            inner: &mut problem,
            local_calls: &local_calls,
            global_calls: &normal_global_calls,
            node_order: None,
            candidate_filter: None,
            bypass_local: false,
            bypass_global: false,
        },
        "overlay",
        1,
    )
    .unwrap()
    .run()
    .unwrap_err();
    assert!(local_calls.get() >= available.len());
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "overlay",
            constructed: 1
        })
    ));

    let bypass_calls = std::cell::Cell::new(0);
    let bypass_global_calls = std::cell::Cell::new(0);
    let bypass = Search::new(
        TestVerifier {
            inner: &mut problem,
            local_calls: &bypass_calls,
            global_calls: &bypass_global_calls,
            node_order: None,
            candidate_filter: None,
            bypass_local: true,
            bypass_global: false,
        },
        "overlay",
        1,
    )
    .unwrap()
    .run()
    .unwrap();
    assert_eq!(bypass_calls.get(), 1);
    let mut coordinates = embedding::Embedding::nearest(&partition)
        .coordinates()
        .to_vec();
    for class in bypass.classes {
        for (node, candidate) in class.variables.into_iter().zip(class.values) {
            *coordinates
                .get_mut(node.0 as usize)
                .expect("mutated assignment node belongs to the partition") = candidate;
        }
    }
    let embedding = embedding::Embedding::from_owned(coordinates);
    let segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
        .collect();
    let mut witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &embedding,
            &[&emission],
            &[segments.as_slice()],
            &mut witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(matches!(witnesses.0.as_slice(), [
        embedding::Witness::Contact { .. }
    ]));
}

#[test]
fn candidate_lattices_use_real_binade_neighbors_and_exact_order() {
    for center in [0.5_f64, 1.0, 2.0, 1024.0] {
        let upper_step = center.next_up() - center;
        let lower_step = center - center.next_down();
        assert_eq!(lower_step, upper_step * 0.5);
        let offset = dyadic(upper_step / 8.0);
        let exact = dyadic(center).try_add(offset).unwrap();
        let (nearest, candidates, ordered) = planar_candidate_values(
            exact.clone(),
            exact.clone(),
            embedding::EmbeddingDomain::Plane,
        );
        assert_eq!(nearest, XY::new(center, center));
        assert_eq!(candidates.len(), 9);
        for x in [center.next_down(), center, center.next_up()] {
            assert!(
                candidates
                    .iter()
                    .any(|candidate| candidate.x.to_bits() == x.to_bits())
            );
        }
        for y in [center.next_down(), center, center.next_up()] {
            assert!(
                candidates
                    .iter()
                    .any(|candidate| candidate.y.to_bits() == y.to_bits())
            );
        }
        let ordered_x: Vec<_> = ordered
            .iter()
            .filter_map(|candidate| {
                (candidate.y.to_bits() == center.to_bits()).then_some(candidate.x)
            })
            .collect();
        assert_eq!(
            ordered_x,
            [center, center.next_down(), center.next_up()],
            "exact displacement order around {center}"
        );
        for candidate in candidates {
            let bound = dyadic(1.5 * upper_step);
            let delta_x = dyadic(candidate.x)
                .try_subtract(exact.clone())
                .unwrap()
                .abs();
            let delta_y = dyadic(candidate.y)
                .try_subtract(exact.clone())
                .unwrap()
                .abs();
            assert_ne!(delta_x.cmp(&bound), Ordering::Greater);
            assert_ne!(delta_y.cmp(&bound), Ordering::Greater);
        }
    }
}

#[test]
fn zero_subnormal_and_finite_extreme_candidates_keep_their_real_keys() {
    let least = f64::from_bits(1);
    let exact_half = dyadic(least).try_product(&dyadic(0.5)).unwrap();
    let (zero, zero_candidates, _) = planar_candidate_values(
        exact_half.clone(),
        exact_half.clone(),
        embedding::EmbeddingDomain::Plane,
    );
    assert_eq!(zero.x, 0.0);
    assert!(
        zero_candidates
            .iter()
            .any(|candidate| candidate.x.to_bits() == (-least).to_bits())
    );
    assert!(
        zero_candidates
            .iter()
            .any(|candidate| candidate.x.to_bits() == 0.0_f64.to_bits())
    );
    assert!(
        zero_candidates
            .iter()
            .any(|candidate| candidate.x.to_bits() == least.to_bits())
    );
    let zero_keys: Vec<_> = zero_candidates
        .iter()
        .map(|candidate| PointKey::new(*candidate))
        .collect();
    for (index, key) in zero_keys.iter().enumerate() {
        assert!(
            !zero_keys
                .get(..index)
                .expect("earlier candidate keys are in range")
                .contains(key)
        );
    }
    assert_eq!(
        PointKey::new(XY::new(-0.0, -0.0)),
        PointKey::new(XY::new(0.0, 0.0))
    );

    for x in [least, -least] {
        let y = if x.is_sign_positive() {
            exact_half.clone()
        } else {
            dyadic(-least).try_product(&dyadic(0.5)).unwrap()
        };
        let (_, candidates, _) =
            planar_candidate_values(dyadic(x), y, embedding::EmbeddingDomain::Plane);
        assert!(
            candidates
                .iter()
                .all(|candidate| { candidate.x.is_finite() && candidate.y.is_finite() })
        );
        let keys: Vec<_> = candidates
            .iter()
            .map(|candidate| PointKey::new(*candidate))
            .collect();
        for (index, key) in keys.iter().enumerate() {
            assert!(
                !keys
                    .get(..index)
                    .expect("earlier candidate keys are in range")
                    .contains(key)
            );
        }
    }

    let max_step = f64::MAX - f64::MAX.next_down();
    let exact_max = dyadic(f64::MAX)
        .try_subtract(dyadic(max_step / 8.0))
        .unwrap();
    let subnormal = dyadic(least).try_product(&dyadic(0.5)).unwrap();
    let (nearest, candidates, _) =
        planar_candidate_values(exact_max, subnormal, embedding::EmbeddingDomain::Plane);
    assert_eq!(nearest.x, f64::MAX);
    assert!(candidates.iter().all(|candidate| candidate.x.is_finite()));
    assert!(
        candidates
            .iter()
            .any(|candidate| candidate.x.to_bits() == f64::MAX.to_bits())
    );
    assert!(
        candidates
            .iter()
            .any(|candidate| candidate.x.to_bits() == f64::MAX.next_down().to_bits())
    );
}

#[test]
fn exactly_represented_candidate_axes_keep_nearest_values() {
    let exact_x = dyadic(0.5);
    let y_step = 0.75_f64.next_up() - 0.75;
    let inexact_y = dyadic(0.75).try_add(dyadic(y_step / 8.0)).unwrap();
    let (nearest, candidates, _) =
        planar_candidate_values(exact_x, inexact_y, embedding::EmbeddingDomain::Plane);

    assert_eq!(nearest.x, 0.5);
    assert!(!candidates.is_empty());
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.x.to_bits() == nearest.x.to_bits()),
        "an exactly represented axis must retain its nearest coordinate: {candidates:?}"
    );

    let x_step = 0.5_f64.next_up() - 0.5;
    let inexact_x = dyadic(0.5).try_add(dyadic(x_step / 8.0)).unwrap();
    let exact_y = dyadic(0.75);
    let (nearest, candidates, _) =
        planar_candidate_values(inexact_x, exact_y, embedding::EmbeddingDomain::Plane);

    assert_eq!(nearest.y, 0.75);
    assert!(!candidates.is_empty());
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.y.to_bits() == nearest.y.to_bits()),
        "an exactly represented axis must retain its nearest coordinate: {candidates:?}"
    );
}

#[test]
fn rectangle_and_geographic_domains_filter_boundary_neighbors() {
    let half = 0.5_f64;
    let half_step = half.next_up() - half;
    let exact_x = dyadic(half).try_add(dyadic(half_step / 8.0)).unwrap();
    let three_quarters = 0.75_f64;
    let three_quarters_step = three_quarters.next_up() - three_quarters;
    let exact_y = dyadic(three_quarters)
        .try_add(dyadic(three_quarters_step / 8.0))
        .unwrap();
    let bounds = Bounds::new(half, 0.0, 1.0, 1.0).unwrap();
    let (nearest, candidates, _) = planar_candidate_values(
        exact_x,
        exact_y,
        embedding::EmbeddingDomain::Rectangle(bounds),
    );
    assert_eq!(nearest.x, half);
    assert_eq!(candidates.len(), 6);
    assert!(candidates.iter().all(|candidate| candidate.x >= half));

    let frame = crate::geographic::GeographicFrame::crs84();
    let longitude_step = 180.0 - 180.0_f64.next_down();
    let latitude_step = 90.0 - 90.0_f64.next_down();
    let exact_longitude = dyadic(180.0)
        .try_subtract(dyadic(longitude_step * 0.75))
        .unwrap();
    let exact_latitude = dyadic(90.0)
        .try_subtract(dyadic(latitude_step * 0.75))
        .unwrap();
    let (nearest, candidates, _) = planar_candidate_values(
        exact_longitude,
        exact_latitude,
        embedding::EmbeddingDomain::Geographic(frame),
    );
    assert_eq!(nearest.x, 180.0_f64.next_down());
    assert_eq!(nearest.y, 90.0_f64.next_down());
    assert!(candidates.iter().all(|candidate| {
        candidate.x.abs() <= 180.0
            && candidate.y.abs() <= 90.0
            && candidate.x.to_bits() != 180.0_f64.to_bits()
            && candidate.x.to_bits() != (-180.0_f64).to_bits()
            && candidate.y.to_bits() != 90.0_f64.to_bits()
            && candidate.y.to_bits() != (-90.0_f64).to_bits()
    }));
}

#[test]
fn distant_collision_classes_are_searched_independently() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let first_group = lattice_crossing_group(1.0, exact_power_of_two(-52));
    let second_group = lattice_crossing_group(8.0, exact_power_of_two(-49));
    let case = run(&[
        first_group[0],
        first_group[1],
        first_group[2],
        second_group[0],
        second_group[1],
        second_group[2],
    ]);
    let partition = case.partition();
    let first_emission = lattice_group_emission(&partition, 1.0);
    let second_emission = lattice_group_emission(&partition, 8.0);
    let pair_emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    let first_crossing = lattice_group_crossing(&partition, 1.0);
    let second_crossing = lattice_group_crossing(&partition, 8.0);
    let cache = || vec![None; partition.node_count()];

    let first_problem =
        embedding::PlanarProblem::new(&partition, &[&first_emission], &[first_crossing], cache())
            .unwrap();
    assert_eq!(Problem::classes(&first_problem).len(), 1);
    let first = Search::new(first_problem, "overlay", 2)
        .unwrap()
        .run()
        .unwrap();

    let second_problem =
        embedding::PlanarProblem::new(&partition, &[&second_emission], &[second_crossing], cache())
            .unwrap();
    assert_eq!(Problem::classes(&second_problem).len(), 1);
    let second = Search::new(second_problem, "overlay", 2)
        .unwrap()
        .run()
        .unwrap();

    let pair_problem = embedding::PlanarProblem::new(
        &partition,
        &[&pair_emission],
        &[first_crossing, second_crossing],
        cache(),
    )
    .unwrap();
    let classes = Problem::classes(&pair_problem);
    assert_eq!(classes.len(), 2);
    assert!(classes.iter().all(|class| class.len() == 2));
    let pair = Search::new(pair_problem, "overlay", 4)
        .unwrap()
        .run()
        .unwrap();
    assert!(
        pair.evaluations <= first.evaluations + second.evaluations,
        "paired search used {} evaluations; independent classes used {} + {}",
        pair.evaluations,
        first.evaluations,
        second.evaluations
    );
}

#[test]
fn same_chord_endpoint_domains_retain_the_legal_diagonal_assignment() {
    let line = crate::io::parse_wkt("LINESTRING (0 0, 2 2)").unwrap();
    let triangle =
        crate::io::parse_wkt("POLYGON ((1 0, 1.0000000000000002 4, 1.0000000000000002 8, 1 0))")
            .unwrap();
    let inventory = ExactSourceInventory::from_shapes(&[&line, &triangle]).unwrap();
    let mut budget = ExpansionBudget::new("intersection", "same-chord embedding test");
    let partition = inventory.partition(&mut budget).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let cells = inventory
        .select_cells(
            &partition,
            &topology,
            &edges,
            &global,
            crate::geometry::OverlayOp::Intersection,
            false,
        )
        .unwrap();
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &global)
        .unwrap();
    let emission = plan.emission(&topology, &edges);
    assert_eq!(emission.items.len(), 1);
    assert_eq!(emission.chords, 1);
    assert!(emission.faces.is_none());

    let nearest = XY::new(1.0, 1.0);
    let [source_start, source_end] = *emission
        .items
        .first()
        .expect("the selected line/area intersection has one chord");
    let mut endpoints = [source_start, source_end];
    try_sort_by(&mut endpoints, |left, right| {
        partition.compare_nodes(*left, *right)
    })
    .unwrap();
    let [first, second] = endpoints;
    assert_eq!(partition.nearest_xy(first), nearest);
    assert_eq!(partition.nearest_xy(second), nearest);
    for node in [first, second] {
        let exact = partition.exact(node).unwrap();
        let [x, y, _] = exact.as_ref();
        assert_eq!(x, y, "both exact intersections lie on the source diagonal");
    }
    assert_eq!(
        partition.compare_axis(first, second, Axis::X).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        partition.compare_axis(first, second, Axis::Y).unwrap(),
        Ordering::Less
    );
    let upper = XY::new(nearest.x.next_up(), nearest.y.next_up());
    assert_ne!(PointKey::new(nearest), PointKey::new(upper));

    let mut problem = embedding::PlanarProblem::new(&partition, &[&emission], &[first], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let allowed_assignment = |node: NodeId, candidate: XY| {
        (node == first && candidate == nearest) || (node == second && candidate == upper)
    };
    let local_calls = std::cell::Cell::new(0);
    let global_calls = std::cell::Cell::new(0);
    let assignment = Search::new(
        TestVerifier {
            inner: &mut problem,
            local_calls: &local_calls,
            global_calls: &global_calls,
            node_order: None,
            candidate_filter: Some(&allowed_assignment),
            bypass_local: false,
            bypass_global: false,
        },
        "intersection",
        2,
    )
    .unwrap()
    .run()
    .expect("the legal nearest/upper joint assignment survives embedding search");
    let solution = assignment
        .classes
        .first()
        .expect("the selected chord's movable endpoints share one prepared class");
    assert_eq!(solution.variables.as_slice(), &[first, second]);
    assert_eq!(solution.values.as_slice(), &[nearest, upper]);

    let mut coordinates = embedding::Embedding::nearest(&partition)
        .coordinates()
        .to_vec();
    for (&node, &candidate) in solution.variables.iter().zip(&solution.values) {
        *coordinates
            .get_mut(node.0 as usize)
            .expect("the selected chord endpoint belongs to the partition") = candidate;
    }
    let embedding = embedding::Embedding::from_owned(coordinates);
    let segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
        .collect();
    assert_eq!(embedding.xy(first), nearest);
    assert_eq!(embedding.xy(second), upper);
    let mut sink = embedding::FirstFailure::default();
    assert!(
        embedding::certify(
            &partition,
            &embedding,
            &[&emission],
            &[segments.as_slice()],
            &mut sink,
        )
        .unwrap()
        .is_continue()
    );
}

#[test]
fn long_chord_with_disjoint_axiswise_candidate_domains_keeps_independent_classes() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let source = run(&[seg(0.0, 0.0, 0.0, 0.0)]);
    let minimum_subnormal = dyadic(f64::from_bits(1));
    let cases = [
        (
            "horizontal",
            dyadic(-1.0e100),
            minimum_subnormal.try_product(&dyadic(0.5)).unwrap(),
            dyadic(1.0e100),
            minimum_subnormal.try_product(&dyadic(0.5)).unwrap(),
        ),
        (
            "vertical",
            dyadic(1.0e100),
            dyadic(-1.0).try_add(dyadic(exact_power_of_two(-55))).unwrap(),
            dyadic(1.0e100),
            dyadic(1.0).try_add(dyadic(exact_power_of_two(-54))).unwrap(),
        ),
    ];

    for (name, start_x, start_y, end_x, end_y) in cases {
        let mut partition = source.partition();
        let start = append_constructed_point(&mut partition, start_x, start_y);
        let end = append_constructed_point(&mut partition, end_x, end_y);
        let emission = embedding::Emission {
            items: vec![[start, end]],
            chords: 1,
            areal_chords: 0,
            fan_spans: Vec::new(),
            faces: None,
        };
        let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[start], vec![
            None;
            partition
                .node_count(
                )
        ])
        .unwrap();

        let mut start_candidates = Vec::new();
        problem
            .fill_candidates(start, &mut start_candidates, &mut Unmetered)
            .unwrap();
        let mut end_candidates = Vec::new();
        problem
            .fill_candidates(end, &mut end_candidates, &mut Unmetered)
            .unwrap();
        assert!(
            start_candidates.iter().all(|left| end_candidates
                .iter()
                .all(|right| PointKey::new(*left) != PointKey::new(*right))),
            "{name} endpoint candidate-key domains must be disjoint"
        );
        assert_eq!(
            Problem::classes(&problem),
            &[vec![start]],
            "{name} endpoint domains must remain independently repairable"
        );
    }
}

#[test]
fn local_pair_with_two_movable_left_endpoints_owns_both_in_one_class() {
    let case = run(&[seg(1.0, 0.0, 1.0, 0.0)]);
    let mut partition = case.partition();
    let minimum_subnormal = dyadic(f64::from_bits(1));
    let start = append_constructed_point(
        &mut partition,
        dyadic(0.0),
        minimum_subnormal.try_product(&dyadic(0.125)).unwrap(),
    );
    let end = append_constructed_point(
        &mut partition,
        dyadic(2.0),
        minimum_subnormal.try_product(&dyadic(0.375)).unwrap(),
    );
    let fixed = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
    let emission = embedding::Emission {
        items: vec![[start, end], [fixed, fixed]],
        chords: 1,
        areal_chords: 0,
        fan_spans: Vec::new(),
        faces: None,
    };
    let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[start], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let assignment = Search::new(problem, "difference", 2)
        .unwrap()
        .run()
        .unwrap();

    assert_eq!(assignment.classes.len(), 1);
    let solution = assignment.classes.first().unwrap();
    assert_eq!(solution.variables.as_slice(), &[start, end]);
    let [line_start, line_end] = solution.values.as_slice() else {
        panic!("the local pair assigns both movable line endpoints");
    };
    let line = Segment {
        start: *line_start,
        end: *line_end,
    };
    let fixed_point = Segment {
        start: XY::new(1.0, 0.0),
        end: XY::new(1.0, 0.0),
    };
    assert_eq!(
        segment_contact_exact(line, fixed_point),
        Contact::Disjoint,
        "the complete local pair must evaluate both movable line endpoints"
    );
}

fn degree_two_reversal_segments() -> [Segment; 3] {
    [
        seg(
            0.999_999_999_999_670_5,
            1.000_000_000_000_199_2,
            1.000_000_000_000_295_5,
            0.999_999_999_999_821_7,
        ),
        seg(
            0.999_999_999_999_786_4,
            1.000_000_000_000_594_4,
            1.000_000_000_000_156,
            0.999_999_999_999_566_3,
        ),
        seg(
            1.000_000_000_000_566_9,
            0.999_999_999_999_632_3,
            0.999_999_999_999_542_1,
            1.000_000_000_000_297_3,
        ),
    ]
}

fn fan_reversal_quad_segments() -> [Segment; 6] {
    use crate::geometry::numerics::product::exact_power_of_two;
    let step = exact_power_of_two(-52);
    let point = |(x, y): (i32, i32)| XY::new(1.0 + f64::from(x) * step, 1.0 + f64::from(y) * step);
    let [a, b, c, d] = [(0, 9), (9, 5), (11, 5), (3, 8)].map(point);
    [
        seg(a.x, a.y, b.x, b.y),
        seg(b.x, b.y, c.x, c.y),
        seg(c.x, c.y, d.x, d.y),
        seg(d.x, d.y, a.x, a.y),
        seg(a.x, a.y, c.x, c.y),
        seg(b.x, b.y, d.x, d.y),
    ]
}

fn exact_orientation_order(
    partition: &ExactPartition<'_>,
    [first_node, second_node, third_node]: [NodeId; 3],
) -> crate::error::Result<Ordering> {
    let rational_coordinates = |node| -> crate::error::Result<(ExactRational, ExactRational)> {
        let exact = partition.exact(node)?;
        let [x_numerator, y_numerator, homogeneous_weight] = &*exact;
        Ok((
            ExactRational::new(x_numerator.clone(), homogeneous_weight.clone())?,
            ExactRational::new(y_numerator.clone(), homogeneous_weight.clone())?,
        ))
    };
    let (first_x, first_y) = rational_coordinates(first_node)?;
    let (second_x, second_y) = rational_coordinates(second_node)?;
    let (third_x, third_y) = rational_coordinates(third_node)?;
    let first_to_second_x = second_x.try_subtract(&first_x)?;
    let first_to_second_y = second_y.try_subtract(&first_y)?;
    let first_to_third_x = third_x.try_subtract(&first_x)?;
    let first_to_third_y = third_y.try_subtract(&first_y)?;
    first_to_second_x
        .try_product(&first_to_third_y)?
        .try_subtract(&first_to_second_y.try_product(&first_to_third_x)?)?
        .try_cmp(&ExactRational::zero())
}

fn rounded_orientation_order(
    embedding: &embedding::Embedding<'_>,
    [a, b, c]: [NodeId; 3],
) -> Ordering {
    let (a, b, c) = (embedding.xy(a), embedding.xy(b), embedding.xy(c));
    match stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y) {
        ExactSign::Negative => Ordering::Less,
        ExactSign::Zero => Ordering::Equal,
        ExactSign::Positive => Ordering::Greater,
    }
}

fn unique_emission_nodes(emission: &embedding::Emission) -> Vec<NodeId> {
    let mut nodes = Vec::new();
    for &[start, end] in emission
        .items
        .get(..emission.chords)
        .expect("chord count is within emitted items")
    {
        for node in [start, end] {
            if !nodes.contains(&node) {
                nodes.push(node);
            }
        }
    }
    nodes
}

fn emission_for_faces(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edges: &[u32],
    global: &GlobalFaces,
    selected_faces: &[u32],
) -> crate::error::Result<embedding::Emission> {
    let mut faces = vec![false; global.count as usize];
    for &face in selected_faces {
        *faces
            .get_mut(face as usize)
            .expect("selected face belongs to this global face table") = true;
    }
    let cells = ArealCells {
        faces,
        spans: vec![false; partition.spans.len()],
        nodes: vec![false; partition.node_count()],
    };
    let plan = cells.emission_plan(partition, topology, edges, global)?;
    Ok(plan.emission(topology, edges))
}

#[test]
fn degree_two_triangle_reversal_is_rejected_by_g_and_published_when_g_is_bypassed() {
    let case = run(&degree_two_reversal_segments());
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 3);
    let at_origin = partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == XY::new(1.0, 1.0))
        .expect("first crossing rounds to the origin cell");
    let at_east = partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == XY::new(1.0_f64.next_up(), 1.0))
        .expect("second crossing rounds east");
    let at_north = partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == XY::new(1.0, 1.0_f64.next_up()))
        .expect("third crossing rounds north");
    let triangle = [at_origin, at_east, at_north];
    assert_eq!(
        exact_orientation_order(&partition, triangle).unwrap(),
        Ordering::Less
    );
    let nearest = embedding::Embedding::nearest(&partition);
    assert_eq!(
        rounded_orientation_order(&nearest, triangle),
        Ordering::Greater
    );
    let emission = {
        let (topology, edges) = partition.local_topology().unwrap();
        let global = partition.global_faces(&topology, &edges).unwrap();
        emission::polygonize_emission(&partition, &topology, &edges, &global).unwrap()
    };
    assert_eq!(emission.chords, 3);
    assert_eq!(emission.areal_chords, 3);
    assert_eq!(unique_emission_nodes(&emission).len(), 3);
    assert!(unique_emission_nodes(&emission).contains(&at_origin));
    let nearest_segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(nearest.coordinates(), item))
        .collect();
    embedding::check_emitted_fans(
        &partition,
        &nearest,
        &emission.items[..emission.chords],
        &emission.fan_spans,
        &nearest_segments[..emission.chords],
        emission.areal_chords,
    )
    .unwrap();
    let mut nearest_witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &nearest,
            &[&emission],
            &[nearest_segments.as_slice()],
            &mut nearest_witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(matches!(nearest_witnesses.0.as_slice(), [
        embedding::Witness::Face { .. }
    ]));

    let mut movable_cache = vec![None; partition.node_count()];
    let mut problem = embedding::PlanarProblem::new(
        &partition,
        &[&emission],
        &triangle,
        std::mem::take(&mut movable_cache),
    )
    .unwrap();
    let constructed = Problem::classes(&problem)
        .iter()
        .map(Vec::len)
        .sum::<usize>();
    assert_eq!(constructed, 3);
    let local_calls = std::cell::Cell::new(0);
    let global_calls = std::cell::Cell::new(0);
    let bypass = Search::new(
        TestVerifier {
            inner: &mut problem,
            local_calls: &local_calls,
            global_calls: &global_calls,
            node_order: None,
            candidate_filter: None,
            bypass_local: false,
            bypass_global: true,
        },
        "polygonize",
        constructed,
    )
    .unwrap()
    .run()
    .unwrap();
    assert_eq!(global_calls.get(), 1);

    let mut bypass_coordinates = nearest.coordinates().to_vec();
    for assignment in &bypass.classes {
        for (&node, &candidate) in assignment.variables.iter().zip(&assignment.values) {
            *bypass_coordinates
                .get_mut(node.0 as usize)
                .expect("bypass assignment node belongs to the partition") = candidate;
        }
    }
    let bypass_embedding = embedding::Embedding::from_owned(bypass_coordinates);
    for node in triangle {
        assert_eq!(bypass_embedding.xy(node), partition.nearest_xy(node));
    }
    let bypass_segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(bypass_embedding.coordinates(), item))
        .collect();
    let mut bypass_witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &bypass_embedding,
            &[&emission],
            &[bypass_segments.as_slice()],
            &mut bypass_witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(matches!(bypass_witnesses.0.as_slice(), [
        embedding::Witness::Face { .. }
    ]));

    match partition.embed(&[&emission], "polygonize") {
        Ok(recovered) => {
            let recovered_segments: Vec<_> = emission
                .items
                .iter()
                .map(|&item| embedding::Embedding::segment_at(recovered.coordinates(), item))
                .collect();
            let mut witnesses = WitnessLog::default();
            assert!(
                embedding::certify(
                    &partition,
                    &recovered,
                    &[&emission],
                    &[recovered_segments.as_slice()],
                    &mut witnesses,
                )
                .unwrap()
                .is_continue()
            );
            assert!(witnesses.0.is_empty());
        },
        Err(error) => assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(
                GeometryErrorKind::ExactEmbeddingUnrepresentable { .. }
            )
        )),
    }
}

#[derive(Clone, Copy)]
struct FanCandidateStatus {
    p_clean: bool,
    f_clean: bool,
    g_clean: bool,
}

fn classify_fan_candidates(
    partition: &ExactPartition<'_>,
    emission: &embedding::Emission,
    center: NodeId,
) -> crate::error::Result<Vec<FanCandidateStatus>> {
    let problem = embedding::PlanarProblem::new(partition, &[emission], &[center], vec![
        None;
        partition
            .node_count(
            )
    ])?;
    let classes = Problem::classes(&problem);
    let class = classes
        .iter()
        .position(|members| members.contains(&center))
        .expect("fan origin belongs to one movable class");
    let fixed = problem.fixed_keys(class, &[center], &mut Unmetered)?;
    let mut candidates = Vec::new();
    problem.fill_candidates(center, &mut candidates, &mut Unmetered)?;
    let mut work = Unmetered;
    try_sort_by_with_work(&mut candidates, &mut work, |left, right, work| {
        problem.compare_candidates(center, *left, *right, work)
    })?;
    let mut seen: Vec<PointKey> = Vec::new();
    candidates.retain(|candidate| {
        let key = problem.candidate_key(*candidate);
        !fixed.contains(&key) && !seen.contains(&key) && {
            seen.push(key);
            true
        }
    });

    let nearest = embedding::Embedding::nearest(partition);
    let mut classified = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let mut coordinates = nearest.coordinates().to_vec();
        *coordinates
            .get_mut(center.0 as usize)
            .expect("fan origin belongs to the partition") = candidate;
        let embedding = embedding::Embedding::from_owned(coordinates);
        let segments: Vec<_> = emission
            .items
            .iter()
            .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
            .collect();
        let mut witnesses = WitnessLog::default();
        let _certificate = embedding::certify(
            partition,
            &embedding,
            &[emission],
            &[segments.as_slice()],
            &mut witnesses,
        )?;
        let p_clean = witnesses.0.iter().all(|witness| {
            !matches!(
                witness,
                embedding::Witness::Collision(_, _) | embedding::Witness::Contact { .. }
            )
        });
        let f_clean = !witnesses.0.contains(&embedding::Witness::Fan(center));
        let g_clean = embedding::check_global_faces(
            partition,
            &embedding,
            &emission.items,
            emission
                .faces
                .as_deref()
                .expect("fan-pruning emissions retain global faces"),
        )
        .is_ok();
        classified.push(FanCandidateStatus {
            p_clean,
            f_clean,
            g_clean,
        });
    }
    Ok(classified)
}

#[test]
fn neighbor_float_fan_flip_is_classified_against_p_and_g() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let mut case = run(&fan_reversal_quad_segments());
    case.isolated.push(XY::new(7.0, 7.0));
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 1);
    let center = *partition.crossings().first().unwrap();
    let isolated = *partition
        .isolated
        .first()
        .expect("the unselected point supplies an empty fan row");
    assert_eq!(isolated.0.wrapping_add(1), center.0);
    assert_eq!(
        usize::try_from(center.0).unwrap().wrapping_add(1),
        partition.node_count(),
        "the four-departure fan is the final CSR row"
    );
    let step = exact_power_of_two(-52);
    let lattice =
        |(x, y): (i32, i32)| XY::new(1.0 + f64::from(x) * step, 1.0 + f64::from(y) * step);
    let [q0, q1, q2, q3] = [(0, 9), (9, 5), (11, 5), (3, 8)].map(lattice);
    let [q0, q1, q2, q3] = [
        partition.stored_node(q0).unwrap(),
        partition.stored_node(q1).unwrap(),
        partition.stored_node(q2).unwrap(),
        partition.stored_node(q3).unwrap(),
    ];
    let nearest = embedding::Embedding::nearest(&partition);
    assert_eq!(
        nearest.xy(center),
        XY::new(1.0 + 4.0 * exact_power_of_two(-52), 1.0 + 8.0 * exact_power_of_two(-52))
    );
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let face_count = global.count;
    let mut face_01 = None;
    let mut face_12 = None;
    let mut face_23 = None;
    let mut face_30 = None;
    for face in 1..face_count {
        let single = emission_for_faces(&partition, &topology, &edges, &global, &[face]).unwrap();
        let nodes = unique_emission_nodes(&single);
        if [center, q0, q1].iter().all(|node| nodes.contains(node)) {
            face_01 = Some(face);
        }
        if [center, q1, q2].iter().all(|node| nodes.contains(node)) {
            face_12 = Some(face);
        }
        if [center, q2, q3].iter().all(|node| nodes.contains(node)) {
            face_23 = Some(face);
        }
        if [center, q3, q0].iter().all(|node| nodes.contains(node)) {
            face_30 = Some(face);
        }
    }
    let opposite_faces = [
        [
            face_01.expect("the first bounded triangle exists"),
            face_23.expect("the third bounded triangle exists"),
        ],
        [
            face_12.expect("the second bounded triangle exists"),
            face_30.expect("the fourth bounded triangle exists"),
        ],
    ];
    let mut correctness_control = None;
    let mut work_control = None;
    for selected_faces in opposite_faces {
        let emission =
            emission_for_faces(&partition, &topology, &edges, &global, &selected_faces).unwrap();
        let center_degree = emission
            .items
            .get(..emission.areal_chords)
            .expect("areal chord prefix belongs to the emission")
            .iter()
            .filter(|item| item.contains(&center))
            .count();
        if center_degree != 4 {
            continue;
        }
        let statuses = classify_fan_candidates(&partition, &emission, center).unwrap();
        let Some(first_g_accepted) = statuses
            .iter()
            .position(|status| status.p_clean && status.g_clean)
        else {
            continue;
        };
        let accepted = *statuses
            .get(first_g_accepted)
            .expect("accepted candidate index belongs to its status row");
        if !accepted.f_clean {
            correctness_control = Some((selected_faces, accepted));
            break;
        }
        if work_control.is_none()
            && let Some(pruned) = statuses
                .get(..first_g_accepted)
                .expect("earlier candidates precede the accepted candidate")
                .iter()
                .find(|status| status.p_clean && !status.f_clean && !status.g_clean)
        {
            work_control = Some((selected_faces, *pruned));
        }
    }
    let (selected_faces, selected_fan_candidate) = correctness_control
        .or(work_control)
        .expect("the bounded face/candidate table contains an F-sensitive case");
    let emission =
        emission_for_faces(&partition, &topology, &edges, &global, &selected_faces).unwrap();
    let center_degree = emission
        .items
        .get(..emission.areal_chords)
        .expect("areal chord prefix belongs to the emission")
        .iter()
        .filter(|item| item.contains(&center))
        .count();
    assert_eq!(center_degree, 4);
    let mut problem = embedding::PlanarProblem::new(&partition, &[&emission], &[center], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    assert_eq!(classes.first().unwrap(), &vec![center]);
    let fixed = problem.fixed_keys(0, &[center], &mut Unmetered).unwrap();
    let mut candidates = Vec::new();
    problem
        .fill_candidates(center, &mut candidates, &mut Unmetered)
        .unwrap();
    let mut work = Unmetered;
    try_sort_by_with_work(&mut candidates, &mut work, |left, right, work| {
        problem.compare_candidates(center, *left, *right, work)
    })
    .unwrap();
    candidates.retain(|candidate| !fixed.contains(&problem.candidate_key(*candidate)));
    assert!(candidates.contains(&nearest.xy(center)));

    let normal_local_calls = std::cell::Cell::new(0);
    let normal_global_calls = std::cell::Cell::new(0);
    let normal = Search::new(
        TestVerifier {
            inner: &mut problem,
            local_calls: &normal_local_calls,
            global_calls: &normal_global_calls,
            node_order: None,
            candidate_filter: None,
            bypass_local: false,
            bypass_global: false,
        },
        "polygonize",
        1,
    )
    .unwrap()
    .run()
    .unwrap();
    let bypass_global_calls = std::cell::Cell::new(0);
    let without_fan_pruning = Search::new(
        FanBypassVerifier {
            inner: &mut problem,
            global_calls: &bypass_global_calls,
        },
        "polygonize",
        1,
    )
    .unwrap()
    .run()
    .unwrap();
    let normal_assignment = normal.classes.first().unwrap();
    let bypass_assignment = without_fan_pruning.classes.first().unwrap();
    assert!(
        !selected_fan_candidate.g_clean,
        "the bounded quad fixture found an F-only publication rather than a G-pruned candidate"
    );
    assert_eq!(normal_assignment.values, bypass_assignment.values);
    assert!(
        bypass_global_calls.get() > normal_global_calls.get(),
        "same result but no added G work: baseline {}, fan-bypassed {}",
        normal_global_calls.get(),
        bypass_global_calls.get()
    );
}

#[test]
fn node_id_candidate_ordering_breaks_permutation_wkb_determinism() {
    fn serialize_for_order(input_order: &[usize], node_id_order: bool) -> Vec<u8> {
        let anchors = [
            XY::new(100.0, -10.0),
            XY::new(100.0, 0.0),
            XY::new(100.0, 10.0),
            XY::new(100.0, 20.0),
        ];
        let source = anchors
            .iter()
            .map(|point| seg(point.x, point.y, point.x, point.y))
            .collect::<Vec<_>>();
        let case = run(&source);
        let mut partition = case.partition();
        let nodes = append_lattice_collision_points_in_order(&mut partition, 4, input_order);
        let anchor_nodes: Vec<_> = anchors
            .iter()
            .map(|&point| {
                partition
                    .stored_node(point)
                    .expect("every labelled line has a stored anchor")
            })
            .collect();
        let emission = embedding::Emission {
            items: nodes
                .iter()
                .zip(&anchor_nodes)
                .map(|(&node, &anchor)| [node, anchor])
                .collect(),
            chords: nodes.len(),
            areal_chords: 0,
            fan_spans: Vec::new(),
            faces: None,
        };
        let mut problem = embedding::PlanarProblem::new(&partition, &[&emission], &nodes, vec![
                None;
                partition
                    .node_count()
            ])
        .unwrap();
        let local_calls = std::cell::Cell::new(0);
        let global_calls = std::cell::Cell::new(0);
        let by_node_id = |left: NodeId, right: NodeId| left.0.cmp(&right.0);
        let node_order =
            node_id_order.then_some(&by_node_id as &dyn Fn(NodeId, NodeId) -> Ordering);
        let assignment = Search::new(
            TestVerifier {
                inner: &mut problem,
                local_calls: &local_calls,
                global_calls: &global_calls,
                node_order,
                candidate_filter: None,
                bypass_local: true,
                bypass_global: true,
            },
            "embedding_test",
            nodes.len(),
        )
        .unwrap()
        .run()
        .unwrap();
        let mut coordinates = embedding::Embedding::nearest(&partition)
            .coordinates()
            .to_vec();
        for class in assignment.classes {
            for (node, candidate) in class.variables.into_iter().zip(class.values) {
                *coordinates
                    .get_mut(node.0 as usize)
                    .expect("assignment node belongs to this partition") = candidate;
            }
        }
        let embedding = embedding::Embedding::from_owned(coordinates);
        let lines = nodes
            .iter()
            .zip(anchor_nodes)
            .map(|(&node, anchor)| {
                let points = [embedding.xy(node), embedding.xy(anchor)];
                Shape::LineString(LineSeq::from_proven(
                    CoordSeq::try_from_xy(&points).expect("line points are finite"),
                ))
            })
            .collect();
        let output = Shape::geometry_collection(lines).unwrap();
        crate::io::to_wkb(&output, None, false).unwrap()
    }

    let forward_order = [0, 1, 2, 3];
    let reverse_order = [3, 2, 1, 0];
    assert_eq!(
        serialize_for_order(&forward_order, false),
        serialize_for_order(&reverse_order, false),
        "exact node order keeps the serialized embedding stable"
    );
    assert_ne!(
        serialize_for_order(&forward_order, true),
        serialize_for_order(&reverse_order, true),
        "node-ID order changes which exact vertex receives each candidate"
    );
}
