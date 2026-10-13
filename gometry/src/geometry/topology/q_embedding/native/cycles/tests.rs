    use super::*;

    #[derive(Clone, Copy)]
    struct StartSpec {
        node: EmissionNodeId,
        spelling: NativeChartSpellingKey,
        reversed: bool,
    }

    impl StartSpec {
        fn forward(node: EmissionNodeId, spelling: NativeChartSpellingKey) -> Self {
            Self {
                node,
                spelling,
                reversed: false,
            }
        }
    }

    struct CycleSpec {
        region: usize,
        shell: bool,
        starts: Vec<StartSpec>,
    }

    struct CountingWork(u64);

    impl Work for CountingWork {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> Result<()> {
            self.0 = self
                .0
                .checked_add(ticks)
                .ok_or(ResourceError::SizeOverflow)?;
            Ok(())
        }

        fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
            let ticks = count
                .checked_mul(ticks_per_item)
                .ok_or(ResourceError::SizeOverflow)?;
            self.charge(u64::try_from(ticks).map_err(|_| ResourceError::SizeOverflow)?)
        }
    }

    struct SeededInputs(u64);

    impl SeededInputs {
        fn next(&mut self) -> u32 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 32) as u32
        }

        fn spelling(&mut self) -> NativeChartSpellingKey {
            match self.next() & 7 {
                0 => NativeChartSpellingKey::Interior(-1),
                1 => NativeChartSpellingKey::Interior(0),
                2 => NativeChartSpellingKey::Interior(1),
                3 => NativeChartSpellingKey::SeamEast,
                4 => NativeChartSpellingKey::SeamWest,
                _ => NativeChartSpellingKey::Pole(u64::from(self.next())),
            }
        }
    }

    fn synthetic_graph(
        specifications: Vec<CycleSpec>,
    ) -> (
        Vec<NativeGraphCycle>,
        Vec<[u32; 2]>,
        Vec<NativeChartVertexKey>,
    ) {
        let mut cycles = Vec::with_capacity(specifications.len());
        let mut edge_nodes = Vec::new();
        let mut vertex_keys = Vec::new();
        for specification in specifications {
            let mut edges = Vec::with_capacity(specification.starts.len());
            for start in specification.starts {
                let vertex = u32::try_from(vertex_keys.len()).expect("small test graph vertex");
                let other_vertex = vertex.checked_add(1).expect("small test graph endpoint");
                let atom = u32::try_from(edge_nodes.len()).expect("small test graph edge");
                vertex_keys.push(NativeChartVertexKey {
                    node: start.node,
                    spelling: start.spelling,
                });
                vertex_keys.push(NativeChartVertexKey {
                    node: EmissionNodeId(u32::MAX),
                    spelling: NativeChartSpellingKey::Pole(u64::MAX),
                });
                edge_nodes.push(if start.reversed {
                    [other_vertex, vertex]
                } else {
                    [vertex, other_vertex]
                });
                edges.push(NativeGraphCycleEdge {
                    atom,
                    reversed: start.reversed,
                    contributors: Vec::new(),
                });
            }
            cycles.push(NativeGraphCycle {
                face: 0,
                region: specification.region,
                edges,
                shell: specification.shell,
            });
        }
        (cycles, edge_nodes, vertex_keys)
    }

    fn query(
        region: usize,
        shell: bool,
        node: u32,
        spelling: Option<NativeChartSpellingKey>,
    ) -> NativeCycleStartQuery {
        NativeCycleStartQuery {
            region,
            shell,
            node: EmissionNodeId(node),
            spelling,
        }
    }

    fn exhaustive_rotations_for_queries(
        queries: &[NativeCycleStartQuery],
        cycles: &[NativeGraphCycle],
        edge_nodes: &[[u32; 2]],
        vertex_keys: &[NativeChartVertexKey],
        budget: &mut impl Work,
    ) -> Result<Vec<Option<usize>>> {
        let mut rotations = vec![None; cycles.len()];
        for &query in queries {
            let mut matching_cycle = None;
            let mut ambiguous = false;
            for (cycle_index, cycle) in cycles.iter().enumerate() {
                budget.charge_into_error(1)?;
                if cycle.region != query.region || cycle.shell != query.shell {
                    continue;
                }
                let Some(start) = exhaustive_native_cycle_start_vertex_index(
                    edge_nodes,
                    vertex_keys,
                    cycle,
                    query.node,
                    query.spelling,
                    budget,
                )?
                else {
                    continue;
                };
                if matching_cycle
                    .replace(NativeCycleStartCandidate { cycle_index, start })
                    .is_some()
                {
                    ambiguous = true;
                    break;
                }
            }
            if ambiguous {
                continue;
            }
            if let Some(candidate) = matching_cycle {
                let slot = rotations.get_mut(candidate.cycle_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Native cycle rotation row is unknown".into())
                })?;
                if slot.is_some_and(|previous| previous != candidate.start) {
                    return Err(GeometryErrorKind::Invalid(
                        "Native Q rings request conflicting starts on one graph cycle".into(),
                    )
                    .into());
                }
                *slot = Some(candidate.start);
            }
        }
        Ok(rotations)
    }

    fn assert_same_differential_result(
        indexed: &Result<Vec<Option<usize>>>,
        exhaustive: &Result<Vec<Option<usize>>>,
        context: &str,
    ) {
        match (indexed, exhaustive) {
            (Ok(indexed), Ok(exhaustive)) => assert_eq!(indexed, exhaustive, "{context}"),
            (Err(indexed), Err(exhaustive)) => {
                assert!(
                    matches!(
                        indexed.kind(),
                        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
                    ),
                    "{context}: indexed result has the wrong error kind"
                );
                assert!(
                    matches!(
                        exhaustive.kind(),
                        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
                    ),
                    "{context}: exhaustive result has the wrong error kind"
                );
            },
            (indexed, exhaustive) => panic!(
                "{context}: indexed/exhaustive error disagreement: indexed={indexed:?}, exhaustive={exhaustive:?}"
            ),
        }
    }

    fn differential_rotations(
        queries: &[NativeCycleStartQuery],
        cycles: &[NativeGraphCycle],
        edge_nodes: &[[u32; 2]],
        vertex_keys: &[NativeChartVertexKey],
        context: &str,
    ) -> Result<Vec<Option<usize>>> {
        let indexed = native_cycle_rotations_for_queries(
            queries,
            cycles,
            edge_nodes,
            vertex_keys,
            &mut Unmetered,
        );
        let exhaustive = exhaustive_rotations_for_queries(
            queries,
            cycles,
            edge_nodes,
            vertex_keys,
            &mut Unmetered,
        );
        assert_same_differential_result(&indexed, &exhaustive, context);
        indexed
    }

    #[test]
    fn native_cycle_start_index_matches_exhaustive_scan_on_seeded_small_graphs() {
        let mut inputs = SeededInputs(0x6E61_7469_7665_6379);
        for case in 0..96 {
            let cycle_count = inputs.next() & 7;
            let mut specifications = Vec::new();
            for _ in 0..cycle_count {
                let region = usize::try_from(inputs.next() & 3).expect("small test region");
                let shell = inputs.next() & 1 == 0;
                let start_count = inputs.next() & 7;
                let mut starts = Vec::new();
                for _ in 0..start_count {
                    starts.push(StartSpec {
                        node: EmissionNodeId(inputs.next() & 7),
                        spelling: inputs.spelling(),
                        reversed: inputs.next() & 1 != 0,
                    });
                }
                specifications.push(CycleSpec {
                    region,
                    shell,
                    starts,
                });
            }
            let query_count = (inputs.next() & 7).max(1);
            let mut queries = Vec::new();
            for _ in 0..query_count {
                let spelling = if inputs.next().trailing_zeros() >= 2 {
                    None
                } else {
                    Some(inputs.spelling())
                };
                queries.push(query(
                    usize::try_from(inputs.next() & 3).expect("small test region"),
                    inputs.next() & 1 == 0,
                    inputs.next() & 7,
                    spelling,
                ));
            }
            let (cycles, edge_nodes, vertex_keys) = synthetic_graph(specifications);

            let indexed = native_cycle_rotations_for_queries(
                &queries,
                &cycles,
                &edge_nodes,
                &vertex_keys,
                &mut Unmetered,
            );
            let exhaustive = exhaustive_rotations_for_queries(
                &queries,
                &cycles,
                &edge_nodes,
                &vertex_keys,
                &mut Unmetered,
            );
            assert_same_differential_result(
                &indexed,
                &exhaustive,
                &format!("seeded graph case {case}"),
            );
        }
    }

    #[test]
    fn native_cycle_start_lookup_preserves_first_match_fallback_and_ambiguity() -> Result<()> {
        let interior_zero = NativeChartSpellingKey::Interior(0);
        let interior_one = NativeChartSpellingKey::Interior(1);
        let interior_two = NativeChartSpellingKey::Interior(2);
        let (cycles, edge_nodes, vertex_keys) = synthetic_graph(vec![
            CycleSpec {
                region: 2,
                shell: true,
                starts: vec![
                    StartSpec::forward(EmissionNodeId(5), interior_zero),
                    StartSpec::forward(EmissionNodeId(5), interior_zero),
                    StartSpec::forward(EmissionNodeId(2), interior_one),
                    StartSpec::forward(EmissionNodeId(3), interior_one),
                    StartSpec::forward(EmissionNodeId(3), interior_two),
                ],
            },
            CycleSpec {
                region: 2,
                shell: true,
                starts: vec![StartSpec::forward(EmissionNodeId(5), interior_zero)],
            },
        ]);

        let first_exact = differential_rotations(
            &[query(2, true, 5, Some(interior_zero))],
            &cycles[..1],
            &edge_nodes[..5],
            &vertex_keys,
            "repeated exact spelling in one cycle",
        )?;
        assert_eq!(first_exact, [Some(0)]);

        let unique_node_fallback = differential_rotations(
            &[query(2, true, 2, Some(interior_zero))],
            &cycles[..1],
            &edge_nodes[..5],
            &vertex_keys,
            "unique-node fallback",
        )?;
        assert_eq!(unique_node_fallback, [Some(2)]);

        let repeated_node_without_exact_spelling = differential_rotations(
            &[query(2, true, 3, Some(interior_zero))],
            &cycles[..1],
            &edge_nodes[..5],
            &vertex_keys,
            "repeated node without exact spelling",
        )?;
        assert_eq!(repeated_node_without_exact_spelling, [None]);

        let exact_spelling_precedes_nonmatching_occurrences = differential_rotations(
            &[query(2, true, 3, Some(interior_one))],
            &cycles[..1],
            &edge_nodes[..5],
            &vertex_keys,
            "exact spelling beats other node occurrences",
        )?;
        assert_eq!(exact_spelling_precedes_nonmatching_occurrences, [Some(3)]);

        let unspecified_spelling_chooses_the_first_node_start = differential_rotations(
            &[query(2, true, 3, None)],
            &cycles[..1],
            &edge_nodes[..5],
            &vertex_keys,
            "unspecified spelling chooses first node start",
        )?;
        assert_eq!(unspecified_spelling_chooses_the_first_node_start, [Some(3)]);

        let cross_cycle_ambiguity = differential_rotations(
            &[query(2, true, 5, Some(interior_zero))],
            &cycles,
            &edge_nodes,
            &vertex_keys,
            "multiple matching cycles are ambiguous",
        )?;
        assert_eq!(cross_cycle_ambiguity, [None, None]);

        let (fallback_ambiguity_cycles, fallback_edge_nodes, fallback_vertex_keys) =
            synthetic_graph(vec![
                CycleSpec {
                    region: 3,
                    shell: true,
                    starts: vec![StartSpec::forward(EmissionNodeId(7), interior_zero)],
                },
                CycleSpec {
                    region: 3,
                    shell: true,
                    starts: vec![StartSpec::forward(EmissionNodeId(7), interior_one)],
                },
            ]);
        let exact_and_unique_fallback_are_ambiguous = differential_rotations(
            &[query(3, true, 7, Some(interior_zero))],
            &fallback_ambiguity_cycles,
            &fallback_edge_nodes,
            &fallback_vertex_keys,
            "exact match plus another cycle's unique fallback is ambiguous",
        )?;
        assert_eq!(exact_and_unique_fallback_are_ambiguous, [None, None]);
        Ok(())
    }

    #[test]
    fn native_cycle_start_conflicting_requests_keep_the_existing_error() {
        let spelling = NativeChartSpellingKey::Interior(0);
        let (cycles, edge_nodes, vertex_keys) = synthetic_graph(vec![CycleSpec {
            region: 4,
            shell: true,
            starts: vec![
                StartSpec::forward(EmissionNodeId(1), spelling),
                StartSpec::forward(EmissionNodeId(2), spelling),
            ],
        }]);
        let queries = [
            query(4, true, 1, Some(spelling)),
            query(4, true, 2, Some(spelling)),
        ];
        let indexed = native_cycle_rotations_for_queries(
            &queries,
            &cycles,
            &edge_nodes,
            &vertex_keys,
            &mut Unmetered,
        );
        let exhaustive = exhaustive_rotations_for_queries(
            &queries,
            &cycles,
            &edge_nodes,
            &vertex_keys,
            &mut Unmetered,
        );
        assert_same_differential_result(&indexed, &exhaustive, "conflicting ring starts");
        let error = indexed.expect_err("conflicting starts on one cycle must remain an error");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn native_cycle_start_work_scales_with_rings_and_cycles() -> Result<()> {
        fn measured_ticks(ring_count: u32, cycle_count: u32) -> Result<u64> {
            let mut specifications = Vec::new();
            for _ in 0..cycle_count {
                specifications.push(CycleSpec {
                    region: 0,
                    shell: true,
                    starts: vec![StartSpec::forward(
                        EmissionNodeId(0),
                        NativeChartSpellingKey::Interior(0),
                    )],
                });
            }
            let (cycles, edge_nodes, vertex_keys) = synthetic_graph(specifications);
            let queries = (0..ring_count)
                .map(|ring| {
                    query(
                        0,
                        true,
                        ring + 100,
                        Some(NativeChartSpellingKey::Interior(0)),
                    )
                })
                .collect::<Vec<_>>();
            let mut work = CountingWork(0);
            let rotations = native_cycle_rotations_for_queries(
                &queries,
                &cycles,
                &edge_nodes,
                &vertex_keys,
                &mut work,
            )?;
            assert!(rotations.iter().all(Option::is_none));
            Ok(work.0)
        }

        let measured = [(8, 8), (16, 16), (32, 32)]
            .map(|(rings, cycles)| measured_ticks(rings, cycles))
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        for (&ticks, (rings, cycles)) in measured.iter().zip([(8_u64, 8_u64), (16, 16), (32, 32)]) {
            assert!(
                ticks <= 5 * (rings + cycles),
                "cycle-start work must stay linear in K + M: K={rings}, M={cycles}, ticks={ticks}"
            );
        }
        assert!(
            measured[1] <= measured[0] * 3 && measured[2] <= measured[1] * 3,
            "doubling K and M must not approach the fourfold growth of a K×M scan: {measured:?}"
        );
        Ok(())
    }
